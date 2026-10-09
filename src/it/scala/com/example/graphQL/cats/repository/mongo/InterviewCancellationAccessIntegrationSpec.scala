package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.*
import java.util.UUID
import scala.concurrent.duration.*

/** DHW-15 against the real replica set: by direct id, strangers see nothing and change nothing, and every role is held
  * to the actions it may drive. A denied attempt writes nothing anywhere.
  */
final class InterviewCancellationAccessIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  import InterviewLifecycleScenario.{now, replacement}

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  private final case class Footprint(
      workflow: InterviewWorkflow,
      documents: List[Long],
      application: Option[String]
  )

  private def footprint(s: InterviewLifecycleScenario): IO[Footprint] =
    for {
      workflow <- s.current
      counts <- List(
        MongoCollections.InterviewWorkflows,
        MongoCollections.InterviewWorkflowCommands,
        MongoCollections.InterviewWorkflowInbox,
        MongoCollections.ApplicationEvents,
        MongoCollections.EventOutbox
      ).traverse(s.count(_))
      application <- s.applicationStatus
    } yield Footprint(workflow, counts, application)

  private val strangerCandidate = UserId(UUID.randomUUID())
  private val strangerRecruiter = UserId(UUID.randomUUID())

  private def proposalOpen(s: InterviewLifecycleScenario): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent
        .Propose(replacement.startsAt, replacement.endsAt, s.workflow.recruiterId, now, InterviewProposalTtl.Default),
      s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "propose"),
      0L,
      now
    )

  private def requested(s: InterviewLifecycleScenario): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.RequestReschedule(now),
      s.actor(UserRole.Candidate, s.workflow.candidateId, input = "request"),
      0L,
      now
    )

  private val events: List[(String, UserRole, InterviewLifecycleEvent)] = List(
    ("cancel", UserRole.Candidate, InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now)),
    ("cancel", UserRole.Recruiter, InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now)),
    ("request", UserRole.Candidate, InterviewLifecycleEvent.RequestReschedule(now)),
    ("dismiss", UserRole.Recruiter, InterviewLifecycleEvent.DismissRescheduleRequest),
    (
      "propose",
      UserRole.Recruiter,
      InterviewLifecycleEvent
        .Propose(replacement.startsAt, replacement.endsAt, strangerRecruiter, now, InterviewProposalTtl.Default)
    ),
    ("withdraw", UserRole.Recruiter, InterviewLifecycleEvent.WithdrawProposal(now)),
    ("accept", UserRole.Candidate, InterviewLifecycleEvent.AcceptProposal(now)),
    ("decline", UserRole.Candidate, InterviewLifecycleEvent.DeclineProposal(now))
  )

  private def strangers(
      s: InterviewLifecycleScenario,
      before: Footprint,
      expected: Long
  ): IO[Unit] =
    events.traverse_ { (name, role, event) =>
      val intruder = if (role == UserRole.Candidate) strangerCandidate else strangerRecruiter
      val tailored = event match {
        case InterviewLifecycleEvent.Propose(start, end, _, at, ttl) =>
          InterviewLifecycleEvent.Propose(start, end, intruder, at, ttl)
        case other => other
      }
      for {
        outcome <- s.apply(tailored, s.actor(role, intruder, input = s"$name-by-stranger"), expected, now)
        after <- footprint(s)
      } yield {
        assertEquals(outcome, Right(InterviewLifecycleOutcome.NotVisible), s"$name by a stranger")
        assertEquals(after, before, s"$name by a stranger wrote something")
      }
    }

  test("another candidate and a non-owning recruiter reach neither a settled workflow nor an open proposal") {
    scenario { s =>
      for {
        flagged <- requested(s)
        before <- footprint(s)
        _ <- strangers(s, before, flagged.revision)
      } yield ()
    } *> scenario { s =>
      for {
        open <- proposalOpen(s)
        before <- footprint(s)
        _ <- strangers(s, before, open.revision)
      } yield ()
    }
  }

  private def denied(
      s: InterviewLifecycleScenario,
      before: Footprint,
      expected: Long,
      attempts: List[(String, UserRole, UserId, InterviewLifecycleEvent)]
  ): IO[Unit] =
    attempts.traverse_ { (name, role, actor, event) =>
      for {
        outcome <- s.apply(event, s.actor(role, actor, input = s"$name-denied"), expected, now)
        after <- footprint(s)
      } yield {
        assertEquals(outcome, Left(RepositoryError.InvalidEvent), name)
        assertEquals(after, before, s"$name wrote something")
      }
    }

  test("participants and Admin are held to the actions their role allows, by the guarded write itself") {
    scenario { s =>
      val candidate = s.workflow.candidateId
      val recruiter = s.workflow.recruiterId
      val admin = UserId(UUID.randomUUID())
      for {
        open <- proposalOpen(s)
        before <- footprint(s)
        _ <- denied(
          s,
          before,
          open.revision,
          List(
            ("candidate withdraws", UserRole.Candidate, candidate, InterviewLifecycleEvent.WithdrawProposal(now)),
            (
              "candidate proposes",
              UserRole.Candidate,
              candidate,
              InterviewLifecycleEvent
                .Propose(replacement.startsAt, replacement.endsAt, candidate, now, InterviewProposalTtl.Default)
            ),
            ("candidate dismisses", UserRole.Candidate, candidate, InterviewLifecycleEvent.DismissRescheduleRequest),
            ("recruiter accepts", UserRole.Recruiter, recruiter, InterviewLifecycleEvent.AcceptProposal(now)),
            ("recruiter declines", UserRole.Recruiter, recruiter, InterviewLifecycleEvent.DeclineProposal(now)),
            ("recruiter requests", UserRole.Recruiter, recruiter, InterviewLifecycleEvent.RequestReschedule(now)),
            ("Admin accepts for the candidate", UserRole.Admin, admin, InterviewLifecycleEvent.AcceptProposal(now)),
            ("Admin declines for the candidate", UserRole.Admin, admin, InterviewLifecycleEvent.DeclineProposal(now)),
            ("Admin requests", UserRole.Admin, admin, InterviewLifecycleEvent.RequestReschedule(now)),
            (
              "a recruiter proposes in another's name",
              UserRole.Recruiter,
              recruiter,
              InterviewLifecycleEvent
                .Propose(replacement.startsAt, replacement.endsAt, strangerRecruiter, now, InterviewProposalTtl.Default)
            ),
            (
              "a candidate cancels as the recruiter",
              UserRole.Candidate,
              candidate,
              InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now)
            ),
            (
              "a recruiter cancels as Admin",
              UserRole.Recruiter,
              recruiter,
              InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Admin, now)
            )
          )
        )
      } yield ()
    }
  }

  test("a person can only drive the seven user events and the system never drives them") {
    scenario { s =>
      val candidate = s.workflow.candidateId
      val recruiter = s.workflow.recruiterId
      for {
        before <- footprint(s)
        systemEvents = List[InterviewLifecycleEvent](
          InterviewLifecycleEvent.CancelConfirmed(now),
          InterviewLifecycleEvent.CancelOutcomeUnknown,
          InterviewLifecycleEvent.HoldConfirmed,
          InterviewLifecycleEvent.SwapCommitted,
          InterviewLifecycleEvent.ProposalExpired(now),
          InterviewLifecycleEvent.NotificationDelivered(InterviewParticipant.Candidate),
          InterviewLifecycleEvent.RetryExhausted("provider"),
          InterviewLifecycleEvent.Repair
        )
        _ <- denied(
          s,
          before,
          0L,
          systemEvents.flatMap(event =>
            List(
              (s"candidate drives $event", UserRole.Candidate, candidate, event),
              (s"recruiter drives $event", UserRole.Recruiter, recruiter, event)
            )
          )
        )
        userEventsFromTheSystem <- List[InterviewLifecycleEvent](
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
          InterviewLifecycleEvent.RequestReschedule(now),
          InterviewLifecycleEvent.AcceptProposal(now)
        ).traverse(event =>
          s.apply(
            event,
            InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(UUID.randomUUID().toString)),
            0L,
            now
          )
        )
        after <- footprint(s)
      } yield {
        assertEquals(userEventsFromTheSystem, List.fill(3)(Left(RepositoryError.InvalidEvent)))
        assertEquals(after, before)
      }
    }
  }

  test("repeating a request whose flag is set with fresh keys stores nothing, however often it is repeated") {
    scenario { s =>
      for {
        flagged <- requested(s)
        before <- footprint(s)
        outcomes <- List
          .fill(25)(())
          .traverse(_ =>
            s.apply(
              InterviewLifecycleEvent.RequestReschedule(now),
              s.actor(UserRole.Candidate, s.workflow.candidateId, UUID.randomUUID(), "request-again"),
              flagged.revision,
              now
            )
          )
        after <- footprint(s)
      } yield {
        assertEquals(outcomes.distinct, List(Right(InterviewLifecycleOutcome.Duplicate(flagged))))
        assertEquals(after, before)
      }
    }
  }
}
