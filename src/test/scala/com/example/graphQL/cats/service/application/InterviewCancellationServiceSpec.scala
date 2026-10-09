package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{ActorContext, RepositoryError, ServiceFixtures, UseCaseError}
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** DHW-15, DHW-16 (service part), DHW-19 and the unit part of DHW-23 through the real service and the pure policy, over
  * an in-memory repository that honors the guarded-write contract.
  */
final class InterviewCancellationServiceSpec extends CatsEffectSuite {
  import ServiceFixtures.*

  private val otherCandidateId = UserId(new UUID(0L, 101L))
  private val otherRecruiterId = UserId(new UUID(0L, 102L))
  private val workflowId = InterviewWorkflowId(new UUID(0L, 20L))
  private val interval = InterviewInterval(now.plusSeconds(172800), now.plusSeconds(176400))
  private val replacement = InterviewInterval(now.plusSeconds(259200), now.plusSeconds(262800))
  private val bothNotified = Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)

  private def workflowIn(
      phase: InterviewWorkflowPhase,
      customize: InterviewWorkflow => InterviewWorkflow = identity
  ): InterviewWorkflow =
    customize(
      InterviewWorkflow(
        workflowId,
        applicationId,
        candidateId,
        recruiterId,
        interval,
        now.plusSeconds(300),
        new UUID(0L, 21L),
        3L,
        phase,
        bothNotified,
        recruiterId
      )
    )

  private def proposalOpen(expiresAt: Instant = now.plusSeconds(172800)): InterviewWorkflow =
    workflowIn(
      InterviewWorkflowPhase.ProposalPending,
      _.copy(proposal = Some(InterviewRescheduleProposal(replacement, recruiterId, expiresAt)))
    )

  private val candidateActor = ActorContext(candidateId, UserRole.Candidate)
  private val recruiterActor = ActorContext(recruiterId, UserRole.Recruiter)
  private val adminActor = ActorContext(adminId, UserRole.Admin)
  private val otherCandidateActor = ActorContext(otherCandidateId, UserRole.Candidate)
  private val otherRecruiterActor = ActorContext(otherRecruiterId, UserRole.Recruiter)

  private final case class Arrangement(
      service: InterviewSchedulingService,
      repository: InMemoryInterviewWorkflowRepository
  ) {
    def stored: IO[InterviewWorkflow] = repository.store.get.map(_.workflows(workflowId))
    def commands: IO[Int] = repository.store.get.map(_.commands.size)
    def history: IO[Vector[(UserId, ApplicationStatus, ApplicationStatus, String)]] =
      repository.store.get.map(_.history)
    def status: IO[ApplicationStatus] = repository.applicationStatus.get
  }

  private def arranged(
      workflow: InterviewWorkflow,
      clock: Instant = now,
      status: ApplicationStatus = ApplicationStatus.Interview,
      ttl: InterviewProposalTtl = InterviewProposalTtl.Default
  ): IO[Arrangement] =
    for {
      users <- Ref.of[IO, Map[UserId, User]](
        Map(
          candidateId -> candidate,
          recruiterId -> recruiter,
          adminId -> admin,
          otherCandidateId -> candidate.copy(id = otherCandidateId, name = "Other candidate"),
          otherRecruiterId -> recruiter.copy(id = otherRecruiterId, name = "Other recruiter")
        )
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      applications <- Ref.of[IO, Map[ApplicationId, Application]](
        Map(applicationId -> createdApplication.copy(status = status))
      )
      events <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      errors <- Ref.of[IO, Option[RepositoryError]](None)
      repository <- InMemoryInterviewWorkflowRepository.create(workflow, status)
    } yield Arrangement(
      new InterviewSchedulingService(
        new InMemoryUsers(users),
        new InMemoryJobs(jobs),
        new InMemoryApplications(applications, events, errors),
        repository,
        5.minutes,
        IO.pure(clock),
        proposalTtl = ttl
      ),
      repository
    )

  private enum Action(val name: String) {
    case Cancel extends Action("cancel")
    case Request extends Action("requestReschedule")
    case Dismiss extends Action("dismissRescheduleRequest")
    case Propose extends Action("proposeReschedule")
    case Withdraw extends Action("withdrawReschedule")
    case Accept extends Action("acceptReschedule")
    case Decline extends Action("declineReschedule")
  }

  private def run(
      a: Arrangement,
      action: Action,
      actor: ActorContext,
      expectedRevision: Long = 3L,
      key: UUID = UUID.randomUUID()
  ): IO[Either[InterviewActionError, InterviewWorkflow]] = {
    val id = workflowId
    val result = action match {
      case Action.Cancel  => a.service.cancel(actor, id, expectedRevision, key)
      case Action.Request => a.service.requestReschedule(actor, id, expectedRevision, key)
      case Action.Dismiss => a.service.dismissRescheduleRequest(actor, id, expectedRevision, key)
      case Action.Propose =>
        a.service.proposeReschedule(actor, id, expectedRevision, replacement.startsAt, replacement.endsAt, key)
      case Action.Withdraw => a.service.withdrawReschedule(actor, id, expectedRevision, key)
      case Action.Accept   => a.service.acceptReschedule(actor, id, expectedRevision, key)
      case Action.Decline  => a.service.declineReschedule(actor, id, expectedRevision, key)
    }
    result.value
  }

  private def startState(action: Action): InterviewWorkflow = action match {
    case Action.Dismiss => workflowIn(InterviewWorkflowPhase.Completed, _.copy(rescheduleRequestedAt = Some(now)))
    case Action.Withdraw | Action.Accept | Action.Decline => proposalOpen()
    case Action.Cancel | Action.Request | Action.Propose  => workflowIn(InterviewWorkflowPhase.Completed)
  }

  private val forbidden = InterviewActionError.UseCase(UseCaseError.Domain(DomainError.Forbidden))
  private val notFound = InterviewActionError.UseCase(UseCaseError.Domain(DomainError.NotFound("interviewWorkflow")))

  private enum Expectation { case Allowed, Forbidden, NotFound }
  import Expectation.*

  // The authorization matrix of the specification, one row per action: candidate, owning recruiter, Admin, another
  // candidate, a non-owning recruiter. Anything not allowed is denied.
  private val matrix: List[(Action, List[(ActorContext, String, Expectation)])] = {
    def row(action: Action, own: Expectation, recruiter: Expectation, adminResult: Expectation) =
      action -> List(
        (candidateActor, "the application's candidate", own),
        (recruiterActor, "the owning recruiter", recruiter),
        (adminActor, "Admin", adminResult),
        (otherCandidateActor, "another candidate", NotFound),
        (otherRecruiterActor, "a non-owning recruiter", NotFound)
      )
    List(
      row(Action.Cancel, Allowed, Allowed, Allowed),
      row(Action.Request, Allowed, Forbidden, Forbidden),
      row(Action.Dismiss, Forbidden, Allowed, Allowed),
      row(Action.Propose, Forbidden, Allowed, Allowed),
      row(Action.Withdraw, Forbidden, Allowed, Allowed),
      row(Action.Accept, Allowed, Forbidden, Forbidden),
      row(Action.Decline, Allowed, Forbidden, Forbidden)
    )
  }

  for {
    (action, actors) <- matrix
    (actor, who, expectation) <- actors
  } test(s"DHW-15 ${action.name} by $who is $expectation") {
    arranged(startState(action)).flatMap { a =>
      for {
        result <- run(a, action, actor)
        after <- a.stored
        commands <- a.commands
        status <- a.status
        history <- a.history
      } yield expectation match {
        case Allowed =>
          assert(result.isRight, clue(result))
          assertEquals(after.revision, 4L)
        case Forbidden =>
          assertEquals(result, Left(forbidden))
          assertEquals(
            (after, commands, status, history),
            (startState(action), 0, ApplicationStatus.Interview, Vector.empty)
          )
        case NotFound =>
          assertEquals(result, Left(notFound))
          assertEquals(
            (after, commands, status, history),
            (startState(action), 0, ApplicationStatus.Interview, Vector.empty)
          )
      }
    }
  }

  test("DHW-15 an unauthenticated or mismatching claim reaches no workflow") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
      a.service
        .cancel(ActorContext(candidateId, UserRole.Admin), workflowId, 3L, UUID.randomUUID())
        .value
        .map(result => assert(result.isLeft, clue(result)))
    }
  }

  test("DHW-16 cancellation records who cancelled: generated feedback and the actor, never user text") {
    List(
      (candidateActor, candidateId, "Interview cancelled by the candidate."),
      (recruiterActor, recruiterId, "Interview cancelled by the recruiter."),
      (adminActor, adminId, "Interview cancelled by an administrator.")
    ).traverse_ { case (actor, userId, text) =>
      arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
        for {
          result <- run(a, Action.Cancel, actor)
          history <- a.history
          status <- a.status
          rows <- a.repository.store.get.map(_.commands.map(_._3))
        } yield {
          assertEquals(result.map(_.phase), Right(InterviewWorkflowPhase.CancelPending))
          assertEquals(history, Vector((userId, ApplicationStatus.Interview, ApplicationStatus.Rejected, text)))
          assertEquals(status, ApplicationStatus.Rejected)
          assertEquals(
            rows,
            Vector(InterviewLifecycleCommand.CancelCalendarSlot(InterviewWorkflow.cancellationKey(workflowId, 0)))
          )
        }
      }
    }
  }

  test("DHW-16 the candidate gains no generic status power: every status mutation stays recruiter and Admin only") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(candidateId -> candidate, recruiterId -> recruiter, adminId -> admin))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      applications <- Ref.of[IO, Map[ApplicationId, Application]](
        Map(applicationId -> createdApplication.copy(status = ApplicationStatus.Interview))
      )
      events <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      errors <- Ref.of[IO, Option[RepositoryError]](None)
      service = new ApplicationService(
        new InMemoryUsers(users),
        new InMemoryJobs(jobs),
        new InMemoryApplications(applications, events, errors),
        com.example.graphQL.cats.service.mutation.TestIdempotency.noop,
        clock = FixedTestClock.at(now)
      )
      results <- ApplicationStatus.values.toList.traverse(target =>
        service
          .changeStatus(
            IdempotencyRequest.fromCanonicalInput(UUID.randomUUID(), s"candidate-$target"),
            candidateActor,
            applicationId,
            target,
            Some("feedback"),
            Some("reason")
          )
          .value
      )
      stored <- applications.get
      written <- events.get
    } yield {
      assertEquals(
        results.distinct,
        List[Either[UseCaseError, Application]](Left(UseCaseError.Domain(DomainError.Forbidden)))
      )
      assertEquals(stored(applicationId).status, ApplicationStatus.Interview)
      assertEquals(written, Vector.empty)
    }
  }

  test("DHW-19 typed outcomes leave no command, revision or status change") {
    val started = now.plusSeconds(172800)
    val cases: List[(String, InterviewWorkflow, Action, Instant, ApplicationStatus, InterviewActionError)] = List(
      (
        "a started interval",
        workflowIn(InterviewWorkflowPhase.Completed),
        Action.Cancel,
        started,
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.InterviewAlreadyStarted)
      ),
      (
        "an already cancelled workflow",
        workflowIn(InterviewWorkflowPhase.Cancelled, _.copy(cancelledAt = Some(now))),
        Action.Cancel,
        now,
        ApplicationStatus.Rejected,
        InterviewActionError.Workflow(InterviewWorkflowError.InterviewAlreadyCancelled)
      ),
      (
        "an unsettled phase",
        workflowIn(InterviewWorkflowPhase.RescheduleHoldPending, _.copy(pendingInterval = Some(replacement))),
        Action.Propose,
        now,
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.InterviewNotSettled)
      ),
      (
        "an open proposal",
        proposalOpen(),
        Action.Propose,
        now,
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.ProposalAlreadyOpen)
      ),
      (
        "a request while a proposal is open",
        proposalOpen(),
        Action.Request,
        now,
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.ProposalAlreadyOpen)
      ),
      (
        "no open proposal to accept",
        workflowIn(InterviewWorkflowPhase.Completed),
        Action.Accept,
        now,
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.NoOpenProposal)
      ),
      (
        "no open proposal to withdraw",
        workflowIn(InterviewWorkflowPhase.Completed),
        Action.Withdraw,
        now,
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.NoOpenProposal)
      ),
      (
        "an expired proposal that the expiry command has not yet closed",
        proposalOpen(now.plusSeconds(60)),
        Action.Accept,
        now.plusSeconds(60),
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.ProposalExpired)
      ),
      (
        "a stale revision",
        workflowIn(InterviewWorkflowPhase.Completed),
        Action.Request,
        now,
        ApplicationStatus.Interview,
        InterviewActionError.Workflow(InterviewWorkflowError.StaleRevision)
      )
    )
    cases.traverse_ { case (label, workflow, action, clock, status, expected) =>
      val actor = action match {
        case Action.Propose | Action.Withdraw | Action.Dismiss => recruiterActor
        case _                                                 => candidateActor
      }
      arranged(workflow, clock, status).flatMap { a =>
        for {
          result <- run(a, action, actor, expectedRevision = if (label == "a stale revision") 2L else 3L)
          after <- a.stored
          commands <- a.commands
          currentStatus <- a.status
        } yield {
          assertEquals(result, Left(expected), label)
          assertEquals((after, commands, currentStatus), (workflow, 0, status), label)
        }
      }
    }
  }

  test("DHW-19 a hired or rejected application refuses cancel, propose, accept and request with a typed transition") {
    List(Action.Cancel, Action.Propose, Action.Request).traverse_ { action =>
      List(ApplicationStatus.Hired, ApplicationStatus.Rejected).traverse_ { status =>
        val actor = if (action == Action.Propose) recruiterActor else candidateActor
        arranged(workflowIn(InterviewWorkflowPhase.Completed), status = status).flatMap { a =>
          for {
            result <- run(a, action, actor)
            commands <- a.commands
          } yield {
            val target = if (action == Action.Cancel) ApplicationStatus.Rejected else ApplicationStatus.Interview
            assertEquals(
              result,
              Left(
                InterviewActionError.UseCase(UseCaseError.Domain(DomainError.InvalidStatusTransition(status, target)))
              )
            )
            assertEquals(commands, 0)
          }
        }
      }
    }
  }

  test(
    "DHW-19 an open proposal of a no longer interviewing application cannot be accepted, but can still be declined"
  ) {
    // The proposal then simply expires; declining and withdrawing stay possible and touch no application.
    arranged(proposalOpen(), status = ApplicationStatus.Hired).flatMap { a =>
      for {
        accepted <- run(a, Action.Accept, candidateActor)
        declined <- run(a, Action.Decline, candidateActor)
      } yield {
        assertEquals(
          accepted,
          Left(
            InterviewActionError.UseCase(
              UseCaseError.Domain(
                DomainError.InvalidStatusTransition(ApplicationStatus.Hired, ApplicationStatus.Interview)
              )
            )
          )
        )
        assertEquals(declined.map(_.phase), Right(InterviewWorkflowPhase.Completed))
      }
    }
  }

  test("DHW-20 a proposal belongs to the trusted proposer, expires at the earliest limit and clears the request flag") {
    val requested = workflowIn(InterviewWorkflowPhase.Completed, _.copy(rescheduleRequestedAt = Some(now)))
    arranged(requested, ttl = InterviewProposalTtl.from(java.time.Duration.ofHours(2)).toOption.get).flatMap { a =>
      run(a, Action.Propose, adminActor).map { result =>
        val proposal = result.toOption.flatMap(_.proposal)
        assertEquals(proposal.map(_.proposedBy), Some(adminId))
        assertEquals(proposal.map(_.expiresAt), Some(now.plus(java.time.Duration.ofHours(2))))
        assertEquals(result.toOption.flatMap(_.rescheduleRequestedAt), None)
        assertEquals(result.map(_.phase), Right(InterviewWorkflowPhase.ProposalPending))
      }
    }
  }

  test("DHW-23 a repeated request with the same key returns the same workflow and changes nothing twice") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
      val key = UUID.randomUUID()
      for {
        first <- run(a, Action.Cancel, candidateActor, key = key)
        second <- run(a, Action.Cancel, candidateActor, key = key)
        history <- a.history
        commands <- a.commands
      } yield {
        assertEquals(second, first)
        assertEquals(history.size, 1)
        assertEquals(commands, 1)
      }
    }
  }

  test("DHW-23 a key reused for another operation or another input is a typed conflict") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
      val key = UUID.randomUUID()
      for {
        first <- run(a, Action.Request, candidateActor, key = key)
        otherOperation <- run(a, Action.Cancel, candidateActor, key = key)
        otherRevision <- run(a, Action.Request, candidateActor, expectedRevision = 9L, key = key)
        status <- a.status
      } yield {
        assert(first.isRight)
        assertEquals(
          otherOperation,
          Left(InterviewActionError.UseCase(UseCaseError.Repository(RepositoryError.Conflict)))
        )
        assertEquals(
          otherRevision,
          Left(InterviewActionError.UseCase(UseCaseError.Repository(RepositoryError.Conflict)))
        )
        assertEquals(status, ApplicationStatus.Interview)
      }
    }
  }

  test("DHW-23 cancel beats an open proposal and the late accept finds nothing to accept") {
    arranged(proposalOpen()).flatMap { a =>
      for {
        cancelled <- run(a, Action.Cancel, candidateActor)
        late <- run(a, Action.Accept, candidateActor, expectedRevision = 4L)
        stale <- run(a, Action.Accept, candidateActor, expectedRevision = 3L)
        stored <- a.stored
        history <- a.history
      } yield {
        assertEquals(cancelled.map(_.phase), Right(InterviewWorkflowPhase.CancelPending))
        assertEquals(late, Left(InterviewActionError.Workflow(InterviewWorkflowError.NoOpenProposal)))
        assertEquals(stale, Left(InterviewActionError.Workflow(InterviewWorkflowError.StaleRevision)))
        assertEquals(stored.proposal, None)
        assertEquals(history.size, 1)
      }
    }
  }

  test("DHW-23 cancel against cancel: the second cancel finds the first one's revision and writes nothing") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
      for {
        first <- run(a, Action.Cancel, candidateActor)
        second <- run(a, Action.Cancel, recruiterActor)
        third <- run(a, Action.Cancel, adminActor, expectedRevision = 4L)
        history <- a.history
      } yield {
        assert(first.isRight)
        assertEquals(second, Left(InterviewActionError.Workflow(InterviewWorkflowError.StaleRevision)))
        assertEquals(third, Left(InterviewActionError.Workflow(InterviewWorkflowError.InterviewAlreadyCancelled)))
        assertEquals(history.size, 1)
      }
    }
  }

  test("DHW-23 accept wins against withdraw and expiry by revision, and the second proposal is refused") {
    arranged(proposalOpen()).flatMap { a =>
      for {
        accepted <- run(a, Action.Accept, candidateActor)
        withdraw <- run(a, Action.Withdraw, recruiterActor)
        withdrawFresh <- run(a, Action.Withdraw, recruiterActor, expectedRevision = 4L)
        proposeAgain <- run(a, Action.Propose, recruiterActor, expectedRevision = 4L)
      } yield {
        assertEquals(accepted.map(_.phase), Right(InterviewWorkflowPhase.RescheduleHoldPending))
        assertEquals(withdraw, Left(InterviewActionError.Workflow(InterviewWorkflowError.StaleRevision)))
        assertEquals(withdrawFresh, Left(InterviewActionError.Workflow(InterviewWorkflowError.NoOpenProposal)))
        assertEquals(proposeAgain, Left(InterviewActionError.Workflow(InterviewWorkflowError.InterviewNotSettled)))
      }
    }
  }

  test("DHW-23 a second proposal is refused until the first is withdrawn, then allowed") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
      for {
        first <- run(a, Action.Propose, recruiterActor)
        second <- run(a, Action.Propose, adminActor, expectedRevision = 4L)
        _ <- run(a, Action.Withdraw, recruiterActor, expectedRevision = 4L)
        third <- run(a, Action.Propose, adminActor, expectedRevision = 5L)
      } yield {
        assert(first.isRight)
        assertEquals(second, Left(InterviewActionError.Workflow(InterviewWorkflowError.ProposalAlreadyOpen)))
        assert(third.isRight, clue(third))
      }
    }
  }

  test("DHW-19 dismissing a request that was never made is a typed outcome and writes nothing") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
      for {
        result <- run(a, Action.Dismiss, recruiterActor)
        commands <- a.commands
      } yield {
        assertEquals(result, Left(InterviewActionError.Workflow(InterviewWorkflowError.NoRescheduleRequest)))
        assertEquals(commands, 0)
      }
    }
  }

  test("DHW-23 repeating a request whose flag is already set returns the workflow and stores no receipt") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed, _.copy(rescheduleRequestedAt = Some(now)))).flatMap { a =>
      for {
        results <- List.fill(5)(run(a, Action.Request, candidateActor)).sequence
        receipts <- a.repository.store.get.map(_.requestReceipts.size)
        stored <- a.stored
      } yield {
        assert(results.forall(_ == Right(stored)), clue(results))
        assertEquals(receipts, 0)
      }
    }
  }

  test("DHW-19 a replacement interval must be a valid future interval different from the current one") {
    arranged(workflowIn(InterviewWorkflowPhase.Completed)).flatMap { a =>
      for {
        past <- a.service
          .proposeReschedule(
            recruiterActor,
            workflowId,
            3L,
            now.minusSeconds(60),
            now.plusSeconds(60),
            UUID.randomUUID()
          )
          .value
        reversed <- a.service
          .proposeReschedule(
            recruiterActor,
            workflowId,
            3L,
            replacement.endsAt,
            replacement.startsAt,
            UUID.randomUUID()
          )
          .value
        unchanged <- a.service
          .proposeReschedule(recruiterActor, workflowId, 3L, interval.startsAt, interval.endsAt, UUID.randomUUID())
          .value
        commands <- a.commands
      } yield {
        assertEquals(past, Left(InterviewActionError.Workflow(InterviewWorkflowError.StartMustBeInFuture)))
        assertEquals(reversed, Left(InterviewActionError.Workflow(InterviewWorkflowError.EndMustFollowStart)))
        assertEquals(unchanged, Left(InterviewActionError.Workflow(InterviewWorkflowError.RescheduleIntervalUnchanged)))
        assertEquals(commands, 0)
      }
    }
  }

  test("DHW-29 repair and notification repair are Admin only and touch nothing for anyone else") {
    arranged(workflowIn(InterviewWorkflowPhase.RepairRequired)).flatMap { a =>
      List(candidateActor, recruiterActor, otherCandidateActor, otherRecruiterActor).traverse_ { actor =>
        for {
          repair <- a.service.repair(actor, workflowId, 3L, UUID.randomUUID()).value
          listing <- a.service.notificationRepairs(actor, workflowId).value
          requeue <- a.service.repairNotifications(actor, workflowId, UUID.randomUUID()).value
          stored <- a.stored
        } yield {
          assertEquals(repair, Left(UseCaseError.Domain(DomainError.Forbidden)))
          assertEquals(listing, Left(UseCaseError.Domain(DomainError.Forbidden)))
          assertEquals(requeue, Left(UseCaseError.Domain(DomainError.Forbidden)))
          assertEquals(stored.phase, InterviewWorkflowPhase.RepairRequired)
        }
      }
    }
  }
}
