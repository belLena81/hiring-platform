package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.application.InterviewMessagePolicy
import com.example.graphQL.cats.service.port.*
import scala.concurrent.duration.*

/** An expiry has no external effect, so every way it can fail ends in the proposal being expired: a proposal is never
  * left open and the claimer is never starved by a row it cannot finish.
  */
final class InterviewProposalExpiryIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  import InterviewLifecycleScenario.{now, replacement}

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  private def success[A](operation: RepositoryIO[A]): IO[A] =
    operation.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))

  private def proposed(s: InterviewLifecycleScenario): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent
        .Propose(replacement.startsAt, replacement.endsAt, s.workflow.recruiterId, now, InterviewProposalTtl.Default),
      s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "propose"),
      0L,
      now
    )

  private def expiryRow(s: InterviewLifecycleScenario): IO[InterviewWorkflowCommandRecord] =
    s.commandRows.flatMap(rows =>
      IO.fromOption(rows.find(_.command.isInstanceOf[InterviewLifecycleCommand.ExpireProposal]))(
        new AssertionError("no expiry row")
      )
    )

  private def messageFor(workflow: InterviewWorkflow, row: InterviewWorkflowCommandRecord): InterviewMessage =
    InterviewMessage(
      InterviewMessagePolicy.stableId(row.stepId),
      workflow.id.value,
      row.stepId,
      InterviewStep.ExpireProposal,
      row.revision,
      workflow.id.value,
      workflow.preCommitDeadline,
      None,
      row.occurredAt
    )

  test("a publisher that gives up on the expiry expires the proposal and leaves nothing to claim again") {
    scenario { s =>
      for {
        open <- proposed(s)
        due = open.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
        claims <- success(s.repository.claimDueCommands("publisher", due.plusSeconds(1), due.plusSeconds(60), 8))
        claim <- IO.fromOption(claims.find(_.record.command.isInstanceOf[InterviewLifecycleCommand.ExpireProposal]))(
          new AssertionError("the expiry was not claimable once due")
        )
        resolution <- success(s.repository.requireRepair(claim, due.plusSeconds(2), "publication_exhausted"))
        after <- s.current
        again <- success(s.repository.claimDueCommands("publisher", due.plusSeconds(3), due.plusSeconds(60), 8))
        row <- expiryRow(s)
      } yield {
        assertEquals(resolution, InterviewPublicationResolution.Repaired)
        assertEquals(after.phase, InterviewWorkflowPhase.Completed)
        assertEquals(after.proposal, None)
        assert(
          !again.exists(_.record.command.isInstanceOf[InterviewLifecycleCommand.ExpireProposal]),
          "the expiry row is not claimed again"
        )
        assertNotEquals(row.state, InterviewWorkflowCommandState.Claimed)
      }
    }
  }

  test("an expiry row whose give-up resolved the workflow is superseded, not left in repair") {
    scenario { s =>
      for {
        open <- proposed(s)
        due = open.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
        claims <- success(s.repository.claimDueCommands("publisher", due.plusSeconds(1), due.plusSeconds(60), 8))
        claim <- IO.fromOption(claims.find(_.record.command.isInstanceOf[InterviewLifecycleCommand.ExpireProposal]))(
          new AssertionError("the expiry was not claimable once due")
        )
        _ <- success(s.repository.requireRepair(claim, due.plusSeconds(2), "publication_exhausted"))
        row <- expiryRow(s)
      } yield assertEquals(row.state, InterviewWorkflowCommandState.Superseded)
    }
  }

  test("an expiry message older than the replay window expires the proposal instead of being quarantined") {
    scenario { s =>
      for {
        open <- proposed(s)
        due = open.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
        row <- expiryRow(s)
        late = s.worker("late-worker", clock = IO.pure(due.plus(java.time.Duration.ofDays(8))))
        durable <- late.receiveCommand(messageFor(open, row))
        after <- s.current
        quarantined <- s.count(
          MongoCollections.InterviewWorkflowInbox,
          com.mongodb.client.model.Filters.eq("documentType", "quarantine")
        )
      } yield {
        assert(durable)
        assertEquals(after.phase, InterviewWorkflowPhase.Completed)
        assertEquals(after.proposal, None)
        assertEquals(quarantined, 0L)
      }
    }
  }

  test("an expiry message that arrives too early is acknowledged and its row requeued for its due time") {
    scenario { s =>
      for {
        open <- proposed(s)
        due = open.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
        row <- expiryRow(s)
        early = s.worker("early-worker", clock = IO.pure(due.minusSeconds(3600)))
        durable <- early.receiveCommand(messageFor(open, row))
        after <- s.current
        quarantined <- s.count(
          MongoCollections.InterviewWorkflowInbox,
          com.mongodb.client.model.Filters.eq("documentType", "quarantine")
        )
        requeued <- expiryRow(s)
        onTime <- s.worker("on-time-worker", clock = IO.pure(due.plusSeconds(1))).receiveCommand(messageFor(open, row))
      } yield {
        assert(durable, "an early message is acknowledged so the partition does not restart-loop")
        assertEquals(after.phase, InterviewWorkflowPhase.ProposalPending)
        assertEquals(quarantined, 0L)
        assertEquals(requeued.state, InterviewWorkflowCommandState.Pending)
        assert(requeued.availableAt.isAfter(due), "requeued after its due time, with a backoff")
        assert(onTime)
      }
    }
  }

  test("a deferral while the publisher still holds the row requeues it and a later markPublished cannot resurrect it") {
    scenario { s =>
      for {
        open <- proposed(s)
        due = open.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
        claims <- success(s.repository.claimDueCommands("publisher", due.plusSeconds(1), due.plusSeconds(60), 8))
        claim <- IO.fromOption(claims.find(_.record.command.isInstanceOf[InterviewLifecycleCommand.ExpireProposal]))(
          new AssertionError("the expiry was not claimable once due")
        )
        availableAt = due.plusSeconds(30)
        requeued <- success(s.repository.deferExpiry(claim.record, availableAt))
        afterDefer <- expiryRow(s)
        published <- s.repository.markPublished(claim, due.plusSeconds(2)).value
        afterPublish <- expiryRow(s)
        early <- success(s.repository.claimDueCommands("publisher", due.plusSeconds(10), due.plusSeconds(60), 8))
        expired <- s
          .worker("late-worker", clock = IO.pure(availableAt.plusSeconds(2)))
          .receiveCommand(messageFor(open, afterDefer))
        afterExpiry <- expiryRow(s)
      } yield {
        assert(requeued)
        assertEquals(afterDefer.state, InterviewWorkflowCommandState.Pending)
        assertEquals(afterDefer.availableAt, availableAt)
        assert(published.isLeft, "the publisher's claim no longer holds the row")
        assertEquals(afterPublish.state, InterviewWorkflowCommandState.Pending)
        assert(!early.exists(_.record.command.isInstanceOf[InterviewLifecycleCommand.ExpireProposal]))
        assert(expired)
        // Once the clock caught up the expiry executed: its result is recorded for the result consumer to apply.
        assertEquals(afterExpiry.result, Some(InterviewCommandResult.Succeeded))
      }
    }
  }

  test("a proposal whose expiry has not run yet can still be withdrawn after its expiry time") {
    scenario { s =>
      for {
        open <- proposed(s)
        due = open.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
        withdrawn <- s.apply(
          InterviewLifecycleEvent.WithdrawProposal(due.plusSeconds(1)),
          s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "withdraw"),
          open.revision,
          due.plusSeconds(1)
        )
        after <- s.current
      } yield {
        assert(withdrawn.exists(_.isInstanceOf[InterviewLifecycleOutcome.Applied]), clue(withdrawn))
        assertEquals(after.phase, InterviewWorkflowPhase.Completed)
        assertEquals(after.proposal, None)
      }
    }
  }
}
