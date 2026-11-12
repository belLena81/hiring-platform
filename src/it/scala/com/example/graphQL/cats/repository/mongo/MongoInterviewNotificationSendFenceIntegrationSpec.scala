package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{Filters, Updates}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.duration.*

/** DHW-26 / Security finding F1 on the real replica set: the read-only pre-send fence answers like the receipt ledger's
  * four checks (claim, participants, kind in phase) and writes nothing. The hiring-commit predicate is covered by the
  * pure policy tests only; arranging a workflow in `NotificationsPending` needs the full scheduling saga and is left to
  * the worker integration specs.
  */
final class MongoInterviewNotificationSendFenceIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private final case class Arranged(
      fixture: MongoAccessEvaluationSupport.Fixture,
      fence: InterviewNotificationSendFence[IO],
      workflow: InterviewWorkflow,
      claim: ClaimedInterviewWorkflowCommand
  )

  private def success[A](result: cats.data.EitherT[IO, ?, A]): IO[A] =
    result.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"unexpected $error")), IO.pure))

  private def arranged[A](use: Arranged => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop[IO])
      val start = Instant.now().truncatedTo(ChronoUnit.MILLIS).plus(3, ChronoUnit.DAYS)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        workflow <- MongoInterviewContractWorkflows.create(
          fixture,
          repository,
          UserId(UUID.randomUUID()),
          UserId(UUID.randomUUID()),
          InterviewInterval(start, start.plus(1, ChronoUnit.HOURS))
        )
        now <- IO.realTimeInstant
        due <- success(repository.claimDueCommands("send-fence-due", now, 600.seconds, 1))
        initial <- IO.fromOption(due.headOption)(new AssertionError("missing command claim"))
        outcome <- success(repository.claimExecution(initial.record, "send-fence-exec", now, 600.seconds, 3))
        claim <- outcome match {
          case InterviewExecutionClaimOutcome.Acquired(value) => IO.pure(value)
          case other => IO.raiseError(new AssertionError(s"missing execution claim: $other"))
        }
        result <- use(Arranged(fixture, repository.notificationSendFence, workflow, claim))
      } yield result
    }

  private def verify(a: Arranged, kind: InterviewNotificationKind, claim: Option[ClaimedInterviewWorkflowCommand]) =
    a.fence.verify(a.workflow.id, kind, claim)

  test("DHW-26 F1 a held claim with active participants passes for a kind allowed in every phase") {
    arranged { a =>
      verify(a, InterviewNotificationKind.RescheduleProposed, Some(a.claim)).map(r => assertEquals(r, Right(())))
    }
  }

  test("DHW-26 F1 a missing claim, another owner's token and another workflow's claim are all refused") {
    arranged { a =>
      val kind = InterviewNotificationKind.RescheduleProposed
      List(
        None,
        Some(a.claim.copy(fencingToken = UUID.randomUUID())),
        Some(a.claim.copy(owner = "someone-else")),
        Some(a.claim.copy(record = a.claim.record.copy(workflowId = InterviewWorkflowId(UUID.randomUUID()))))
      ).traverse(claim => verify(a, kind, claim))
        .map(results => assertEquals(results, List.fill(4)(Left(InterviewNotificationSendRefusal.ClaimNotHeld))))
    }
  }

  test("DHW-26 F1 a claim whose lease has expired by the database clock is refused as not held") {
    arranged { a =>
      for {
        commands <- Mongo4catsCollections.documents(a.fixture.database, MongoCollections.InterviewWorkflowCommands)
        held <- verify(a, InterviewNotificationKind.RescheduleProposed, Some(a.claim))
        _ <- commands.updateOne(
          Filters.eq(MongoFields.Id, MongoInterviewWorkflowCodec.commandId(a.workflow.id, a.claim.record.stepId)),
          Updates.set("claimUntil", java.util.Date.from(Instant.now().minus(1, ChronoUnit.HOURS)))
        )
        expired <- verify(a, InterviewNotificationKind.RescheduleProposed, Some(a.claim))
      } yield {
        assertEquals(held, Right(()))
        assertEquals(expired, Left(InterviewNotificationSendRefusal.ClaimNotHeld))
      }
    }
  }

  test("DHW-26 F1 a deleted participant refuses the send, whichever participant it is") {
    List(true, false).traverse_ { candidate =>
      arranged { a =>
        val subject = if (candidate) a.workflow.candidateId else a.workflow.recruiterId
        for {
          users <- Mongo4catsCollections.documents(a.fixture.database, MongoCollections.Users)
          _ <- users.updateOne(
            Filters.eq(MongoFields.Id, subject.value.toString),
            // The users validator only admits a Deleted account without its personal data (UserFields.erasedOnDeletion).
            Updates.combine(
              (Updates.set(MongoFields.AccountStatus, "Deleted") ::
                Updates.set(MongoFields.DeletedAt, java.util.Date.from(Instant.now())) ::
                UserFields.erasedOnDeletion.map(Updates.unset))*
            )
          )
          result <- verify(a, InterviewNotificationKind.RescheduleProposed, Some(a.claim))
        } yield assertEquals(result, Left(InterviewNotificationSendRefusal.ParticipantInactive))
      }
    }
  }

  test("DHW-26 F1 a round kind outside its phase is refused: Scheduled and Cancelled before their notification phase") {
    arranged { a =>
      List(InterviewNotificationKind.Scheduled, InterviewNotificationKind.Cancelled)
        .traverse(kind => verify(a, kind, Some(a.claim)))
        .map(results =>
          assertEquals(results, List.fill(2)(Left(InterviewNotificationSendRefusal.KindNotAllowedInPhase)))
        )
    }
  }

  test("DHW-26 F1 the fence records nothing: no receipt and no fence counter changes") {
    arranged { a =>
      def counters = for {
        workflows <- Mongo4catsCollections.documents(a.fixture.database, MongoCollections.InterviewWorkflows)
        commands <- Mongo4catsCollections.documents(a.fixture.database, MongoCollections.InterviewWorkflowCommands)
        workflow <- workflows.find(Filters.eq(MongoFields.Id, a.workflow.id.value.toString)).first
        command <- commands
          .find(Filters.eq(MongoFields.Id, MongoInterviewWorkflowCodec.commandId(a.workflow.id, a.claim.record.stepId)))
          .first
        count <- MongoRepositoryTestSupport.count(
          a.fixture.database,
          MongoCollections.InterviewNotificationReceipts,
          Filters.eq("workflowId", a.workflow.id.value.toString)
        )
      } yield (workflow.map(_.get("providerFence")), command.map(_.get("effectFence")), count)
      for {
        before <- counters
        _ <- verify(a, InterviewNotificationKind.RescheduleProposed, Some(a.claim))
        _ <- verify(a, InterviewNotificationKind.Scheduled, Some(a.claim))
        after <- counters
      } yield {
        assertEquals(after, before)
        assertEquals(after._3, 0L)
      }
    }
  }
}
