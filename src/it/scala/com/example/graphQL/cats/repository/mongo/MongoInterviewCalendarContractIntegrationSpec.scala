package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{Filters, Updates}
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

/** Runs the shared calendar contract against the platform's durable calendar ledger on a real replica set. */
final class MongoInterviewCalendarContractIntegrationSpec
    extends MongoIntegrationSuite
    with InterviewCalendarProviderContract {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  override protected def withSubject[A](use: InterviewCalendarContractSubject => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
      val workflows = Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflows)
      def update(workflow: InterviewWorkflow, changes: org.bson.conversions.Bson*): IO[Unit] =
        workflows.flatMap(
          _.updateOne(Filters.eq(MongoFields.Id, workflow.id.value.toString), Updates.combine(changes*)).void
        )
      val subject = new InterviewCalendarContractSubject {
        override val provider: InterviewCalendarProvider = LedgerInterviewCalendarProvider.durable(repository)

        override def newWorkflow(candidate: UserId, recruiter: UserId, interval: InterviewInterval) = {
          val now = Instant.now()
          for {
            workflow <- IO.fromEither(
              InterviewWorkflow
                .create(
                  InterviewWorkflowId(UUID.randomUUID()),
                  ApplicationId(UUID.randomUUID()),
                  candidate,
                  recruiter,
                  interval,
                  now.plusSeconds(300),
                  UUID.randomUUID(),
                  ApplicationStatus.Accepted
                )
                .leftMap(error => new AssertionError(s"Invalid workflow: $error"))
            )
            _ <- InterviewSchedulingFixtures.seed(fixture.database, List(workflow), now)
            _ <- repository
              .create(
                workflow,
                InterviewWorkflow.initialCommand(workflow),
                UUID.randomUUID(),
                MutationReceiptFingerprint.fromCanonicalInput(workflow.id.value.toString),
                now
              )
              .value
              .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"create failed: $error")), _ => IO.unit))
          } yield workflow
        }

        override def requireReplacementHold(workflow: InterviewWorkflow, replacement: InterviewInterval) =
          update(
            workflow,
            Updates.set("phase", InterviewWorkflowPhase.RescheduleHoldPending.toString),
            Updates.set("pendingStartsAt", Date.from(replacement.startsAt)),
            Updates.set("pendingEndsAt", Date.from(replacement.endsAt))
          ).as(
            workflow.copy(phase = InterviewWorkflowPhase.RescheduleHoldPending, pendingInterval = Some(replacement))
          )

        override def requireCancellation(workflow: InterviewWorkflow, generation: Int) =
          if (workflow.phase == InterviewWorkflowPhase.RescheduleHoldPending)
            update(
              workflow,
              Updates.set("phase", InterviewWorkflowPhase.RescheduleCancelOldPending.toString),
              Updates.set("generation", Int.box(generation + 1)),
              Updates.unset("pendingStartsAt"),
              Updates.unset("pendingEndsAt")
            ).as(
              workflow.copy(
                phase = InterviewWorkflowPhase.RescheduleCancelOldPending,
                generation = generation + 1,
                pendingInterval = None
              )
            )
          else
            update(
              workflow,
              Updates.set("phase", InterviewWorkflowPhase.CancelPending.toString),
              Updates.set("generation", Int.box(generation))
            ).as(workflow.copy(phase = InterviewWorkflowPhase.CancelPending, generation = generation))
      }
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop) *> use(subject)
    }
}
