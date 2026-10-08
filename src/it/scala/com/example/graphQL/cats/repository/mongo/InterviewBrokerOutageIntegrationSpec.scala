package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Ref}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.infrastructure.kafka.{InterviewBrokerFixture, InterviewKafkaRuntime}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.service.application.{
  ApplicationService,
  InterviewSchedulingService,
  InterviewWorkflowWorker,
  InterviewWorkerSettings
}
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import com.mongodb.client.model.{Filters, Updates}
import java.util.UUID
import scala.concurrent.duration.*

final class InterviewBrokerOutageIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private def success[E, A](operation: IO[Either[E, A]]): IO[A] = operation.flatMap {
    case Right(value) => IO.pure(value)
    case Left(error)  => IO.raiseError(new AssertionError(s"Hiring operation failed: $error"))
  }

  private def awaitCompleted(repository: InterviewWorkflowRepository, id: InterviewWorkflowId): IO[Unit] =
    success(repository.findForAdmin(id).value).flatMap {
      case Some(value) if value.phase == InterviewWorkflowPhase.Completed      => IO.unit
      case Some(value) if value.phase == InterviewWorkflowPhase.RepairRequired =>
        IO.raiseError(new AssertionError("Interview unexpectedly requires repair"))
      case _ => IO.sleep(100.millis) *> awaitCompleted(repository, id)
    }

  test("Mongo hiring and scheduling continue during a dedicated broker process outage and recover once") {
    InterviewBrokerFixture.resource(InterviewBrokerFixture.uniqueTopics).use { broker =>
      mongoResource.use { fixture =>
        val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
        val users = MongoUserRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop
        )
        val jobs = MongoJobRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop
        )
        val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
        val hiring = new ApplicationService(
          users,
          jobs,
          applications,
          Idempotent(MongoMutationReceiptRepository.transactional(fixture.database, fixture.client, Diagnostics.noop))
        )
        val scheduling = new InterviewSchedulingService(users, jobs, applications, repository, 3.minutes)
        val worker = new InterviewWorkflowWorker(
          repository,
          FakeInterviewCalendarProvider.durable(repository),
          FakeInterviewNotificationProvider.durable(repository),
          InterviewWorkerSettings("broker-outage-worker", 100.millis, 30.seconds, 5.seconds, 5, 100.millis, 1.second),
          Diagnostics.noop
        )
        for {
          now <- IO.realTimeInstant
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop, broker.topics)
          template = InterviewWorkflow
            .create(
              InterviewWorkflowId(UUID.randomUUID()),
              ApplicationId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              InterviewInterval(now.plusSeconds(3600), now.plusSeconds(3660)),
              now.plusSeconds(180),
              UUID.randomUUID(),
              ApplicationStatus.Accepted
            )
            .fold(error => fail(s"Invalid fixture: $error"), identity)
          _ <- InterviewSchedulingFixtures.seed(fixture.database, List(template), now)
          collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Applications)
          _ <- collection.updateOne(
            Filters.eq("_id", template.applicationId.value.toString),
            Updates.set("status", ApplicationStatus.Created.toString)
          )
          attemptedOffline <- Deferred[IO, Unit]
          published <- Ref.of[IO, List[InterviewMessage]](Nil)
          replayTarget <- Ref.of[IO, Option[UUID]](None)
          replayProcessed <- Deferred[IO, Unit]
          scheduled <- {
            def invalid(identity: String): IO[Boolean] =
              IO.realTimeInstant.flatMap(at => repository.quarantine(identity, at).value.map(_.isRight))
            val transports = for {
              orchestrator <- InterviewKafkaRuntime.resource(broker.config(worker = false), Diagnostics.noop) {
                case Left(identity) => invalid(identity)
                case Right(message) => worker.receiveResult(message)
              }
              executor <- InterviewKafkaRuntime.resource(broker.config(worker = true), Diagnostics.noop) {
                case Left(identity) => invalid(identity)
                case Right(message) =>
                  worker.receiveCommand(message).flatTap { accepted =>
                    replayTarget.get.flatMap { target =>
                      if (accepted && target.contains(message.messageId)) replayProcessed.complete(()).void
                      else IO.unit
                    }
                  }
              }
              routing = new InterviewTransport {
                def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
                  (if (message.result.nonEmpty) executor else orchestrator).generationFor(message)
                def publish(message: InterviewMessage): IO[Unit] =
                  attemptedOffline.complete(()).void *>
                    (if (message.result.nonEmpty) executor else orchestrator).publish(message) *>
                    published.update(_ :+ message)
              }
              _ <- worker.publisher(routing)
            } yield orchestrator
            transports.use { commandPublisher =>
              for {
                _ <- broker.stopProcess
                actor = ActorContext(template.recruiterId, UserRole.Recruiter)
                request = IdempotencyRequest.fromCanonicalInput(UUID.randomUUID(), "accept-during-outage")
                accepted <- success(
                  hiring
                    .changeStatus(request, actor, template.applicationId, ApplicationStatus.Accepted, None, None)
                    .value
                ).timeout(5.seconds)
                _ = assertEquals(accepted.status, ApplicationStatus.Accepted)
                workflow <- success(
                  scheduling
                    .schedule(
                      actor,
                      template.applicationId,
                      template.interval.startsAt,
                      template.interval.endsAt,
                      template.idempotencyKey
                    )
                    .value
                ).timeout(5.seconds)
                repeated <- success(
                  scheduling
                    .schedule(
                      actor,
                      template.applicationId,
                      template.interval.startsAt,
                      template.interval.endsAt,
                      template.idempotencyKey
                    )
                    .value
                ).timeout(5.seconds)
                _ = assertEquals(repeated.id, workflow.id)
                _ <- attemptedOffline.get.timeout(10.seconds)
                pending <- success(repository.findForAdmin(workflow.id).value)
                _ = assertEquals(pending.map(_.phase), Some(InterviewWorkflowPhase.ReservationPending))
                reservationsBefore <- MongoRepositoryTestSupport.count(
                  fixture.database,
                  MongoCollections.FakeInterviewCalendarReservations
                )
                notificationsBefore <- MongoRepositoryTestSupport.count(
                  fixture.database,
                  MongoCollections.FakeInterviewNotificationReceipts
                )
                _ = assertEquals(reservationsBefore, 0L)
                _ = assertEquals(notificationsBefore, 0L)
                _ <- broker.restartProcess
                _ <- awaitCompleted(repository, workflow.id).timeout(90.seconds)
                application <- success(applications.find(template.applicationId).value)
                _ = assertEquals(application.map(_.status), Some(ApplicationStatus.Interview))
                messages <- published.get
                command <- IO
                  .fromOption(messages.find(_.result.isEmpty))(new AssertionError("No command was published"))
                _ <- replayTarget.set(Some(command.messageId))
                _ <- commandPublisher.publish(command)
                _ <- replayProcessed.get.timeout(30.seconds)
                afterReplay <- success(
                  scheduling
                    .schedule(
                      actor,
                      template.applicationId,
                      template.interval.startsAt,
                      template.interval.endsAt,
                      template.idempotencyKey
                    )
                    .value
                )
                _ = assertEquals(afterReplay.id, workflow.id)
              } yield workflow
            }
          }
          finalState <- success(repository.findForAdmin(scheduled.id).value)
          reservations <- MongoRepositoryTestSupport.count(
            fixture.database,
            MongoCollections.FakeInterviewCalendarReservations
          )
          notifications <- MongoRepositoryTestSupport.count(
            fixture.database,
            MongoCollections.FakeInterviewNotificationReceipts
          )
          history <- MongoRepositoryTestSupport.count(
            fixture.database,
            MongoCollections.ApplicationEvents,
            Filters.eq("applicationId", template.applicationId.value.toString)
          )
          workflowCount <- MongoRepositoryTestSupport.count(
            fixture.database,
            MongoCollections.InterviewWorkflows,
            Filters.eq("documentType", "workflow")
          )
          requestReceipts <- MongoRepositoryTestSupport.count(
            fixture.database,
            MongoCollections.InterviewWorkflows,
            Filters.eq("documentType", "requestReceipt")
          )
        } yield {
          assertEquals(finalState.map(_.phase), Some(InterviewWorkflowPhase.Completed))
          assertEquals(workflowCount, 1L)
          assertEquals(requestReceipts, 1L)
          assertEquals(reservations, 1L)
          assertEquals(notifications, 2L)
          assertEquals(history, 2L)
        }
      }
    }
  }
}
