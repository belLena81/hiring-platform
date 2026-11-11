package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.infrastructure.kafka.{InterviewBrokerFixture, InterviewKafkaRuntime}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HiringReadService}
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

  private def awaitCompleted(repository: InterviewWorkflowRepository[IO], id: InterviewWorkflowId): IO[Unit] =
    success(repository.findForAdmin(id).value).flatMap {
      case Some(value) if value.phase == InterviewWorkflowPhase.Completed      => IO.unit
      case Some(value) if value.phase == InterviewWorkflowPhase.RepairRequired =>
        IO.raiseError(new AssertionError("Interview unexpectedly requires repair"))
      case _ => IO.sleep(100.millis) *> awaitCompleted(repository, id)
    }

  test("Mongo hiring and scheduling continue during a dedicated broker process outage and recover once") {
    InterviewBrokerFixture.resource(InterviewBrokerFixture.uniqueTopics).use { broker =>
      mongoResource.use { fixture =>
        val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop[IO])
        val users = MongoUserRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop[IO]
        )
        val jobs = MongoJobRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop[IO]
        )
        val applications =
          MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
        val hiring = new ApplicationService(
          users,
          jobs,
          applications,
          HiringReadService(users, jobs, applications),
          Idempotent(
            MongoMutationReceiptRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
          ),
          cats.effect.Clock[IO],
          cats.effect.std.UUIDGen[IO]
        )
        val scheduling = new InterviewSchedulingService[IO](
          users,
          jobs,
          applications,
          repository,
          Idempotent(
            MongoMutationReceiptRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
          ),
          3.minutes
        )
        val worker = new InterviewWorkflowWorker[IO](
          repository,
          LedgerInterviewCalendarProvider.durable(repository),
          LedgerInterviewNotificationProvider.durable(repository),
          InterviewWorkerSettings("broker-outage-worker", 100.millis, 30.seconds, 5.seconds, 5, 100.millis, 1.second),
          Diagnostics.noop[IO]
        )
        for {
          now <- IO.realTimeInstant
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO], broker.topics)
          template = InterviewWorkflow
            .create(
              InterviewWorkflowId(UUID.randomUUID()),
              ApplicationId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              InterviewInterval(now.plusSeconds(3600), now.plusSeconds(3660)),
              now.plusSeconds(180),
              UUID.randomUUID(),
              ApplicationStatus.Accepted,
              UserId(UUID.randomUUID()),
              com.example.graphQL.cats.domain.model.UserRole.Recruiter
            )
            .map(workflow => workflow.copy(initiatedBy = workflow.recruiterId))
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
            def invalid(identity: String): RepositoryF[IO, Unit] =
              RepositoryF.lift(IO.realTimeInstant).flatMap(repository.quarantine(identity, _))
            val transports = for {
              orchestrator <- InterviewKafkaRuntime.resource(broker.config(worker = false), Diagnostics.noop[IO]) {
                case Left(identity) => invalid(identity)
                case Right(message) => worker.receiveResult(message)
              }
              executor <- InterviewKafkaRuntime.resource(broker.config(worker = true), Diagnostics.noop[IO]) {
                case Left(identity) => invalid(identity)
                case Right(message) =>
                  worker.receiveCommand(message).semiflatTap { _ =>
                    replayTarget.get.flatMap { target =>
                      if (target.contains(message.messageId)) replayProcessed.complete(()).void
                      else IO.unit
                    }
                  }
              }
              routing = new InterviewTransport[IO] {
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
                  MongoCollections.InterviewCalendarReservations
                )
                notificationsBefore <- MongoRepositoryTestSupport.count(
                  fixture.database,
                  MongoCollections.InterviewNotificationReceipts
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
            MongoCollections.InterviewCalendarReservations
          )
          notifications <- MongoRepositoryTestSupport.count(
            fixture.database,
            MongoCollections.InterviewNotificationReceipts
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
            MongoCollections.MutationReceipts,
            Filters.eq("operation", "interviewWorkflow")
          )
          legacyRequestReceipts <- MongoRepositoryTestSupport.count(
            fixture.database,
            MongoCollections.InterviewWorkflows,
            Filters.eq("documentType", "requestReceipt")
          )
          _ <- ApplicationStatusInvariant.assertAgrees(fixture.database, template.applicationId)
        } yield {
          assertEquals(finalState.map(_.phase), Some(InterviewWorkflowPhase.Completed))
          assertEquals(workflowCount, 1L)
          assertEquals(requestReceipts, 1L)
          assertEquals(legacyRequestReceipts, 0L)
          assertEquals(reservations, 1L)
          assertEquals(notifications, 2L)
          assertEquals(history, 2L)
        }
      }
    }
  }

  import InterviewLifecycleScenario.{now, replacement}

  /** What the stopped broker saw: whether the publisher tried to publish while it was down. */
  private final case class OutageTransport(attempted: Deferred[IO, Unit])

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  /** Real broker transports around the scenario's counting providers, as the scheduling outage test wires them. */
  private def withBrokerTransport[A](
      s: InterviewLifecycleScenario,
      broker: InterviewBrokerFixture.Running,
      worker: InterviewWorkflowWorker[IO]
  )(use: OutageTransport => IO[A]): IO[A] =
    Deferred[IO, Unit].flatMap { attempted =>
      def invalid(identity: String): RepositoryF[IO, Unit] =
        RepositoryF.lift(IO.realTimeInstant).flatMap(s.repository.quarantine(identity, _))
      val transports = for {
        orchestrator <- InterviewKafkaRuntime.resource(broker.config(worker = false), Diagnostics.noop[IO]) {
          case Left(identity) => invalid(identity)
          case Right(message) => worker.receiveResult(message)
        }
        executor <- InterviewKafkaRuntime.resource(broker.config(worker = true), Diagnostics.noop[IO]) {
          case Left(identity) => invalid(identity)
          case Right(message) => worker.receiveCommand(message)
        }
        routing = new InterviewTransport[IO] {
          def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
            (if (message.result.nonEmpty) executor else orchestrator).generationFor(message)
          def publish(message: InterviewMessage): IO[Unit] =
            attempted.complete(()).void *>
              (if (message.result.nonEmpty) executor else orchestrator).publish(message)
        }
        _ <- worker.publisher(routing)
      } yield ()
      transports.use(_ => use(OutageTransport(attempted)))
    }

  /** Polls until `done` holds, sampling the application status each time so a wrong intermediate status is caught. */
  private def awaitSampling(
      s: InterviewLifecycleScenario,
      done: InterviewWorkflow => IO[Boolean]
  ): IO[(InterviewWorkflow, List[Option[String]])] = {
    def loop(samples: List[Option[String]]): IO[(InterviewWorkflow, List[Option[String]])] =
      for {
        workflow <- s.current
        status <- s.applicationStatus
        finished <- done(workflow)
        next = status :: samples
        result <-
          if (workflow.phase == InterviewWorkflowPhase.RepairRequired)
            IO.raiseError(new AssertionError("Interview unexpectedly requires repair"))
          else if (finished) IO.pure((workflow, next))
          else IO.sleep(100.millis) *> loop(next)
      } yield result
    loop(Nil).timeout(90.seconds)
  }

  private def reservationCount(s: InterviewLifecycleScenario): IO[Long] =
    s.count(MongoCollections.InterviewCalendarReservations, Filters.eq("workflowId", s.id.value.toString))

  test(
    "DHW-28 DHW-12 a cancel committed while the broker process is stopped keeps the application rejected and converges once after restart"
  ) {
    InterviewBrokerFixture.resource(InterviewTopicPair.Default).use { broker =>
      scenario { s =>
        val worker = s.worker("broker-outage-cancel-worker")
        val key = InterviewWorkflow.cancellationKey(s.id, 0)
        withBrokerTransport(s, broker, worker) { transport =>
          for {
            _ <- broker.stopProcess
            // Given the broker is down, when the candidate cancels, then the Mongo commit still succeeds promptly.
            cancelling <- s
              .applied(
                InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
                s.actor(UserRole.Candidate, s.workflow.candidateId),
                0L,
                now
              )
              .timeout(5.seconds)
            _ <- transport.attempted.get.timeout(10.seconds)
            // The publisher kept trying against the dead broker: no provider effect can have happened.
            offline <- (s.current, s.applicationStatus, s.isHeld(0), s.faults.cancelCalls.get, s.receipts).tupled
            _ <- broker.restartProcess
            converged <- awaitSampling(s, w => IO.pure(w.phase == InterviewWorkflowPhase.Cancelled))
            _ <- awaitSampling(s, _ => s.receipts.map(_.size >= 2))
            after <- (
              s.isReleased(0),
              s.applicationStatus,
              s.receipts,
              s.faults.confirmedCancels.get,
              s.faults.cancelCalls.get,
              s.faults.deliveries.get,
              reservationCount(s)
            ).tupled
            _ <- ApplicationStatusInvariant.assertAgrees(s.fixture.database, s.workflow.applicationId)
          } yield {
            assertEquals(cancelling.phase, InterviewWorkflowPhase.CancelPending)
            val (workflowOffline, statusOffline, heldOffline, callsOffline, receiptsOffline) = offline
            assertEquals(workflowOffline.phase, InterviewWorkflowPhase.CancelPending)
            assertEquals(statusOffline, Some("Rejected"))
            assert(heldOffline, "the slot stays held until the provider confirms, which needs the broker")
            assertEquals(callsOffline, Nil)
            assertEquals(receiptsOffline, Nil)
            val (released, status, receipts, confirmed, calls, deliveries, reservations) = after
            assertEquals(converged._1.phase, InterviewWorkflowPhase.Cancelled)
            assert(converged._2.forall(_.contains("Rejected")), clue(converged._2))
            assert(released)
            assertEquals(status, Some("Rejected"))
            assertEquals(confirmed, List(key), "the provider confirmed the cancel exactly once")
            assert(calls.nonEmpty && calls.forall(_ == key), "every provider attempt reused the same key")
            assertEquals(
              receipts.map(r => (r.kind, r.participant)).toSet,
              Set(
                (InterviewNotificationKind.Cancelled, InterviewParticipant.Candidate),
                (InterviewNotificationKind.Cancelled, InterviewParticipant.Recruiter)
              )
            )
            assertEquals(receipts.size, 2, "one durable receipt per recipient")
            assertEquals(deliveries.distinct.size, 2, "each recipient has one delivery identity")
            assertEquals(reservations, 1L)
          }
        }
      }
    }
  }

  test(
    "DHW-28 DHW-12 an accepted reschedule committed while the broker process is stopped keeps the application in Interview and swaps once after restart"
  ) {
    InterviewBrokerFixture.resource(InterviewTopicPair.Default).use { broker =>
      scenario { s =>
        val worker = s.worker("broker-outage-reschedule-worker")
        withBrokerTransport(s, broker, worker) { transport =>
          for {
            _ <- broker.stopProcess
            proposed <- s
              .applied(
                InterviewLifecycleEvent.Propose(
                  replacement.startsAt,
                  replacement.endsAt,
                  s.workflow.recruiterId,
                  now,
                  InterviewProposalTtl.Default
                ),
                s.actor(UserRole.Recruiter, s.workflow.recruiterId),
                0L,
                now
              )
              .timeout(5.seconds)
            accepted <- s
              .applied(
                InterviewLifecycleEvent.AcceptProposal(now),
                s.actor(UserRole.Candidate, s.workflow.candidateId),
                proposed.revision,
                now
              )
              .timeout(5.seconds)
            _ <- transport.attempted.get.timeout(10.seconds)
            offline <- (
              s.current,
              s.applicationStatus,
              s.isHeld(0),
              s.reservation(1),
              s.faults.holdCalls.get,
              s.faults.cancelCalls.get
            ).tupled
            _ <- broker.restartProcess
            converged <- awaitSampling(
              s,
              w => IO.pure(w.phase == InterviewWorkflowPhase.Completed && w.generation == 1)
            )
            // The proposal notice to the candidate and the two reschedule notices are plain due work after the swap.
            _ <- awaitSampling(s, _ => s.receipts.map(_.size >= 3))
            after <- (
              s.reservation(0),
              s.reservation(1),
              s.applicationStatus,
              s.receipts,
              s.faults.holdCalls.get,
              s.faults.confirmedCancels.get,
              s.faults.liveHolds.get,
              reservationCount(s)
            ).tupled
            // The arranged fixture sets `Interview` directly (no history row), and a reschedule must never write a
            // status: so the history must stay empty and the stored status unchanged.
            history <- ApplicationStatusInvariant.history(s.fixture.database, s.workflow.applicationId)
          } yield {
            assertEquals(history, Nil, "a reschedule never writes an application status or history row")
            assertEquals(accepted.phase, InterviewWorkflowPhase.RescheduleHoldPending)
            val (workflowOffline, statusOffline, oldHeldOffline, newOffline, holdsOffline, cancelsOffline) = offline
            assertEquals(workflowOffline.phase, InterviewWorkflowPhase.RescheduleHoldPending)
            assertEquals(workflowOffline.pendingInterval, Some(replacement))
            assertEquals(statusOffline, Some("Interview"))
            assert(oldHeldOffline, "the old slot is never given up while the broker is down")
            assertEquals(newOffline, None)
            assertEquals((holdsOffline, cancelsOffline), (Nil, Nil))
            val (oldHold, newHold, status, receipts, holds, confirmed, timeline, reservations) = after
            assertEquals(converged._1.interval, replacement)
            assertEquals(converged._1.pendingInterval, None)
            assert(converged._2.forall(_.contains("Interview")), clue(converged._2))
            assertEquals(status, Some("Interview"))
            assert(oldHold.exists(_.releasedAt.nonEmpty), "the old slot is cancelled last")
            assert(newHold.exists(_.releasedAt.isEmpty), "the replacement stays held")
            assertEquals(holds.distinct, List(InterviewWorkflow.reservationKey(s.id, 1)), "one replacement hold key")
            assertEquals(confirmed, List(InterviewWorkflow.cancellationKey(s.id, 0)), "the old slot is cancelled once")
            assertEquals(reservations, 2L, "one reservation per generation, no duplicates")
            assert(timeline.nonEmpty && timeline.forall(_ >= 1), clue(timeline))
            assertEquals(
              receipts.filter(_.kind == InterviewNotificationKind.Rescheduled).map(_.participant).toSet,
              Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)
            )
            assertEquals(receipts.size, 3, "one durable receipt per notification")
          }
        }
      }
    }
  }
}
