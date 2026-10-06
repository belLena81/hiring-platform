package com.example.graphQL.cats.repository.mongo

import cats.data.EitherT
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.infrastructure.kafka.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.application.{InterviewWorkflowWorker, InterviewWorkerSettings}
import com.example.graphQL.cats.service.port.*
import io.circe.Json
import munit.CatsEffectSuite
import org.bson.Document
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Opt-in proof uses an isolated SASL broker; it never modifies the normal local topics. */
final class InterviewSchedulingWorkerIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 8.minutes

  private def success[A](operation: RepositoryIO[A]): IO[A] = operation.value.flatMap {
    case Right(value) => IO.pure(value)
    case Left(error)  => IO.raiseError(new AssertionError(s"Interview repository failure: $error"))
  }

  private def configuration: IO[Map[String, String]] = IO.blocking {
    val file =
      Path.of(sys.env.getOrElse("INTERVIEW_KAFKA_PROOF_ROOT", ".local/data/interview-kafka-proof"), "runtime.env")
    if (!Files.exists(file)) Map.empty
    else
      Files
        .readString(file)
        .linesIterator
        .filter(line => line.nonEmpty && !line.startsWith("#"))
        .flatMap(line =>
          line.split("=", 2).toList match {
            case key :: value :: Nil =>
              Some(
                key.stripPrefix("export ").trim -> value.trim
                  .stripPrefix("'")
                  .stripSuffix("'")
                  .stripPrefix("\"")
                  .stripSuffix("\"")
              )
            case _ => None
          }
        )
        .toMap
  }

  private def waitCompleted(
      repository: InterviewWorkflowRepository,
      workflows: List[InterviewWorkflow],
      completionTimes: Ref[IO, Map[UUID, Long]],
      startedNanos: Long,
      backlog: Ref[IO, List[Json]],
      remaining: Int = 600
  ): IO[Unit] =
    for {
      states <- workflows.traverse(value => success(repository.findForAdmin(value.id)))
      now <- IO.monotonic
      pending = states.flatten.filterNot(_.phase == InterviewWorkflowPhase.Completed)
      _ <- backlog.update(samples =>
        (samples :+ Json.obj(
          "elapsedMillis" -> Json.fromLong((now.toNanos - startedNanos) / 1000000L),
          "pendingWorkflows" -> Json.fromInt(pending.size),
          "oldestRequestAgeMillis" -> Json.fromLong(
            if (pending.nonEmpty) (now.toNanos - startedNanos) / 1000000L else 0L
          ),
          "phases" -> Json.obj(states.flatten.groupMapReduce(_.phase.toString)(_ => 1)(_ + _).toList.map {
            case (phase, count) => phase -> Json.fromInt(count)
          }*)
        )).takeRight(600)
      )
      finished = states.flatten.filter(_.phase == InterviewWorkflowPhase.Completed).map(_.id.value)
      _ <- completionTimes.update(existing =>
        existing ++ finished
          .filterNot(existing.contains)
          .map(id => id -> ((now.toNanos - startedNanos) / 1000000L))
      )
      _ <-
        if (finished.size == workflows.size) IO.unit
        else if (remaining <= 0)
          backlog.get.flatMap(samples =>
            IO.blocking {
              Files.writeString(
                Path.of(".local/data/interview-scheduling-failure.json"),
                Json
                  .obj(
                    "samples" -> Json.arr(samples*),
                    "workflows" -> Json.arr(
                      states.flatten.map(w =>
                        Json.obj(
                          "id" -> Json.fromString(w.id.value.toString),
                          "phase" -> Json.fromString(w.phase.toString),
                          "revision" -> Json.fromLong(w.revision)
                        )
                      )*
                    )
                  )
                  .spaces2
              )
            }
          ) *> IO.raiseError(
            new AssertionError(
              s"Unfinished interviews: ${states.flatten.map(_.phase).groupMapReduce(identity)(_ => 1)(_ + _)}"
            )
          )
        else
          IO.sleep(200.millis) *> waitCompleted(
            repository,
            workflows,
            completionTimes,
            startedNanos,
            backlog,
            remaining - 1
          )
    } yield ()

  private def workers(
      repository: MongoInterviewWorkflowRepository,
      env: Map[String, String],
      calendarOverride: Option[InterviewCalendarProvider] = None,
      notificationOverride: Option[InterviewNotificationProvider] = None,
      counters: Ref[IO, Map[String, Long]]
  ): Resource[IO, Unit] = {
    def count(key: String): IO[Unit] = counters.update(values => values.updated(key, values.getOrElse(key, 0L) + 1L))
    val underlyingCalendar = calendarOverride.getOrElse(FakeInterviewCalendarProvider.durable(repository))
    val underlyingNotifications = notificationOverride.getOrElse(FakeInterviewNotificationProvider.durable(repository))
    val calendar = new InterviewCalendarProvider {
      override def reserve(
          id: InterviewWorkflowId,
          key: String,
          candidate: UserId,
          recruiter: UserId,
          interval: InterviewInterval,
          at: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand]
      ): InterviewProviderIO[InterviewCalendarReservation] =
        EitherT.liftF[IO, InterviewProviderError, Unit](count("calendarReserveRequests")) *> underlyingCalendar.reserve(
          id,
          key,
          candidate,
          recruiter,
          interval,
          at,
          execution
        )
      override def lookup(id: InterviewWorkflowId): InterviewProviderIO[Option[InterviewCalendarReservation]] =
        EitherT.liftF[IO, InterviewProviderError, Unit](count("calendarLookupRequests")) *> underlyingCalendar.lookup(
          id
        )
      override def release(
          key: String,
          at: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand]
      ): InterviewProviderIO[Unit] =
        EitherT.liftF[IO, InterviewProviderError, Unit](count("calendarReleaseRequests")) *> underlyingCalendar.release(
          key,
          at,
          execution
        )
    }
    val notifications = new InterviewNotificationProvider {
      override def notify(
          id: InterviewWorkflowId,
          recipient: UserId,
          participant: InterviewParticipant,
          key: String,
          at: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand]
      ): InterviewProviderIO[InterviewNotificationReceipt] =
        EitherT.liftF[IO, InterviewProviderError, Unit](
          count("notificationDeliveryRequests")
        ) *> underlyingNotifications.notify(id, recipient, participant, key, at, execution)
      override def lookup(key: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
        EitherT.liftF[IO, InterviewProviderError, Unit](count("notificationLookupRequests")) *> underlyingNotifications
          .lookup(key)
    }
    val settings =
      InterviewWorkerSettings("interview-worker-a", 100.millis, 60.seconds, 10.seconds, 5, 1.second, 30.seconds)
    val first = new InterviewWorkflowWorker(repository, calendar, notifications, settings)
    val second =
      new InterviewWorkflowWorker(repository, calendar, notifications, settings.copy(workerId = "interview-worker-b"))
    val bootstrap = env("INTERVIEW_KAFKA_BOOTSTRAP")
    def invalid(identity: String): IO[Boolean] =
      IO.realTimeInstant.flatMap(now => repository.quarantine(identity, now).value.map(_.isRight))
    for {
      orchestrator <- InterviewKafkaRuntime.resource(
        InterviewKafkaConfig(
          bootstrap,
          "interview_command_publisher",
          env("KAFKA_INTERVIEW_ORCHESTRATOR_PASSWORD"),
          KafkaSaslSecurityProtocol.Plaintext,
          false
        ),
        Diagnostics.noop
      ) {
        case Left(identity) => invalid(identity)
        case Right(message) => count("brokerResultsConsumed") *> first.receiveResult(message)
      }
      workerTransport <- InterviewKafkaRuntime.resource(
        InterviewKafkaConfig(
          bootstrap,
          "interview_result_publisher",
          env("KAFKA_INTERVIEW_WORKER_PASSWORD"),
          KafkaSaslSecurityProtocol.Plaintext,
          true
        ),
        Diagnostics.noop
      ) {
        case Left(identity) => invalid(identity)
        case Right(message) => count("brokerCommandsConsumed") *> first.receiveCommand(message)
      }
      _ <- InterviewKafkaRuntime.resource(
        InterviewKafkaConfig(
          bootstrap,
          "interview_result_publisher",
          env("KAFKA_INTERVIEW_WORKER_PASSWORD"),
          KafkaSaslSecurityProtocol.Plaintext,
          true
        ),
        Diagnostics.noop
      ) {
        case Left(identity) => invalid(identity)
        case Right(message) => count("brokerCommandsConsumed") *> second.receiveCommand(message)
      }
      routing = new InterviewTransport {
        override def generationFor(message: InterviewMessage): InterviewPublisherGeneration =
          (if (message.result.nonEmpty) workerTransport else orchestrator).generationFor(message)
        override def publish(message: InterviewMessage): IO[Unit] =
          count(
            "brokerPublishRequests"
          ) *> (if (message.result.nonEmpty) workerTransport.publish(message) else orchestrator.publish(message))
      }
      _ <- first.publisher(routing)
      _ <- second.publisher(routing)
    } yield ()
  }

  test("32 interviews survive two worker resources and actual consumer publisher restart") {
    configuration.flatMap { env =>
      if (!env.get("INTERVIEW_KAFKA_EVIDENCE").contains("true"))
        IO.raiseError(new AssertionError("Isolated Kafka proof broker not configured"))
      else
        MongoAccessEvaluationSupport.resource.use { fixture =>
          val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
          for {
            now <- IO.realTimeInstant
            _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
            recruiter = UserId(UUID.randomUUID())
            workflows = (0 until 32).toList.map { ordinal =>
              val interval =
                InterviewInterval(now.plusSeconds(3600L + ordinal * 120L), now.plusSeconds(3660L + ordinal * 120L))
              InterviewWorkflow
                .create(
                  InterviewWorkflowId(UUID.randomUUID()),
                  ApplicationId(UUID.randomUUID()),
                  UserId(UUID.randomUUID()),
                  recruiter,
                  interval,
                  now.plusSeconds(300),
                  UUID.randomUUID(),
                  ApplicationStatus.Accepted
                )
                .fold(error => fail(s"Invalid fixture workflow: $error"), identity)
            }
            _ <- InterviewSchedulingFixtures.seed(fixture.database, workflows, now)
            storageBefore <- MongoAccessEvaluationSupport.command(fixture.database, new Document("dbStats", 1))
            samplesBefore <- fixture.sampleResources
            _ <- fixture.commands.clear
            counters <- Ref.of[IO, Map[String, Long]](Map.empty)
            backlog <- Ref.of[IO, List[Json]](Nil)
            start <- IO.monotonic
            completed <- Ref.of[IO, Map[UUID, Long]](Map.empty)
            persist = (batch: List[InterviewWorkflow]) =>
              batch.traverse_(workflow =>
                success(
                  repository.create(
                    workflow,
                    InterviewWorkflow.initialCommand(workflow),
                    workflow.idempotencyKey,
                    MutationReceiptFingerprint.fromCanonicalInput(workflow.id.value.toString),
                    now
                  )
                )
              )
            _ <- persist(workflows.take(16))
            _ <- persist(workflows.drop(16))
            reservedBeforeCrash <- Deferred[IO, InterviewCalendarReservation]
            calendar = FakeInterviewCalendarProvider.durable(repository)
            pausedCalendar = new InterviewCalendarProvider {
              override def reserve(
                  workflowId: InterviewWorkflowId,
                  key: String,
                  candidate: UserId,
                  recruiter: UserId,
                  interval: InterviewInterval,
                  at: Instant,
                  execution: Option[ClaimedInterviewWorkflowCommand]
              ): InterviewProviderIO[InterviewCalendarReservation] =
                calendar
                  .reserve(workflowId, key, candidate, recruiter, interval, at, execution)
                  .semiflatMap(receipt =>
                    reservedBeforeCrash.complete(receipt).void *> IO.never[InterviewCalendarReservation]
                  )
              override def lookup(id: InterviewWorkflowId): InterviewProviderIO[Option[InterviewCalendarReservation]] =
                calendar.lookup(id)
              override def release(
                  key: String,
                  at: Instant,
                  execution: Option[ClaimedInterviewWorkflowCommand]
              ): InterviewProviderIO[Unit] =
                calendar.release(key, at, execution)
            }
            _ <- workers(repository, env, Some(pausedCalendar), counters = counters).use { _ =>
              reservedBeforeCrash.get
                .timeout(60.seconds)
                .flatMap(receipt =>
                  success(repository.findForAdmin(receipt.workflowId))
                    .map(state => assertEquals(state.map(_.phase), Some(InterviewWorkflowPhase.ReservationPending)))
                )
            }
            deliveredBeforeCrash <- Deferred[IO, InterviewNotificationReceipt]
            notifications = FakeInterviewNotificationProvider.durable(repository)
            pausedNotifications = new InterviewNotificationProvider {
              override def notify(
                  id: InterviewWorkflowId,
                  recipient: UserId,
                  participant: InterviewParticipant,
                  key: String,
                  at: Instant,
                  execution: Option[ClaimedInterviewWorkflowCommand]
              ): InterviewProviderIO[InterviewNotificationReceipt] =
                notifications
                  .notify(id, recipient, participant, key, at, execution)
                  .semiflatMap(receipt =>
                    deliveredBeforeCrash.complete(receipt).void *> IO.never[InterviewNotificationReceipt]
                  )
              override def lookup(key: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
                notifications.lookup(key)
            }
            _ <- workers(repository, env, notificationOverride = Some(pausedNotifications), counters = counters).use {
              _ =>
                deliveredBeforeCrash.get
                  .timeout(120.seconds)
                  .flatMap(receipt =>
                    success(repository.hasHiringReceipt(receipt.workflowId)).map(assert(_)) *>
                      repository
                        .find(receipt.idempotencyKey)
                        .value
                        .map(result => assert(result.toOption.flatten.nonEmpty))
                  )
            }
            _ <- workers(repository, env, counters = counters)
              .use(_ => waitCompleted(repository, workflows, completed, start.toNanos, backlog))
            durations <- completed.get.map(_.values.toList.sorted)
            calendarCount <- Mongo4catsCollections
              .documents(fixture.database, MongoCollections.FakeInterviewCalendarReservations)
              .flatMap(_.count)
            notificationCount <- Mongo4catsCollections
              .documents(fixture.database, MongoCollections.FakeInterviewNotificationReceipts)
              .flatMap(_.count)
            errors <- Mongo4catsCollections
              .documents(fixture.database, MongoCollections.InterviewWorkflows)
              .flatMap(
                _.count(
                  MongoFilter.eq("phase", InterviewWorkflowPhase.RepairRequired.toString).bson,
                  new com.mongodb.client.model.CountOptions()
                )
              )
            after <- fixture.sampleResources
            commands <- fixture.commands.snapshot
            storage <- MongoAccessEvaluationSupport.command(fixture.database, new Document("dbStats", 1))
            counts <- counters.get
            backlogSamples <- backlog.get
            evidence = Json.obj(
              "workflows" -> Json.fromInt(32),
              "workers" -> Json.fromInt(2),
              "actualRestarts" -> Json.fromInt(2),
              "durableCalendarCrash" -> Json.True,
              "durableNotificationCrashAfterHiringCommit" -> Json.True,
              "calendarReservations" -> Json.fromLong(calendarCount),
              "notificationReceipts" -> Json.fromLong(notificationCount),
              "repairRequired" -> Json.fromLong(errors),
              "mongoWorkloadCommandsIncludingRequestPersistenceAndEvidenceReads" -> Json.fromInt(commands.size),
              "brokerAndProviderRequests" -> Json.obj(counts.toList.map { case (key, value) =>
                key -> Json.fromLong(value)
              }*),
              "errors" -> Json.fromLong(errors),
              "backlogSamples" -> Json.arr(backlogSamples*),
              "completionLatencyMillis" -> Json.obj(
                "p50" -> Json.fromLong(durations(15)),
                "p95" -> Json.fromLong(durations(30)),
                "p99" -> Json.fromLong(durations(31))
              ),
              "resourcesBefore" -> samplesBefore,
              "resourcesAfter" -> after,
              "mongoStorageBeforeBytes" -> Json.fromLong(
                storageBefore.get("storageSize", classOf[java.lang.Number]).longValue
              ),
              "mongoStorageAfterBytes" -> Json.fromLong(
                storage.get("storageSize", classOf[java.lang.Number]).longValue
              ),
              "mongoIndexBeforeBytes" -> Json.fromLong(
                storageBefore.get("indexSize", classOf[java.lang.Number]).longValue
              ),
              "mongoIndexAfterBytes" -> Json.fromLong(storage.get("indexSize", classOf[java.lang.Number]).longValue),
              "mongoStorageGrowthBytes" -> Json.fromLong(
                storage
                  .get("storageSize", classOf[java.lang.Number])
                  .longValue - storageBefore.get("storageSize", classOf[java.lang.Number]).longValue
              ),
              "mongoIndexGrowthBytes" -> Json.fromLong(
                storage
                  .get("indexSize", classOf[java.lang.Number])
                  .longValue - storageBefore.get("indexSize", classOf[java.lang.Number]).longValue
              ),
              "sloClaim" -> Json.False
            )
            _ <- IO.blocking {
              val destination = Path.of(".local/data/interview-scheduling-workload.json")
              Files.createDirectories(destination.getParent)
              Files.writeString(destination, evidence.spaces2)
            }
          } yield {
            assertEquals(calendarCount, 32L)
            assertEquals(notificationCount, 64L)
            assertEquals(errors, 0L)
          }
        }
    }
  }
}
