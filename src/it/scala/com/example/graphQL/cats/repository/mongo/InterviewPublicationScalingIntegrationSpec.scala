package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref, Resource}
import com.example.hiring.testing.LocalTestServices
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.infrastructure.kafka.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.application.{InterviewWorkflowWorker, InterviewWorkerSettings}
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.{CountOptions, Filters}
import fs2.Stream
import io.circe.Json
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.{Collections, UUID}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Explicit serialized comparison of publication scheduling, not end-to-end interview/provider throughput. */
final class InterviewPublicationScalingIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 20.minutes
  private val seed = 20261007L
  private val warmup = 100
  private val measured = 500
  private def identity(value: String): UUID =
    UUID.nameUUIDFromBytes(s"$seed:$value".getBytes(StandardCharsets.UTF_8))

  private def success[A](effect: RepositoryIO[A]): IO[A] = effect.value.flatMap {
    case Right(value) => IO.pure(value)
    case Left(error)  => IO.raiseError(new AssertionError(s"Publication repository failed: $error"))
  }

  private def publisherConfig = InterviewKafkaConfig(
    kafkaNamespace.manifest.kafkaBootstrap,
    "interview_command_publisher",
    kafkaNamespace.manifest.orchestratorPassword,
    KafkaSaslSecurityProtocol.Plaintext,
    worker = false,
    topics = interviewTopics,
    workerGroup = kafkaNamespace.workers,
    orchestratorGroup = kafkaNamespace.orchestrator
  )

  private def workflows(cohort: String, count: Int, at: Instant): List[InterviewWorkflow] =
    (0 until count).toList.map { n =>
      InterviewWorkflow
        .create(
          InterviewWorkflowId(identity(s"$cohort:workflow:$n")),
          ApplicationId(identity(s"$cohort:application:$n")),
          UserId(identity(s"$cohort:candidate:$n")),
          UserId(identity(s"$cohort:recruiter")),
          InterviewInterval(at.plusSeconds(3600L + n * 120L), at.plusSeconds(3660L + n * 120L)),
          at.plusSeconds(300),
          identity(s"$cohort:request:$n"),
          ApplicationStatus.Accepted
        )
        .fold(error => fail(s"Invalid publication fixture: $error"), value => value)
    }

  private def seedWork(
      fixture: MongoAccessEvaluationSupport.Fixture,
      repository: MongoInterviewWorkflowRepository,
      batch: List[InterviewWorkflow],
      at: Instant
  ): IO[Unit] =
    InterviewSchedulingFixtures.seed(fixture.database, batch, at) *> batch.traverse_(workflow =>
      success(
        repository.create(
          workflow,
          InterviewWorkflow.initialCommand(workflow),
          workflow.idempotencyKey,
          MutationReceiptFingerprint.fromCanonicalInput(workflow.id.value.toString),
          at
        )
      )
    )

  private def waitPublished(fixture: MongoAccessEvaluationSupport.Fixture, ids: Set[String]): IO[Unit] =
    Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowCommands).flatMap { rows =>
      rows
        .count(
          Filters.and(Filters.in("workflowId", ids.asJava), Filters.eq("commandState", "Published")),
          new CountOptions()
        )
        .flatMap { count =>
          if (count == ids.size.toLong) IO.unit else IO.sleep(50.millis) *> waitPublished(fixture, ids)
        }
    }

  private def brokerRecords(expected: Set[String]): IO[Vector[UUID]] = {
    val config = publisherConfig
    val properties = LocalTestServices.adminProperties(
      kafkaNamespace.manifest,
      "interview_result_publisher",
      kafkaNamespace.manifest.workerPassword
    )
    val _ = properties.put("bootstrap.servers", config.bootstrapServers)
    val _ = properties.put("group.id", kafkaNamespace.workers)
    val _ = properties.put("enable.auto.commit", "false")
    val _ = properties.put("isolation.level", "read_committed")
    val _ = properties.put("key.deserializer", classOf[StringDeserializer].getName)
    val _ = properties.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    Resource
      .make(IO.blocking(new org.apache.kafka.clients.consumer.KafkaConsumer[String, Array[Byte]](properties)))(
        consumer => IO.blocking(consumer.close())
      )
      .use { consumer =>
        val partition = new TopicPartition(interviewTopics.commands, 0)
        def collect(found: Vector[UUID]): IO[Vector[UUID]] =
          if (found.size >= expected.size) IO.pure(found)
          else
            IO.blocking(consumer.poll(java.time.Duration.ofMillis(250))).flatMap { records =>
              val selected = records
                .iterator()
                .asScala
                .toVector
                .filter(row => expected(row.key()))
                .map(row => InterviewMessageCodec.parse(row.value()).fold(reason => fail(reason), _.messageId))
              collect(found ++ selected)
            }
        IO.blocking {
          consumer.assign(Collections.singleton(partition))
          consumer.seekToBeginning(Collections.singleton(partition))
        } *> collect(Vector.empty).timeout(30.seconds)
      }
  }

  private def worker(repository: MongoInterviewWorkflowRepository): InterviewWorkflowWorker =
    new InterviewWorkflowWorker(
      repository,
      LedgerInterviewCalendarProvider.durable(repository),
      LedgerInterviewNotificationProvider.durable(repository),
      InterviewWorkerSettings("publication-measurement", 1.second, 60.seconds, 10.seconds, 5, 1.second, 30.seconds),
      Diagnostics.noop
    )

  private def drain(
      fixture: MongoAccessEvaluationSupport.Fixture,
      repository: MongoInterviewWorkflowRepository,
      batch: List[InterviewWorkflow],
      legacy: Boolean
  ): IO[(Vector[(Double, Double)], Double)] =
    InterviewKafkaRuntime.publisherResource(publisherConfig).use { transport =>
      for {
        completions <- Ref.of[IO, Vector[(Double, Double)]](Vector.empty)
        start <- IO.monotonic
        routing = new InterviewTransport {
          def generationFor(message: InterviewMessage) = transport.generationFor(message)
          def publish(message: InterviewMessage) = for {
            before <- IO.monotonic
            _ <- transport.publish(message)
            after <- IO.monotonic
            _ <- completions.update(_ :+ ((after - start).toNanos / 1000000d, (after - before).toNanos / 1000000d))
          } yield ()
        }
        interpreter = worker(repository)
        loop =
          if (legacy) Stream.repeatEval(interpreter.publishDue(routing)).metered(1.second).void
          else interpreter.publicationStream(routing)
        _ <- Resource.make(loop.compile.drain.start)(_.cancel).use { fiber =>
          IO.race(fiber.joinWithNever, waitPublished(fixture, batch.map(_.id.value.toString).toSet))
            .flatMap {
              case Right(_) => IO.unit
              case Left(_)  => IO.raiseError(new AssertionError("Publication loop stopped before backlog completed"))
            }
            .timeout(3.minutes)
        }
        end <- IO.monotonic
        observed <- completions.get
      } yield (observed, (end - start).toNanos / 1000000000d)
    }

  private def percentile(values: Vector[Double], fraction: Double): Double =
    values.sorted.lift((math.ceil(values.size * fraction).toInt - 1).max(0)).getOrElse(0d)

  private def observe(pair: Int, legacy: Boolean): IO[Json] = mongoResource.use { fixture =>
    val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
    val label = s"pair-$pair-${if (legacy) "legacy" else "immediate"}"
    for {
      at <- IO.realTimeInstant
      _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop, interviewTopics)
      initial = workflows(s"$label-warmup", warmup, at)
      _ <- seedWork(fixture, repository, initial, at)
      _ <- drain(fixture, repository, initial, legacy)
      batch = workflows(s"$label-measured", measured, at)
      _ <- seedWork(fixture, repository, batch, at)
      _ <- fixture.commands.clear
      resourcesBefore <- fixture.sampleResources
      result <- drain(fixture, repository, batch, legacy)
      resourcesAfter <- fixture.sampleResources
      commands <- fixture.commands.snapshot
      records <- brokerRecords(batch.map(_.id.value.toString).toSet)
      (completed, elapsed) = result
      _ = assertEquals(completed.size, measured)
      _ = assertEquals(records.size, measured)
      _ = assertEquals(records.distinct.size, measured)
      names = commands.groupBy(command =>
        List("find", "findAndModify", "update", "commitTransaction", "abortTransaction", "aggregate", "getMore")
          .find(command.containsKey)
          .getOrElse("other")
      )
      latencies = completed.map(_._1)
    } yield Json.obj(
      "pair" -> Json.fromInt(pair),
      "scheduler" -> Json.fromString(if (legacy) "legacy" else "immediate"),
      "seed" -> Json.fromLong(seed),
      "warmup" -> Json.fromInt(warmup),
      "measured" -> Json.fromInt(measured),
      "concurrentPublishers" -> Json.fromInt(1),
      "batchSize" -> Json.fromInt(16),
      "pollIntervalMs" -> Json.fromInt(1000),
      "published" -> Json.fromInt(completed.size),
      "brokerUniqueRecords" -> Json.fromInt(records.distinct.size),
      "kafkaTransactions" -> Json.fromInt(completed.size),
      "unexpectedErrors" -> Json.fromInt(0),
      "elapsedSeconds" -> Json.fromDoubleOrNull(elapsed),
      "publishedPerSecond" -> Json.fromDoubleOrNull(measured / elapsed),
      "backlogCompletionP50Ms" -> Json.fromDoubleOrNull(percentile(latencies, .5d)),
      "backlogCompletionP95Ms" -> Json.fromDoubleOrNull(percentile(latencies, .95d)),
      "backlogCompletionP99Ms" -> Json.fromDoubleOrNull(percentile(latencies, .99d)),
      "oldestBacklogAgeAtDrainMs" -> Json.fromDoubleOrNull(latencies.maxOption.getOrElse(0d)),
      "kafkaTransactionP95Ms" -> Json.fromDoubleOrNull(percentile(completed.map(_._2), .95d)),
      "mongoCommands" -> Json.obj(names.toList.map { case (name, rows) => name -> Json.fromInt(rows.size) }*),
      "resourcesBefore" -> resourcesBefore,
      "resourcesAfter" -> resourcesAfter
    )
  }

  test("paired publication scheduling measurement preserves all durable commands and committed broker records") {
    assume(java.lang.Boolean.getBoolean("hiring.scaling.measure"), "Explicit serialized measurement opt-in required")
    assume(kafkaEvidenceEnabled, "Verified isolated Kafka test manifest required")
    (1 to 3).toList
      .traverse { pair =>
        val modes = if (pair % 2 == 1) List(true, false) else List(false, true)
        modes.traverse(legacy => observe(pair, legacy))
      }
      .flatMap { comparisons =>
        IO.blocking {
          val destination = Path.of(".local/data/interview-publication-scaling.json")
          val _ = Files.createDirectories(destination.getParent)
          val _ = Files.writeString(
            destination,
            Json
              .obj(
                "scope" -> Json.fromString("initial command publication only; no provider or end-to-end SLO"),
                "comparisons" -> Json.arr(comparisons.flatten*)
              )
              .spaces2
          )
        }
      }
  }
}
