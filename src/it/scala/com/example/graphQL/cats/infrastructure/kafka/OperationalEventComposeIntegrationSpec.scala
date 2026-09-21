package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.{KafkaConfig, KafkaConsumerConfig, KafkaPublisherConfig}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.infrastructure.kafka.OperationalEventKafkaRuntime
import com.example.graphQL.cats.shared.events.{OperationalEventType, OperationalEvents}
import munit.CatsEffectSuite
import org.bson.Document
import com.mongodb.client.model.Filters

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/**
  * Runs against the local `compose.yaml` Mongo replica set and Kafka broker.
  * It is opt-in because the normal integration suite owns disposable Mongo containers.
  */
class OperationalEventComposeIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 2.minutes

  private val enabled = sys.props.get("phase5.compose.evidence").contains("true") ||
    sys.env.get("PHASE5_COMPOSE_EVIDENCE").contains("true")
  private val mongoUri = sys.props.getOrElse(
    "phase5.compose.mongo-uri",
    "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
  )
  private val databaseName = s"phase5_evidence_${UUID.randomUUID().toString.replace('-', '_')}"
  private val actor = UserId(UUID.fromString("00000000-0000-0000-0000-000000000301"))
  private val kafka = KafkaConfig(
    enabled = true,
    bootstrapServers = sys.props.getOrElse("phase5.compose.kafka", "127.0.0.1:9092"),
    topic = "hiring.operational-events",
    consumerGroup = s"phase5-evidence-${UUID.randomUUID()}",
    publisher = KafkaPublisherConfig("compose-evidence-writer", 50, 30, 1, 10, 100),
    consumer = KafkaConsumerConfig(enabled = true, receiptTtlDays = 8, quarantineTtlDays = 7)
  )

  private def job(id: JobId, createdAt: Instant): Job = Job(
    id,
    actor,
    s"Evidence job ${id.value}",
    "Compose telemetry evidence job",
    List("Scala"),
    Set("Scala"),
    Location("Cyprus", "Nicosia", remote = true),
    JobStatus.Open,
    createdAt,
    createdAt
  )

  private def waitForReceipts(database: com.mongodb.reactivestreams.client.MongoDatabase, eventIds: Set[String]): IO[Unit] = {
    val receipts = database.getCollection("consumer_receipts")
    def loop(deadline: Instant): IO[Unit] =
      PublisherBridge.collectWithin(receipts.find(Filters.and(
        Filters.eq("consumerGroup", kafka.consumerGroup),
        Filters.in("eventId", eventIds.toList.asJava)
      )).limit(eventIds.size), eventIds.size + 1).flatMap { values =>
        if (values.size >= eventIds.size) IO.unit
        else IO.realTimeInstant.flatMap(now =>
          if (!now.isBefore(deadline)) IO.raiseError(new AssertionError(s"only ${values.size}/${eventIds.size} receipts"))
          else IO.sleep(250.millis) *> loop(deadline)
        )
      }
    IO.realTimeInstant.flatMap(now => loop(now.plusSeconds(60)))
  }

  test("local Compose publishes 100 facts and records receipt p95") {
    if (!enabled) IO.unit
    else {
      MongoDatabaseProbe.clientResource(mongoUri).use { client =>
        val database = client.getDatabase(databaseName)
        val jobs = MongoJobRepository.transactional(database, client)
        val outbox = new MongoOperationalEventOutboxRepository(database)
        val receipts = new MongoConsumerReceiptRepository(database)
        val quarantine = new MongoEventQuarantineRepository(database)
        val evidence = OperationalEventKafkaRuntime.resource(kafka, outbox, receipts, quarantine)
        val work = (0 until 100).toList.traverse { _ =>
          IO.realTimeInstant.flatMap { createdAt =>
            val jobValue = job(JobId(UUID.randomUUID()), createdAt)
            val event = OperationalEvents.jobEvent(
              OperationalEventType.JOB_CREATED,
              UUID.randomUUID(),
              jobValue,
              actor,
              jobValue.createdAt
            )
            jobs.createWithEvents(jobValue, jobValue.createdAt, List(event)).map(result => (event.eventId, result))
          }
        }
        evidence.use { _ =>
          for {
            writes <- work
            _ = assert(writes.forall(_._2.isRight), clues(writes.count(_._2.isRight)))
            eventIds = writes.map(_._1.toString).toSet
            _ <- waitForReceipts(database, eventIds)
            outboxRows <- PublisherBridge.collectWithin(database.getCollection("event_outbox").find(
              new Document("state", "Published")
            ), 120)
            receiptRows <- PublisherBridge.collectWithin(database.getCollection("consumer_receipts")
              .find(Filters.and(
                Filters.eq("consumerGroup", kafka.consumerGroup),
                Filters.in("eventId", eventIds.toList.asJava)
              )).limit(100), 120)
            p95 <- IO {
              val receiptById = receiptRows.map(row => row.get("eventId").toString -> row.getDate("createdAt").toInstant).toMap
              val latencies = outboxRows.flatMap { row =>
                Option(row.getDate("createdAt")).flatMap(created =>
                  receiptById.get(row.get("_id").toString).map(received => Duration.between(created.toInstant, received).toMillis)
                )
              }.sorted
              assertEquals(latencies.size, 100)
              latencies((latencies.size * 95 + 99) / 100 - 1)
            }
          } yield {
            assert(p95 < 30000L, clues(p95))
            println(s"Phase 5 local evidence: events=100, consumerGroup=${kafka.consumerGroup}, p95CommitToReceiptMs=$p95")
          }
        }
      }
    }
  }
}
