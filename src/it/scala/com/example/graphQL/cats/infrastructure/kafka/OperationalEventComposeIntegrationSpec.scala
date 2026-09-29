package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.mongo.MongoRepositoryTestSupport.*
import com.example.graphQL.cats.config.{
  KafkaConfig,
  KafkaConsumerConfig,
  KafkaPublisherConfig,
  KafkaSaslSecurityProtocol
}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.infrastructure.kafka.OperationalEventKafkaRuntime
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.service.events.{OperationalEventType, OperationalEvents}
import munit.CatsEffectSuite
import com.mongodb.client.model.Filters
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import java.time.{Duration, Instant}
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Runs against the local `compose.yaml` Mongo replica set and Kafka broker. It is opt-in because the normal
  * integration suite owns disposable Mongo containers.
  */
class OperationalEventComposeIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 2.minutes

  private val enabled = sys.props.get("phase5.compose.evidence").contains("true") ||
    sys.env.get("PHASE5_COMPOSE_EVIDENCE").contains("true")
  private val mongoUri = sys.props.getOrElse(
    "phase5.compose.mongo-uri",
    "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
  )
  private val databaseName = sys.env
    .get("ANALYTICS_COMPOSE_DATABASE")
    .orElse(sys.props.get("analytics.compose.database"))
    .getOrElse(s"analytics_compose_${UUID.randomUUID().toString.replace('-', '_')}")
  private val actors = Vector.tabulate(10)(index => UserId(new UUID(0L, 0x301L + index)))
  private val kafka = KafkaConfig(
    enabled = true,
    bootstrapServers = sys.props.getOrElse("phase5.compose.kafka", "127.0.0.1:9092"),
    topic = "hiring.operational-events",
    consumerGroup = "hiring-operational-events-integration",
    publisher = KafkaPublisherConfig(
      "compose-evidence-writer",
      50,
      30,
      1,
      10,
      100,
      Some("hiring_publisher_v2"),
      sys.env.get("KAFKA_PUBLISHER_V2_PASSWORD")
    ),
    consumer = KafkaConsumerConfig(
      enabled = true,
      receiptTtlDays = 8,
      quarantineTtlDays = 7,
      saslUsername = Some("analytics_reader"),
      saslPassword = sys.env.get("KAFKA_READER_PASSWORD")
    ),
    saslSecurityProtocol = KafkaSaslSecurityProtocol.Plaintext
  )

  private def job(id: JobId, createdAt: Instant, owner: UserId): Job = Job(
    id,
    owner,
    s"Evidence job ${id.value}",
    "Compose telemetry evidence job",
    List("Scala"),
    Set("Scala"),
    Location("Cyprus", "Nicosia", remote = true),
    JobStatus.Open,
    createdAt,
    createdAt
  )

  private def readCommittedEndOffset: IO[Long] = IO.blocking {
    val properties = new Properties()
    properties.put("bootstrap.servers", kafka.bootstrapServers)
    properties.put("group.id", s"analytics-range-evidence-${UUID.randomUUID()}")
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.put("security.protocol", "SASL_PLAINTEXT")
    properties.put("sasl.mechanism", "PLAIN")
    properties.put(
      "sasl.jaas.config",
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${kafka.consumer.saslUsername.get}\" password=\"${kafka.consumer.saslPassword.get}\";"
    )
    properties.put("isolation.level", "read_committed")
    val consumer = new KafkaConsumer[String, Array[Byte]](properties)
    val partition = new TopicPartition(kafka.topic, 0)
    try {
      consumer.assign(Collections.singleton(partition))
      consumer.endOffsets(Collections.singleton(partition)).get(partition)
    } finally consumer.close()
  }

  private def waitForReceipts(
      database: mongo4cats.database.MongoDatabase[IO],
      eventIds: Set[String]
  ): IO[Unit] = {
    val receipts = database.getCollection(MongoCollections.ConsumerReceipts)
    def loop(deadline: Instant): IO[Unit] =
      MongoRepositoryTestSupport
        .collectWithin(
          receipts
            .find(
              Filters.and(
                Filters.eq("consumerGroup", kafka.consumerGroup),
                Filters.in("eventId", eventIds.toList.asJava)
              )
            )
            .limit(eventIds.size),
          eventIds.size + 1
        )
        .flatMap { values =>
          if (values.size >= eventIds.size) IO.unit
          else
            IO.realTimeInstant.flatMap(now =>
              if (!now.isBefore(deadline))
                IO.raiseError(new AssertionError(s"only ${values.size}/${eventIds.size} receipts"))
              else IO.sleep(250.millis) *> loop(deadline)
            )
        }
    IO.realTimeInstant.flatMap(now => loop(now.plusSeconds(60)))
  }

  test("local Compose publishes 100 facts and records receipt p95") {
    if (!enabled) IO.unit
    else if (kafka.publisher.saslPassword.isEmpty || kafka.consumer.saslPassword.isEmpty)
      IO.raiseError(
        new IllegalStateException("compose evidence requires KAFKA_PUBLISHER_V2_PASSWORD and KAFKA_READER_PASSWORD")
      )
    else {
      MongoDatabaseProbe.clientResource(mongoUri).use { client =>
        client.getDatabase(databaseName).flatMap { database =>
          val jobs = MongoJobRepository.transactional(database, client, new MongoEmbeddingWorkRepository(database))
          val outbox = MongoOperationalEventOutboxRepository.transactional(database, client)
          val receipts = new MongoConsumerReceiptRepository(database)
          val quarantine = new MongoEventQuarantineRepository(database)
          val diagnosticEvents = Ref.of[IO, Vector[String]](Vector.empty)
          val diagnosticsIO = diagnosticEvents.map { events =>
            val diagnostics = new Diagnostics {
              override def event(
                  event: LogEvent,
                  requestId: Option[String],
                  fields: => Map[LogField, String]
              ): IO[Unit] = events.update(
                _ :+ s"${event.category}:${fields.getOrElse(LogField.ErrorType, "unknown")}:${fields.getOrElse(LogField.ErrorLocation, "unavailable")}"
              )
            }
            val evidence = OperationalEventKafkaRuntime.resource(kafka, outbox, receipts, quarantine, diagnostics)
            val work = (0 until 100).toList.traverse { index =>
              IO.realTimeInstant.flatMap { createdAt =>
                val owner = actors(index % actors.size)
                val jobValue = job(JobId(UUID.randomUUID()), createdAt, owner)
                val event = OperationalEvents.jobEvent(
                  OperationalEventType.JOB_CREATED,
                  UUID.randomUUID(),
                  jobValue,
                  owner,
                  jobValue.createdAt
                )
                jobs
                  .createWithEvents(
                    jobValue,
                    jobValue.createdAt,
                    List(event),
                    com.example.graphQL.cats.service.port.MutationWriteContext.directWrite
                  )
                  .map(result => (event.eventId, result))
              }
            }
            (events, evidence, work)
          }
          MongoHiringSetup.initialize(database) *> diagnosticsIO.flatMap { case (observedEvents, evidence, work) =>
            evidence.use { _ =>
              for {
                startOffset <- readCommittedEndOffset
                writes <- work
                _ = assert(writes.forall(_._2.isRight), clues(writes.count(_._2.isRight)))
                eventIds = writes.map(_._1.toString).toSet
                receiptResult <- waitForReceipts(database, eventIds).attempt
                observedDiagnostics <- observedEvents.get
                _ = assert(
                  receiptResult.isRight,
                  clues(
                    s"${receiptResult.swap.toOption.map(_.getMessage)} diagnostics=${observedDiagnostics.mkString(",")}"
                  )
                )
                _ <- IO.fromEither(receiptResult.leftMap(identity))
                endOffset <- readCommittedEndOffset
                _ = assert(endOffset > startOffset, clues(startOffset, endOffset))
                subjectFence <- MongoRepositoryTestSupport.first(
                  database
                    .getCollection(MongoCollections.OutboxSubjectFences)
                    .find(Filters.eq("_id", actors.head.value.toString))
                )
                transactionalIds = subjectFence.toList.flatMap(
                  _.getList("transactionalIds", classOf[String]).asScala.toList
                )
                _ = assert(transactionalIds.nonEmpty && transactionalIds.forall(_.startsWith("hiring-publisher-")))
                outboxRows <- MongoRepositoryTestSupport.collectWithin(
                  database
                    .getCollection(MongoCollections.EventOutbox)
                    .find(
                      Filters.and(
                        Filters.eq("state", "Published"),
                        Filters.in("_id", eventIds.toList.asJava)
                      )
                    )
                    .limit(100),
                  101
                )
                receiptRows <- MongoRepositoryTestSupport.collectWithin(
                  database
                    .getCollection(MongoCollections.ConsumerReceipts)
                    .find(
                      Filters.and(
                        Filters.eq("consumerGroup", kafka.consumerGroup),
                        Filters.in("eventId", eventIds.toList.asJava)
                      )
                    )
                    .limit(100),
                  120
                )
                p95 <- IO {
                  val receiptById =
                    receiptRows.map(row => row.get("eventId").toString -> row.getDate("createdAt").toInstant).toMap
                  val latencies = outboxRows.flatMap { row =>
                    Option(row.getDate("createdAt")).flatMap(created =>
                      receiptById
                        .get(row.get("_id").toString)
                        .map(received => Duration.between(created.toInstant, received).toMillis)
                    )
                  }.sorted
                  assertEquals(latencies.size, 100)
                  latencies((latencies.size * 95 + 99) / 100 - 1)
                }
              } yield {
                assert(p95 < 30000L, clues(p95))
                println(
                  s"analyticsRange database=$databaseName topic=${kafka.topic} partition=0 start=$startOffset end=$endOffset fixtureEvents=100"
                )
                println(s"Operational event evidence: consumerGroup=${kafka.consumerGroup}, p95CommitToReceiptMs=$p95")
              }
            }
          }
        }
      }
    }
  }
}
