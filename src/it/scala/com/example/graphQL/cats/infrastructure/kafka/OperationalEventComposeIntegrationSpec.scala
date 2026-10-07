package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import com.example.hiring.testing.LocalTestServices
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
import com.mongodb.client.model.Filters
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import java.time.{Duration, Instant}
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Exercises the verified isolated test Mongo/Kafka services with fresh per-case namespaces. */
class OperationalEventComposeIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 2.minutes

  private def enabled = kafkaEvidenceEnabled
  private def mongoUri = mongoEndpoint.uri
  private val actors = Vector.tabulate(10)(index => UserId(new UUID(0L, 0x301L + index)))
  private def kafka = KafkaConfig(
    enabled = true,
    bootstrapServers = kafkaNamespace.manifest.kafkaBootstrap,
    topic = kafkaNamespace.events,
    consumerGroup = kafkaNamespace.eventGroup,
    publisher = KafkaPublisherConfig(
      "compose-evidence-writer",
      50,
      30,
      1,
      10,
      100,
      Some("hiring_publisher_v2"),
      Some(kafkaNamespace.manifest.publisherPassword)
    ),
    consumer = KafkaConsumerConfig(
      enabled = true,
      receiptTtlDays = 8,
      quarantineTtlDays = 7,
      saslUsername = Some("analytics_reader"),
      saslPassword = Some(kafkaNamespace.manifest.readerPassword)
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

  test("operational topic accepts a valid escaped skill fact larger than the interview message limit") {
    assume(enabled, "BLOCKED: isolated test service manifest absent")
    val manifest = kafkaNamespace.manifest
    val skills = (0 until 100).map(index => "\u0001" * 197 + index.toString).toSet
    val value = job(JobId(UUID.randomUUID()), Instant.now(), actors.head).copy(skills = skills)
    val event = OperationalEvents
      .jobEvent(OperationalEventType.JOB_CREATED, UUID.randomUUID(), value, actors.head, value.createdAt)
      .fold(error => fail(error.toString), identity)
    val bytes = com.example.graphQL.cats.service.events.OperationalEventJson.bytes(event)
    assert(bytes.length > 65536)
    assert(bytes.length <= com.example.graphQL.cats.service.events.OperationalEventJson.MaxEnvelopeBytes)
    val properties = LocalTestServices.adminProperties(manifest, "hiring_publisher_v2", manifest.publisherPassword)
    val _ = properties.put("key.serializer", classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
    val _ =
      properties.put("value.serializer", classOf[org.apache.kafka.common.serialization.ByteArraySerializer].getName)
    val _ = properties.put("acks", "all")
    val producer = cats.effect.Resource.make(
      IO.blocking(new org.apache.kafka.clients.producer.KafkaProducer[String, Array[Byte]](properties))
    )(value => IO.blocking(value.close()))
    val admin = cats.effect.Resource.make(
      IO.blocking(
        org.apache.kafka.clients.admin.Admin
          .create(LocalTestServices.adminProperties(manifest, "broker", manifest.brokerPassword))
      )
    )(value => IO.blocking(value.close()))
    admin.use { value =>
      IO.blocking {
        val resources = List(kafkaNamespace.events, kafkaNamespace.commands, kafkaNamespace.results).map(name =>
          new org.apache.kafka.common.config.ConfigResource(
            org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
            name
          )
        )
        val configs = value.describeConfigs(resources.asJava).all().get(10L, java.util.concurrent.TimeUnit.SECONDS)
        assertEquals(configs.get(resources.head).get("max.message.bytes").value(), "1048576")
        resources.tail.foreach(resource =>
          assertEquals(configs.get(resource).get("max.message.bytes").value(), "65536")
        )
      } *> producer.use { client =>
        IO.blocking {
          val metadata = client
            .send(
              new org.apache.kafka.clients.producer.ProducerRecord(kafkaNamespace.events, event.partitionKey, bytes)
            )
            .get(10L, java.util.concurrent.TimeUnit.SECONDS)
          assertEquals(metadata.topic(), kafkaNamespace.events)
          assert(metadata.offset() >= 0L)
        }
      }
    }
  }

  test("local Compose publishes 100 facts and records receipt p95") {
    assume(enabled, "BLOCKED: isolated test service manifest absent")
    if (!enabled) IO.raiseError(new IllegalStateException("Isolated service manifest absent"))
    else if (kafka.publisher.saslPassword.isEmpty || kafka.consumer.saslPassword.isEmpty)
      IO.raiseError(
        new IllegalStateException("compose evidence requires KAFKA_PUBLISHER_V2_PASSWORD and KAFKA_READER_PASSWORD")
      )
    else {
      MongoDatabaseProbe.clientResource(mongoUri).use { client =>
        LocalTestServices.database(client).use { database =>
          val jobs = MongoJobRepository.transactional(
            database,
            client,
            new MongoEmbeddingWorkRepository(database, com.example.graphQL.cats.service.Diagnostics.noop),
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val outbox = MongoOperationalEventOutboxRepository.transactional(
            database,
            client,
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val receipts = new MongoConsumerReceiptRepository(database, com.example.graphQL.cats.service.Diagnostics.noop)
          val quarantine =
            new MongoEventQuarantineRepository(database, com.example.graphQL.cats.service.Diagnostics.noop)
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
                val event = OperationalEvents
                  .jobEvent(
                    OperationalEventType.JOB_CREATED,
                    UUID.randomUUID(),
                    jobValue,
                    owner,
                    jobValue.createdAt
                  )
                  .fold(error => fail(error.toString), identity)
                jobs
                  .createWithEvents(
                    jobValue,
                    jobValue.createdAt,
                    List(event),
                    com.example.graphQL.cats.service.port.MutationWriteContext.directWrite
                  )
                  .value
                  .map(result => (event.eventId, result))
              }
            }
            (events, evidence, work)
          }
          MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop) *> diagnosticsIO
            .flatMap { case (observedEvents, evidence, work) =>
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
                  _ = assert(subjectFence.isDefined)
                  _ = assert(subjectFence.forall(!_.containsKey("transactionalIds")))
                  registrations <- MongoRepositoryTestSupport.collectWithin(
                    database
                      .getCollection(MongoProducerRegistrations.Collection)
                      .find(
                        Filters.and(
                          Filters.in("subjectId", actors.map(_.value.toString).asJava),
                          Filters.eq("kind", "Operational"),
                          Filters.eq("state", "Active")
                        )
                      )
                      .limit(100),
                    101
                  )
                  _ = assertEquals(
                    registrations.map(_.getString("subjectId")).toSet,
                    actors.map(_.value.toString).toSet
                  )
                  _ = registrations.foreach { registration =>
                    val transactionalId = registration.getString("transactionalId")
                    assert(transactionalId != null && transactionalId.startsWith("hiring-publisher-"))
                    assertEquals(
                      registration.getString("_id"),
                      s"${registration.getString("subjectId")}:$transactionalId"
                    )
                    assertEquals(
                      UUID.fromString(transactionalId.stripPrefix("hiring-publisher-")).toString,
                      transactionalId.stripPrefix("hiring-publisher-")
                    )
                    assert(registration.getDate("registeredAt") != null)
                    assert(!registration.containsKey("expiresAt"))
                  }
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
                    s"analyticsRange database=${database.underlying.getName} topic=${kafka.topic} partition=0 start=$startOffset end=$endOffset fixtureEvents=100"
                  )
                  println(
                    s"Operational event evidence: consumerGroup=${kafka.consumerGroup}, p95CommitToReceiptMs=$p95"
                  )
                }
              }
            }
        }
      }
    }
  }
}
