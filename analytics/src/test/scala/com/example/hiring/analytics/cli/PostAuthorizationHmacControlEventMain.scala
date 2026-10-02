package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.adapter.mongo.{
  MongoAnalyticsLakehouseLock,
  MongoHmacKeyRetirementAuthorizationStore,
  MongoPublisherStream
}
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.{AnalyticsTopic, PartitionOffsetRange, SubjectPseudonymizer}
import com.example.hiring.analytics.errors.AnalyticsError
import com.mongodb.{ReadConcern, WriteConcern}
import com.mongodb.client.MongoClients
import io.circe.Json
import org.apache.kafka.clients.admin.{Admin, ListOffsetsOptions, OffsetSpec}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.common.serialization.StringSerializer
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, Properties, UUID}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

/** Isolated, post-authorization restart control. Never opens Spark or changes the retained lakehouse or images. */
object PostAuthorizationHmacControlEventMain extends IOApp {
  private val Collection = "analytics_hmac_restart_control_events"
  private val NewControlSubjectId = "084c58fe-787b-410b-b1bb-7193931f06e3"
  private def invalid = AnalyticsError.InvalidConfiguration("isolated restart control evidence is invalid")

  private[cli] def coordinate(
      row: Document,
      expected: PartitionOffsetRange
  ): Either[AnalyticsError, PartitionOffsetRange] =
    for {
      topic <- Option(row.get("topic")).collect { case value: String => value }.toRight(invalid)
      partition <- Option(row.get("partition"))
        .collect { case value: java.lang.Integer => value.intValue }
        .toRight(invalid)
      start <- Option(row.get("startOffset")).collect { case value: java.lang.Long => value.longValue }.toRight(invalid)
      end <- Option(row.get("endOffsetExclusive"))
        .collect { case value: java.lang.Long => value.longValue }
        .toRight(invalid)
      _ <- Either.cond(
        topic == AnalyticsTopic.unwrap(
          expected.topic
        ) && partition == expected.partition && end > start && end - start == 1L,
        (),
        invalid
      )
      range <- PartitionOffsetRange.from(topic, partition, start, end).toEither.leftMap(_ => invalid)
    } yield range

  private def available(admin: Admin, range: PartitionOffsetRange): Boolean = {
    val partition = new TopicPartition(AnalyticsTopic.unwrap(range.topic), range.partition)
    val options = new ListOffsetsOptions(IsolationLevel.READ_COMMITTED).timeoutMs(30000)
    val earliest = admin
      .listOffsets(Map[TopicPartition, OffsetSpec](partition -> OffsetSpec.earliest()).asJava, options)
      .all()
      .get(35L, TimeUnit.SECONDS)
      .get(partition)
      .offset()
    val latest = admin
      .listOffsets(Map[TopicPartition, OffsetSpec](partition -> OffsetSpec.latest()).asJava, options)
      .all()
      .get(35L, TimeUnit.SECONDS)
      .get(partition)
      .offset()
    earliest >= 0L && earliest <= range.startOffset && range.endOffsetExclusive <= latest
  }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      _ <- IO.raiseUnless(args.isEmpty)(invalid)
      settings <- AnalyticsRuntimeConfig.loadBatch[IO]
      common = settings.common
      original <- IO.fromOption(
        settings.manifest.offsetRanges.headOption.filter(_ => settings.manifest.offsetRanges.size == 1)
      )(invalid)
      _ <- IO.raiseUnless(
        original.endOffsetExclusive > original.startOffset && original.endOffsetExclusive - original.startOffset == 1L
      )(invalid)
      oldKeyId <- IO.fromOption(sys.env.get("HIRING_HMAC_ROTATION_OLD_KEY_ID"))(invalid)
      oldSecret <- IO.fromOption(sys.env.get("HIRING_HMAC_ROTATION_OLD_SECRET_BASE64"))(invalid)
      shift <- IO.fromEither(
        HmacRetirementProofCalendar
          .shift(common.lakehouseRoot, common.mongoDatabase, AnalyticsTopic.unwrap(original.topic), oldKeyId)
      )
      _ <- IO.raiseUnless(shift > 0L && common.hmac.keyId == oldKeyId.replace("rotation-old-", "rotation-new-"))(
        invalid
      )
      oldRing <- IO.fromEither(
        SubjectPseudonymizer.validateFromBase64(Some(oldSecret), oldKeyId, None, None).toEither.leftMap(_ => invalid)
      )
      verifier <- IO.fromOption(oldRing.keyVerifiers.find(_._1 == oldKeyId).map(_._2))(invalid)
      lakehouseId <- IO.fromEither(MongoAnalyticsLakehouseLock.lockId(common.lakehouseRoot))
      _ <- AppModule.mongoClient[IO](common.mongoUri).use { client =>
        client.getDatabase(common.mongoDatabase).flatMap { database =>
          new MongoHmacKeyRetirementAuthorizationStore[IO](database, new MongoPublisherStream(common.operational))
            .list(common.lakehouseRoot)
            .flatMap(records =>
              IO.raiseUnless(records.exists(record => record.keyId == oldKeyId && record.originalVerifier == verifier))(
                invalid
              )
            )
        }
      }
      kafkaProperties <- IO.fromEither(KafkaClientProperties.clientProperties(common.kafka))
      properties <- IO.delay {
        val value = new Properties()
        kafkaProperties.foreach { case (key, setting) => value.setProperty(key, setting) }
        value.setProperty("bootstrap.servers", common.kafka.bootstrapServers)
        value.setProperty("key.serializer", classOf[StringSerializer].getName)
        value.setProperty("value.serializer", classOf[StringSerializer].getName)
        value.setProperty("enable.idempotence", "true")
        value.setProperty("acks", "all")
        value.setProperty("delivery.timeout.ms", "30000")
        value
      }
      result <- Resource
        .make(IO.blocking(MongoClients.create(common.mongoUri)))(client => IO.blocking(client.close()))
        .use { mongo =>
          Resource
            .make(IO.blocking(Admin.create(properties)))(admin =>
              IO.blocking(admin.close(java.time.Duration.ofSeconds(5)))
            )
            .use { admin =>
              IO.blocking {
                val collection = mongo
                  .getDatabase(common.mongoDatabase)
                  .withReadConcern(ReadConcern.MAJORITY)
                  .getCollection(Collection)
                  .withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS))
                val binding = new Document("lakehouseId", lakehouseId)
                  .append("oldKeyId", oldKeyId)
                  .append("newKeyId", common.hmac.keyId)
                  .append("stage", "post-authorization-restart-control")
                val rows =
                  collection.find(binding).sort(new Document("observedAt", -1).append("_id", -1)).limit(1).iterator()
                val retained = try if (rows.hasNext) Some(rows.next()) else None
                finally rows.close()
                val candidate = retained.fold(original)(row => coordinate(row, original).fold(throw _, identity))
                if (available(admin, candidate)) candidate -> false
                else {
                  if (collection.countDocuments(binding) >= 32L) throw invalid
                  val at = Instant.now().truncatedTo(ChronoUnit.MILLIS)
                  val eventId = UUID.randomUUID().toString
                  val jobId = UUID.randomUUID().toString
                  val payload = Json
                    .obj(
                      "eventId" -> Json.fromString(eventId),
                      "eventType" -> Json.fromString("JOB_CREATED"),
                      "occurredAt" -> Json.fromString(at.toString),
                      "aggregateType" -> Json.fromString("Job"),
                      "aggregateId" -> Json.fromString(jobId),
                      "actorId" -> Json.fromString(NewControlSubjectId),
                      "payload" -> Json.obj("job" -> Json.obj("skills" -> Json.arr(Json.fromString("Scala"))))
                    )
                    .noSpaces
                  val producer = new KafkaProducer[String, String](properties)
                  val metadata = try {
                    val value = producer
                      .send(
                        new ProducerRecord[String, String](
                          AnalyticsTopic.unwrap(original.topic),
                          original.partition,
                          eventId,
                          payload
                        )
                      )
                      .get(35L, TimeUnit.SECONDS)
                    producer.flush()
                    value
                  } finally producer.close(java.time.Duration.ofSeconds(5))
                  if (
                    metadata.topic() != AnalyticsTopic.unwrap(original.topic) || metadata
                      .partition() != original.partition || metadata.offset() == Long.MaxValue
                  ) throw invalid
                  val row = new Document("_id", eventId)
                    .append("lakehouseId", lakehouseId)
                    .append("oldKeyId", oldKeyId)
                    .append("newKeyId", common.hmac.keyId)
                    .append("stage", "post-authorization-restart-control")
                    .append("observedAt", Date.from(at))
                    .append("topic", metadata.topic())
                    .append("partition", Int.box(metadata.partition()))
                    .append("startOffset", Long.box(metadata.offset()))
                    .append("endOffsetExclusive", Long.box(metadata.offset() + 1L))
                  collection.insertOne(row)
                  val stored = collection.find(new Document("_id", eventId)).first()
                  if (stored == null || stored != row) throw invalid
                  val fresh = coordinate(stored, original).fold(throw _, identity)
                  if (!available(admin, fresh)) throw invalid
                  fresh -> true
                }
              }
            }
        }
      (range, refreshed) = result
      _ <- IO.println(
        s"NEW_EVENT_RANGE=${AnalyticsTopic.unwrap(range.topic)}:${range.partition}:${range.startOffset}:${range.endOffsetExclusive}"
      )
      _ <- IO.println(s"RESTART_CONTROL_REFRESHED=$refreshed")
    } yield ExitCode.Success).handleErrorWith(error =>
      IO.println(s"ISOLATED_RESTART_CONTROL_FAILED class=${error.getClass.getSimpleName}").as(ExitCode.Error)
    )
}
