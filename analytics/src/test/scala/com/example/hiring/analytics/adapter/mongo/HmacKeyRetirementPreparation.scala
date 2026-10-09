package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.adapter.local.LocalProcess
import com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.{AnalyticsOffset, AnalyticsPartition, AnalyticsTopic}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.KafkaRetentionBarrier

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.mongodb.{ReadConcern, WriteConcern}
import com.mongodb.client.model.Projections
import io.circe.generic.auto.*
import mongo4cats.circe.MongoJsonCodecs
import mongo4cats.database.MongoDatabase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.bson.Document

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Captured after old-primary writers are excluded. All horizons start no earlier than this checkpoint. */
private[analytics] final case class HmacKeyRetirementPreparation(
    lakehouseId: String,
    keyId: String,
    originalVerifier: String,
    capturedAt: Instant,
    barrier: KafkaRetentionBarrier,
    lineage: HmacKeyRetirementKafkaLineage
)

private[analytics] object HmacKeyRetirementPreparation {
  private object circeCodecs extends MongoJsonCodecs
  import circeCodecs.*

  private[analytics] final case class PartitionRecord(number: Int, endOffsetExclusive: Long)
  private[analytics] final case class MongoRecord(
      _id: String,
      lakehouseId: String,
      keyId: String,
      originalVerifier: String,
      capturedAt: Instant,
      topic: String,
      partitions: Vector[PartitionRecord],
      clusterId: String,
      topicId: String,
      kafkaVolumeName: String,
      kafkaVolumeMountpoint: String,
      kafkaVolumeCreatedAt: String,
      kafkaBootstrapEndpoint: String
  )

  private[analytics] val mongoRecordRegistry: mongo4cats.codecs.CodecRegistry =
    AnalyticsMongoRecords.registry[MongoRecord](Set("endOffsetExclusive"), Set("number"))

  private[analytics] def decode(record: MongoRecord): Either[AnalyticsError, HmacKeyRetirementPreparation] = {
    val malformed = AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is malformed")
    for {
      _ <- Either.cond(
        record.lakehouseId.matches("[0-9a-f]{64}") && record.keyId.matches("[A-Za-z0-9-]{1,40}"),
        (),
        malformed
      )
      _ <- Either.cond(record._id == s"${record.lakehouseId}:${record.keyId}", (), malformed)
      _ <- Either.cond(record.originalVerifier.matches("[A-Za-z0-9_-]{43}"), (), malformed)
      barrier <- KafkaRetentionBarrier.from(
        record.topic,
        record.partitions.map(partition => partition.number -> partition.endOffsetExclusive)
      )
      lineage = HmacKeyRetirementKafkaLineage(
        record.clusterId,
        record.topicId,
        record.kafkaVolumeName,
        record.kafkaVolumeMountpoint,
        record.kafkaVolumeCreatedAt,
        record.kafkaBootstrapEndpoint
      )
      _ <- Either.cond(
        HmacKeyRetirementKafkaLineage.matches(lineage, lineage),
        (),
        AnalyticsError.InvalidConfiguration("HMAC key retirement Kafka lineage is malformed")
      )
    } yield HmacKeyRetirementPreparation(
      record.lakehouseId,
      record.keyId,
      record.originalVerifier,
      record.capturedAt,
      barrier,
      lineage
    )
  }
}

/** Broker and Docker storage identities must remain identical through the retention wait. */
private[analytics] final case class HmacKeyRetirementKafkaLineage(
    clusterId: String,
    topicId: String,
    volumeName: String,
    volumeMountpoint: String,
    volumeCreatedAt: String,
    bootstrapEndpoint: String
)

private[analytics] object HmacKeyRetirementKafkaLineage {
  def matches(captured: HmacKeyRetirementKafkaLineage, current: HmacKeyRetirementKafkaLineage): Boolean =
    captured == current &&
      captured.clusterId.nonEmpty && captured.topicId.nonEmpty &&
      captured.volumeName.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}") &&
      captured.volumeMountpoint.endsWith("/" + captured.volumeName + "/_data") &&
      captured.volumeCreatedAt.nonEmpty && captured.bootstrapEndpoint.matches("127\\.0\\.0\\.1:[0-9]{1,5}")

  /** The broker contacted at this loopback endpoint must be the running Compose Kafka container using this volume. */
  private[analytics] def bindingMatches(
      configuredEndpoint: String,
      publishedEndpoint: String,
      mounts: Vector[(String, String, String, Boolean)],
      volumeName: String
  ): Boolean =
    configuredEndpoint == publishedEndpoint &&
      mounts.count { case (kind, name, destination, writable) =>
        kind == "volume" && name == volumeName && destination == "/var/lib/kafka/data" && writable
      } == 1 &&
      !mounts.exists { case (kind, name, destination, _) =>
        destination == "/var/lib/kafka/data" && (kind != "volume" || name != volumeName)
      }

  private def docker[F[_]: Async](args: Vector[String]): F[String] =
    LocalProcess
      .run[F](
        args,
        "Kafka Docker inspection timed out",
        "Kafka Docker inspection could not start"
      )
      .flatMap(result =>
        Async[F].fromEither(
          Either.cond(
            result.exitCode == 0,
            result.output,
            AnalyticsError.InvalidConfiguration("Kafka Docker inspection failed")
          )
        )
      )

  private def dockerBrokerContainer[F[_]: Async](connection: KafkaConnection, volumeName: String): F[String] =
    for {
      _ <- Async[F].fromEither(
        Either.cond(
          volumeName.endsWith("_hmac-rotation-kafka") &&
            connection.bootstrapServers.matches("127\\.0\\.0\\.1:[0-9]{1,5}"),
          (),
          AnalyticsError.InvalidConfiguration("Kafka endpoint or isolated volume name is invalid")
        )
      )
      project = volumeName.stripSuffix("_hmac-rotation-kafka")
      idsOutput <- docker[F](
        Vector(
          "docker",
          "ps",
          "--no-trunc",
          "-q",
          "--filter",
          s"label=com.docker.compose.project=$project",
          "--filter",
          "label=com.docker.compose.service=kafka"
        )
      )
      ids = idsOutput.linesIterator.filter(_.nonEmpty).toVector
      _ <- Async[F].fromEither(
        Either.cond(
          ids.size == 1,
          (),
          AnalyticsError.InvalidConfiguration("exactly one isolated Compose Kafka container must be running")
        )
      )
      container = ids.head
      identityText <- docker[F](
        Vector(
          "docker",
          "inspect",
          "--format",
          "{{.Id}}|{{.State.Running}}|{{index .Config.Labels \"com.docker.compose.project\"}}|{{index .Config.Labels \"com.docker.compose.service\"}}",
          container
        )
      )
      identity = identityText.split("\\|", -1).toVector
      _ <- Async[F].fromEither(
        Either.cond(
          identity.size == 4 && identity(0) == container && identity(1) == "true" &&
            identity(2) == project && identity(3) == "kafka",
          (),
          AnalyticsError.InvalidConfiguration("isolated Kafka container identity changed")
        )
      )
      published <- docker[F](Vector("docker", "port", container, "29092/tcp"))
      mountsText <- docker[F](
        Vector(
          "docker",
          "inspect",
          "--format",
          "{{range .Mounts}}{{.Type}}|{{.Name}}|{{.Destination}}|{{.RW}}{{println}}{{end}}",
          container
        )
      )
      mounts <- Async[F].fromEither(mountsText.linesIterator.filter(_.nonEmpty).toVector.traverse { line =>
        line.split("\\|", -1).toVector match {
          case Vector(kind, name, destination, "true")  => Right((kind, name, destination, true))
          case Vector(kind, name, destination, "false") => Right((kind, name, destination, false))
          case _ => Left(AnalyticsError.InvalidConfiguration("isolated Kafka mount inventory is malformed"))
        }
      })
      _ <- Async[F].fromEither(
        Either.cond(
          bindingMatches(connection.bootstrapServers, published, mounts, volumeName),
          (),
          AnalyticsError.InvalidConfiguration("configured Kafka endpoint is not bound to the isolated Kafka volume")
        )
      )
    } yield container

  private def dockerVolume[F[_]: Async](volumeName: String): F[(String, String)] =
    for {
      _ <- Async[F].fromEither(
        Either.cond(
          volumeName.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"),
          (),
          AnalyticsError.InvalidConfiguration("isolated Kafka volume name is invalid")
        )
      )
      result <- docker[F](
        Vector("docker", "volume", "inspect", "--format", "{{.Driver}}|{{.Mountpoint}}|{{.CreatedAt}}", volumeName)
      )
      parts = result.split("\\|", -1).toVector
      _ <- Async[F].fromEither(
        Either.cond(
          parts.size == 3 && parts.head == "local" &&
            parts(1).endsWith("/" + volumeName + "/_data") && parts(2).nonEmpty,
          (),
          AnalyticsError.InvalidConfiguration("isolated Kafka Docker volume identity is unavailable")
        )
      )
    } yield parts(1) -> parts(2)

  def observe[F[_]: Async](
      connection: KafkaConnection,
      topic: String,
      volumeName: String,
      driverExecution: SparkBlockingExecution[F]
  ): F[HmacKeyRetirementKafkaLineage] =
    for {
      clientProperties <- Async[F].fromEither(KafkaClientProperties.clientProperties(connection))
      containerBefore <- dockerBrokerContainer[F](connection, volumeName)
      volume <- dockerVolume[F](volumeName)
      broker <- Resource
        .make(
          driverExecution
            .blocking {
              val properties = new Properties()
              properties.setProperty("bootstrap.servers", connection.bootstrapServers)
              properties.setProperty("request.timeout.ms", "10000")
              properties.setProperty("default.api.timeout.ms", "15000")
              clientProperties.foreach { case (key, value) => properties.setProperty(key, value) }
              val created = try Right(AdminClient.create(properties))
              catch {
                case NonFatal(error) =>
                  val cause = Option(error.getCause).getOrElse(error)
                  val privateValues = Vector(
                    Option(properties.getProperty("sasl.jaas.config")),
                    connection.saslPassword,
                    connection.saslUsername
                  ).flatten.filter(_.nonEmpty).sortBy(value => -value.length)
                  val diagnostic = privateValues
                    .foldLeft(Option(cause.getMessage).getOrElse("unavailable"))((message, value) =>
                      message.replace(value, "[REDACTED]")
                    )
                    .replaceAll("(?i)password\\s*=\\s*[^;\\s]+", "password=[REDACTED]")
                    .replaceAll("[\\r\\n]", " ")
                    .take(240)
                  Left(
                    AnalyticsError.InvalidConfiguration(
                      s"Kafka lineage admin client initialization failed (${cause.getClass.getSimpleName}: $diagnostic)"
                    )
                  )
              }
              created
            }
            .flatMap(Async[F].fromEither)
        )(client => driverExecution.blocking(client.close()).void)
        .use { client =>
          driverExecution
            .blocking {
              def observed[A](name: String)(read: => A): Either[AnalyticsError, A] =
                Either.catchNonFatal(read).leftMap { error =>
                  val cause = Option(error.getCause).getOrElse(error)
                  AnalyticsError.InvalidConfiguration(
                    s"Kafka $name inspection failed (${cause.getClass.getSimpleName})"
                  )
                }
              for {
                clusterId <- observed("cluster ID") {
                  client.describeCluster().clusterId().get(10L, TimeUnit.SECONDS)
                }
                description <- observed("topic ID") {
                  client.describeTopics(java.util.List.of(topic)).allTopicNames().get(10L, TimeUnit.SECONDS).get(topic)
                }
                _ <- Either.cond(
                  description != null && description.topicId() != null,
                  (),
                  AnalyticsError.InvalidConfiguration("Kafka topic identity is unavailable")
                )
              } yield clusterId -> description.topicId().toString
            }
            .flatMap(Async[F].fromEither)
        }
      containerAfter <- dockerBrokerContainer(connection, volumeName)
      _ <- Async[F].raiseUnless(containerBefore == containerAfter)(
        AnalyticsError.InvalidConfiguration("isolated Kafka container changed during broker identity inspection")
      )
      observed = HmacKeyRetirementKafkaLineage(
        broker._1,
        broker._2,
        volumeName,
        volume._1,
        volume._2,
        connection.bootstrapServers
      )
      _ <- Async[F].raiseUnless(matches(observed, observed))(
        AnalyticsError.InvalidConfiguration("Kafka lineage is malformed")
      )
    } yield observed
}

private[analytics] final class MongoHmacKeyRetirementPreparationStore[F[_]: Async](database: MongoDatabase[F]) {
  private val collection = database
    .withReadConcern(ReadConcern.MAJORITY)
    .getCollection[HmacKeyRetirementPreparation.MongoRecord](
      "analytics_hmac_key_retirement_preparations",
      HmacKeyRetirementPreparation.mongoRecordRegistry
    )
    .map(_.withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS)))

  private def id(lakehouseId: String, keyId: String): String = lakehouseId + ":" + keyId

  def insert(root: String, value: HmacKeyRetirementPreparation): F[Unit] =
    for {
      expected <- Async[F].fromEither(MongoAnalyticsLakehouseLock.lockId(root))
      _ <- Async[F].raiseUnless(
        value.lakehouseId == expected && value.keyId.matches("[A-Za-z0-9-]{1,40}") &&
          value.originalVerifier.matches("[A-Za-z0-9_-]{43}")
      )(
        AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is malformed or targets another lakehouse")
      )
      barrier <- Async[F].fromEither(KafkaRetentionBarrier.validate(value.barrier))
      _ <- Async[F].raiseUnless(HmacKeyRetirementKafkaLineage.matches(value.lineage, value.lineage))(
        AnalyticsError.InvalidConfiguration("HMAC key retirement Kafka lineage is malformed")
      )
      _ <- collection
        .flatMap(
          _.insertOne(
            HmacKeyRetirementPreparation.MongoRecord(
              id(value.lakehouseId, value.keyId),
              value.lakehouseId,
              value.keyId,
              value.originalVerifier,
              value.capturedAt,
              AnalyticsTopic.unwrap(barrier.topic),
              barrier.partitions.map(partition =>
                HmacKeyRetirementPreparation.PartitionRecord(
                  AnalyticsPartition.unwrap(partition.number),
                  AnalyticsOffset.unwrap(partition.endOffsetExclusive)
                )
              ),
              value.lineage.clusterId,
              value.lineage.topicId,
              value.lineage.volumeName,
              value.lineage.volumeMountpoint,
              value.lineage.volumeCreatedAt,
              value.lineage.bootstrapEndpoint
            )
          )
        )
        .void
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(_)           =>
            AnalyticsError.InvalidConfiguration("HMAC key retirement preparation could not be persisted")
        }
    } yield ()

  def read(root: String, keyId: String): F[Option[HmacKeyRetirementPreparation]] =
    Async[F].fromEither(MongoAnalyticsLakehouseLock.lockId(root)).flatMap { lakehouseId =>
      collection
        .flatMap(
          _.find(new Document("_id", id(lakehouseId, keyId)))
            .projection(
              Projections.include(
                "_id",
                "lakehouseId",
                "keyId",
                "originalVerifier",
                "capturedAt",
                "topic",
                "partitions",
                "clusterId",
                "topicId",
                "kafkaVolumeName",
                "kafkaVolumeMountpoint",
                "kafkaVolumeCreatedAt",
                "kafkaBootstrapEndpoint"
              )
            )
            .first
        )
        .flatMap {
          case Some(record) =>
            val malformed = AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is malformed")
            Async[F]
              .fromEither(
                HmacKeyRetirementPreparation
                  .decode(record)
                  .flatMap(value =>
                    Either.cond(
                      value.lakehouseId == lakehouseId && value.keyId == keyId,
                      value,
                      malformed
                    )
                  )
              )
              .map(Some(_))
          case None => Async[F].pure(None)
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(_)           =>
            AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is unavailable or malformed")
        }
    }
}

/** Reads actual broker offsets for every partition of the persisted retirement barrier. */
private[analytics] object HmacKeyRetirementKafkaOffsets {
  private def consumer[F[_]: Async](
      connection: KafkaConnection,
      driverExecution: SparkBlockingExecution[F]
  ): Resource[F, KafkaConsumer[Array[Byte], Array[Byte]]] =
    for {
      clientProperties <- Resource.eval(Async[F].fromEither(KafkaClientProperties.clientProperties(connection)))
      client <- Resource.make(driverExecution.blocking {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", connection.bootstrapServers)
        properties.setProperty("group.id", "hiring-analytics-key-retirement")
        properties.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
        properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
        properties.setProperty("enable.auto.commit", "false")
        properties.setProperty("default.api.timeout.ms", "10000")
        clientProperties.foreach { case (key, value) => properties.setProperty(key, value) }
        new KafkaConsumer[Array[Byte], Array[Byte]](properties)
      })(client => driverExecution.blocking(client.close()).void)
    } yield client

  def earliest[F[_]: Async](
      connection: KafkaConnection,
      barrier: KafkaRetentionBarrier,
      driverExecution: SparkBlockingExecution[F]
  ): F[Map[Int, Long]] =
    Async[F].fromEither(KafkaRetentionBarrier.validate(barrier)).flatMap { valid =>
      consumer[F](connection, driverExecution).use { client =>
        driverExecution
          .blocking {
            val topicName = AnalyticsTopic.unwrap(valid.topic)
            val partitions = valid.partitions
              .map(partition => new TopicPartition(topicName, AnalyticsPartition.unwrap(partition.number)))
            val actual = client.beginningOffsets(partitions.asJava)
            val offsets = partitions
              .flatMap(partition =>
                Option(actual.get(partition)).map(value => partition.partition() -> value.longValue())
              )
              .toMap
            KafkaRetentionBarrier.hasExpired(valid, offsets).map(_ => offsets)
          }
          .flatMap(Async[F].fromEither)
      }
    }
}
