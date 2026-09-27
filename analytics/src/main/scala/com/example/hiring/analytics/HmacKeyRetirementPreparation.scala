package com.example.hiring.analytics

import com.example.hiring.analytics.batch.KafkaConnection
import com.example.hiring.analytics.erasure.KafkaRetentionBarrier
import com.example.hiring.analytics.mongo.MongoPublisherStream

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.mongodb.{ReadConcern, WriteConcern}
import com.mongodb.reactivestreams.client.MongoDatabase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.bson.Document

import java.time.Instant
import java.util.{Date, Properties}
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
    captured != null && current != null && captured == current &&
      Option(captured.clusterId).exists(_.nonEmpty) && Option(captured.topicId).exists(_.nonEmpty) &&
      Option(captured.volumeName).exists(_.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")) &&
      Option(captured.volumeMountpoint).exists(_.endsWith("/" + captured.volumeName + "/_data")) &&
      Option(captured.volumeCreatedAt).exists(_.nonEmpty) &&
      Option(captured.bootstrapEndpoint).exists(_.matches("127\\.0\\.0\\.1:[0-9]{1,5}"))

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

  private def docker(args: Vector[String]): String = {
    val process = new ProcessBuilder(args*).redirectErrorStream(true).start()
    if (!process.waitFor(30L, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      throw AnalyticsError.InvalidConfiguration("Kafka Docker inspection timed out")
    }
    val output = new String(process.getInputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim
    if (process.exitValue() != 0)
      throw AnalyticsError.InvalidConfiguration("Kafka Docker inspection failed")
    output
  }

  private def dockerBrokerContainer(connection: KafkaConnection, volumeName: String): IO[String] = IO.blocking {
    val suffix = "_hmac-rotation-kafka"
    if (!volumeName.endsWith(suffix) || !connection.bootstrapServers.matches("127\\.0\\.0\\.1:[0-9]{1,5}"))
      throw AnalyticsError.InvalidConfiguration("Kafka endpoint or isolated volume name is invalid")
    val project = volumeName.stripSuffix(suffix)
    val ids = docker(
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
    ).linesIterator.filter(_.nonEmpty).toVector
    if (ids.size != 1)
      throw AnalyticsError.InvalidConfiguration("exactly one isolated Compose Kafka container must be running")
    val container = ids.head
    val identity = docker(
      Vector(
        "docker",
        "inspect",
        "--format",
        "{{.Id}}|{{.State.Running}}|{{index .Config.Labels \"com.docker.compose.project\"}}|{{index .Config.Labels \"com.docker.compose.service\"}}",
        container
      )
    ).split("\\|", -1).toVector
    if (
      identity.size != 4 || identity(0) != container || identity(1) != "true" ||
      identity(2) != project || identity(3) != "kafka"
    )
      throw AnalyticsError.InvalidConfiguration("isolated Kafka container identity changed")
    val published = docker(Vector("docker", "port", container, "29092/tcp"))
    val mountLines = docker(
      Vector(
        "docker",
        "inspect",
        "--format",
        "{{range .Mounts}}{{.Type}}|{{.Name}}|{{.Destination}}|{{.RW}}{{println}}{{end}}",
        container
      )
    ).linesIterator.filter(_.nonEmpty).toVector
    val mounts = mountLines.map { line =>
      line.split("\\|", -1).toVector match {
        case Vector(kind, name, destination, "true")  => (kind, name, destination, true)
        case Vector(kind, name, destination, "false") => (kind, name, destination, false)
        case _ => throw AnalyticsError.InvalidConfiguration("isolated Kafka mount inventory is malformed")
      }
    }
    if (!bindingMatches(connection.bootstrapServers, published, mounts, volumeName))
      throw AnalyticsError.InvalidConfiguration("configured Kafka endpoint is not bound to the isolated Kafka volume")
    container
  }

  private def dockerVolume(volumeName: String): IO[(String, String)] = IO.blocking {
    if (!volumeName.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"))
      throw AnalyticsError.InvalidConfiguration("isolated Kafka volume name is invalid")
    val result = docker(
      Vector(
        "docker",
        "volume",
        "inspect",
        "--format",
        "{{.Driver}}|{{.Mountpoint}}|{{.CreatedAt}}",
        volumeName
      )
    )
    val parts = result.split("\\|", -1).toVector
    if (
      parts.size != 3 || parts.head != "local" ||
      !parts(1).endsWith("/" + volumeName + "/_data") || parts(2).isEmpty
    )
      throw AnalyticsError.InvalidConfiguration("isolated Kafka Docker volume identity is unavailable")
    parts(1) -> parts(2)
  }

  def observe(connection: KafkaConnection, topic: String, volumeName: String): IO[HmacKeyRetirementKafkaLineage] =
    for {
      containerBefore <- dockerBrokerContainer(connection, volumeName)
      volume <- dockerVolume(volumeName)
      broker <- Resource
        .fromAutoCloseable(IO.blocking {
          val properties = new Properties()
          properties.setProperty("bootstrap.servers", connection.bootstrapServers)
          properties.setProperty("request.timeout.ms", "10000")
          properties.setProperty("default.api.timeout.ms", "15000")
          KafkaConnection.clientProperties(connection).foreach { case (key, value) =>
            properties.setProperty(key, value)
          }
          try AdminClient.create(properties)
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
              throw AnalyticsError.InvalidConfiguration(
                s"Kafka lineage admin client initialization failed (${cause.getClass.getSimpleName}: $diagnostic)"
              )
          }
        })
        .use(client =>
          IO.blocking {
            def observed[A](name: String)(read: => A): A =
              try read
              catch {
                case NonFatal(error) =>
                  val cause = Option(error.getCause).getOrElse(error)
                  throw AnalyticsError.InvalidConfiguration(
                    s"Kafka $name inspection failed (${cause.getClass.getSimpleName})"
                  )
              }
            val clusterId = observed("cluster ID") {
              client.describeCluster().clusterId().get(10L, TimeUnit.SECONDS)
            }
            val description = observed("topic ID") {
              client.describeTopics(java.util.List.of(topic)).allTopicNames().get(10L, TimeUnit.SECONDS).get(topic)
            }
            if (description == null || description.topicId() == null)
              throw AnalyticsError.InvalidConfiguration("Kafka topic identity is unavailable")
            clusterId -> description.topicId().toString
          }
        )
      containerAfter <- dockerBrokerContainer(connection, volumeName)
      _ <- IO.raiseUnless(containerBefore == containerAfter)(
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
      _ <- IO.raiseUnless(matches(observed, observed))(
        AnalyticsError.InvalidConfiguration("Kafka lineage is malformed")
      )
    } yield observed
}

private[analytics] final class MongoHmacKeyRetirementPreparationStore(database: MongoDatabase) {
  private val collection = database
    .getCollection("analytics_hmac_key_retirement_preparations", classOf[Document])
    .withReadConcern(ReadConcern.MAJORITY)
    .withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS))

  private def id(lakehouseId: String, keyId: String): String = lakehouseId + ":" + keyId

  def insert(root: String, value: HmacKeyRetirementPreparation): IO[Unit] =
    for {
      expected <- IO.fromEither(HmacKeyRetirementAuthorization.lakehouseId(root))
      _ <- IO.raiseUnless(
        value.lakehouseId == expected && value.keyId.matches("[A-Za-z0-9-]{1,40}") &&
          value.originalVerifier.matches("[A-Za-z0-9_-]{43}") && value.capturedAt != null
      )(
        AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is malformed or targets another lakehouse")
      )
      barrier <- IO.fromEither(KafkaRetentionBarrier.validate(value.barrier))
      _ <- IO.raiseUnless(HmacKeyRetirementKafkaLineage.matches(value.lineage, value.lineage))(
        AnalyticsError.InvalidConfiguration("HMAC key retirement Kafka lineage is malformed")
      )
      _ <- MongoPublisherStream
        .drain {
          collection.insertOne(
            new Document("_id", id(value.lakehouseId, value.keyId))
              .append("lakehouseId", value.lakehouseId)
              .append("keyId", value.keyId)
              .append("originalVerifier", value.originalVerifier)
              .append("capturedAt", Date.from(value.capturedAt))
              .append("topic", barrier.topic)
              .append("clusterId", value.lineage.clusterId)
              .append("topicId", value.lineage.topicId)
              .append("kafkaVolumeName", value.lineage.volumeName)
              .append("kafkaVolumeMountpoint", value.lineage.volumeMountpoint)
              .append("kafkaVolumeCreatedAt", value.lineage.volumeCreatedAt)
              .append("kafkaBootstrapEndpoint", value.lineage.bootstrapEndpoint)
              .append(
                "partitions",
                barrier.partitions
                  .map(partition =>
                    new Document("number", partition.number).append("endOffsetExclusive", partition.endOffsetExclusive)
                  )
                  .asJava
              )
          )
        }
        .adaptError { case NonFatal(_) =>
          AnalyticsError.InvalidConfiguration("HMAC key retirement preparation could not be persisted")
        }
    } yield ()

  def read(root: String, keyId: String): IO[Option[HmacKeyRetirementPreparation]] =
    IO.fromEither(HmacKeyRetirementAuthorization.lakehouseId(root)).flatMap { lakehouseId =>
      MongoPublisherStream
        .optional(collection.find(new Document("_id", id(lakehouseId, keyId))).first())
        .flatMap {
          case Some(document) =>
            val partitions = Option(document.getList("partitions", classOf[Document])).toVector
              .flatMap(_.asScala)
              .map(partition =>
                KafkaRetentionBarrier.Partition(
                  partition.getInteger("number"),
                  partition.getLong("endOffsetExclusive")
                )
              )
            (for {
              barrier <- KafkaRetentionBarrier.validate(KafkaRetentionBarrier(document.getString("topic"), partitions))
              _ <- Either.cond(
                document.getString("lakehouseId") == lakehouseId &&
                  document.getString("keyId") == keyId &&
                  Option(document.getString("originalVerifier")).exists(_.matches("[A-Za-z0-9_-]{43}")) &&
                  document.getDate("capturedAt") != null,
                (),
                AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is malformed")
              )
              lineage = HmacKeyRetirementKafkaLineage(
                document.getString("clusterId"),
                document.getString("topicId"),
                document.getString("kafkaVolumeName"),
                document.getString("kafkaVolumeMountpoint"),
                document.getString("kafkaVolumeCreatedAt"),
                document.getString("kafkaBootstrapEndpoint")
              )
              _ <- Either.cond(
                HmacKeyRetirementKafkaLineage.matches(lineage, lineage),
                (),
                AnalyticsError.InvalidConfiguration("HMAC key retirement Kafka lineage is malformed")
              )
            } yield HmacKeyRetirementPreparation(
              lakehouseId,
              keyId,
              document.getString("originalVerifier"),
              document.getDate("capturedAt").toInstant,
              barrier,
              lineage
            )).liftTo[IO].map(Some(_))
          case None => IO.pure(None)
        }
        .adaptError { case NonFatal(_) =>
          AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is unavailable or malformed")
        }
    }
}

/** Reads actual broker offsets for every partition of the persisted retirement barrier. */
private[analytics] object HmacKeyRetirementKafkaOffsets {
  private def consumer(connection: KafkaConnection): Resource[IO, KafkaConsumer[Array[Byte], Array[Byte]]] =
    Resource.fromAutoCloseable(IO.blocking {
      val properties = new Properties()
      properties.setProperty("bootstrap.servers", connection.bootstrapServers)
      properties.setProperty("group.id", "hiring-analytics-key-retirement")
      properties.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
      properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
      properties.setProperty("enable.auto.commit", "false")
      properties.setProperty("default.api.timeout.ms", "10000")
      KafkaConnection.clientProperties(connection).foreach { case (key, value) => properties.setProperty(key, value) }
      new KafkaConsumer[Array[Byte], Array[Byte]](properties)
    })

  def earliest(connection: KafkaConnection, barrier: KafkaRetentionBarrier): IO[Map[Int, Long]] =
    IO.fromEither(KafkaRetentionBarrier.validate(barrier)).flatMap { valid =>
      consumer(connection).use { client =>
        IO.blocking {
          val partitions = valid.partitions.map(partition => new TopicPartition(valid.topic, partition.number))
          val actual = client.beginningOffsets(partitions.asJava)
          val offsets = partitions
            .flatMap(partition =>
              Option(actual.get(partition)).map(value => partition.partition() -> value.longValue())
            )
            .toMap
          KafkaRetentionBarrier.hasExpired(valid, offsets).fold(throw _, _ => offsets)
        }
      }
    }
}
