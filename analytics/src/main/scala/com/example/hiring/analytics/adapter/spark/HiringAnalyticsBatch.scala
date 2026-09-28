package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.data.ValidatedNec
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import cats.effect.{Clock, ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, struct, to_json, when}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.time.Instant
import java.net.InetAddress
import java.sql.Timestamp
import java.util.UUID
import java.util.Properties
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.control.NonFatal

/** A bounded source is intentionally separate from storage and publication. */
trait BoundedOperationalEventSource {
  def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[DataFrame]

  /** Test and backfill sources represent every input coordinate directly, so their offsets must be dense. */
  def verifyOffsets(frame: DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
    AnalyticsOffsetRanges.verify(frame, manifest)
}

/** Supplies only current HMAC tokens, allowing the batch to remain independent of MongoDB. */
trait ActiveDeletionMarkerSource {
  def activeSubjectTokens(spark: SparkSession): IO[DataFrame]
}

final case class DataFrameDeletionMarkerSource(tokens: DataFrame) extends ActiveDeletionMarkerSource {
  override def activeSubjectTokens(spark: SparkSession): IO[DataFrame] = IO.pure(tokens)
}

final case class KafkaConnection(
    bootstrapServers: String,
    saslUsername: Option[String] = None,
    saslPassword: Option[String] = None,
    securityProtocol: String = "SASL_SSL",
    allowPlaintext: Boolean = false
)

object KafkaConnection {
  def validate(connection: KafkaConnection): ValidatedNec[String, KafkaConnection] =
    (
      if (connection.bootstrapServers != null && connection.bootstrapServers.trim.nonEmpty) connection.validNec
      else "Kafka bootstrap servers must be non-empty".invalidNec,
      (connection.saslUsername, connection.saslPassword) match {
        case (None, None)                                                            => connection.validNec
        case (Some(user), Some(password)) if user.trim.nonEmpty && password.nonEmpty => connection.validNec
        case _ => "Kafka SASL username and password must both be non-empty".invalidNec
      },
      connection.securityProtocol match {
        case "SASL_SSL" => connection.validNec
        case "SASL_PLAINTEXT" if connection.allowPlaintext && localPlaintextBootstrap(connection.bootstrapServers) =>
          connection.validNec
        case "SASL_PLAINTEXT" if !connection.allowPlaintext =>
          "Kafka SASL_PLAINTEXT requires analytics.kafka.allow-plaintext=true".invalidNec
        case "SASL_PLAINTEXT" =>
          "Kafka SASL_PLAINTEXT is restricted to validated loopback brokers and the local Compose kafka:9092 endpoint".invalidNec
        case _ => "Kafka security protocol must be SASL_SSL or SASL_PLAINTEXT".invalidNec
      }
    ).mapN((_, _, _) => connection)

  private def localPlaintextBootstrap(bootstrapServers: String): Boolean =
    Option(bootstrapServers)
      .filter(_.trim.nonEmpty)
      .exists(_.split(",", -1).forall { endpoint =>
        val broker = endpoint.trim
        val hostPort =
          if (broker.startsWith("[")) {
            val close = broker.indexOf(']')
            if (close > 0 && broker.drop(close + 1).startsWith(":"))
              Some(broker.substring(1, close) -> broker.drop(close + 2))
            else None
          } else {
            val separator = broker.lastIndexOf(':')
            if (separator > 0) Some(broker.substring(0, separator) -> broker.drop(separator + 1)) else None
          }
        hostPort.exists { case (host, rawPort) =>
          val validPort = rawPort.toIntOption.exists(port => port > 0 && port <= 65535)
          val localHost = host.equalsIgnoreCase("localhost") ||
            (host.equalsIgnoreCase("kafka") && rawPort == "9092") ||
            ((host.forall(char => char.isDigit || char == '.' || char == ':') && host.nonEmpty) &&
              Try(InetAddress.getByName(host).isLoopbackAddress).getOrElse(false))
          validPort && localHost
        }
      })

  private def escaped(value: String): String = value.replace("\\", "\\\\").replace("\"", "\\\"")

  def clientProperties(connection: KafkaConnection): Map[String, String] =
    (connection.saslUsername, connection.saslPassword) match {
      case (Some(username), Some(password))
          if connection.securityProtocol == "SASL_SSL" ||
            (connection.securityProtocol == "SASL_PLAINTEXT" && connection.allowPlaintext &&
              localPlaintextBootstrap(connection.bootstrapServers)) =>
        Map(
          "security.protocol" -> connection.securityProtocol,
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" ->
            s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${escaped(username)}\" password=\"${escaped(password)}\";"
        )
      case _ => Map.empty
    }

  def sparkOptions(connection: KafkaConnection): Map[String, String] =
    clientProperties(connection).map { case (key, value) => s"kafka.$key" -> value } ++
      Map("kafka.group.id" -> "hiring-analytics-batch", "kafka.isolation.level" -> "read_committed")
}

/** Reads exactly the offsets named by a manifest; it never starts a streaming query. */
final class KafkaOffsetRangeSource(connection: KafkaConnection) extends BoundedOperationalEventSource {
  override def verifyOffsets(frame: DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
    AnalyticsOffsetRanges.verifyCommittedKafkaRange(frame, manifest)

  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[DataFrame] =
    for {
      _ <- IO.fromEither(KafkaConnection.validate(connection).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      _ <- IO.fromEither(AnalyticsRunManifest.validate(manifest).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      _ <- AnalyticsOffsetRanges.requireNonEmpty(manifest)
      _ <- KafkaOffsetRangeSource.verifyAvailable(connection, manifest)
      frame <- IO
        .blocking {
          spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", connection.bootstrapServers)
            .options(KafkaConnection.sparkOptions(connection))
            .option("assign", KafkaOffsetRangeSource.assignJson(manifest.offsetRanges))
            .option("startingOffsets", KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.startOffset))
            .option("endingOffsets", KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.endOffsetExclusive))
            .option("failOnDataLoss", "true")
            .load()
        }
        .adaptError { case NonFatal(cause) => AnalyticsError.SourceReadFailure(cause) }
    } yield frame
}

object KafkaOffsetRangeSource {
  private def consumer(connection: KafkaConnection): Resource[IO, KafkaConsumer[Array[Byte], Array[Byte]]] =
    Resource.fromAutoCloseable(IO.blocking {
      val settings = new Properties()
      settings.setProperty("bootstrap.servers", connection.bootstrapServers)
      settings.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
      settings.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
      settings.setProperty("enable.auto.commit", "false")
      settings.setProperty("isolation.level", "read_committed")
      settings.setProperty("default.api.timeout.ms", "10000")
      KafkaConnection.clientProperties(connection).foreach { case (key, value) => settings.setProperty(key, value) }
      new KafkaConsumer[Array[Byte], Array[Byte]](settings)
    })

  private[analytics] def verifyAvailable(connection: KafkaConnection, manifest: AnalyticsRunManifest): IO[Unit] =
    consumer(connection).use { client =>
      IO.blocking {
        val topic = manifest.offsetRanges.head.topic
        val partitions = Option(client.partitionsFor(topic)).toVector.flatMap(_.asScala).map(_.partition()).toSet
        val missing = manifest.offsetRanges.find(range => !partitions.contains(range.partition))
        missing match {
          case Some(range) =>
            Left(
              AnalyticsError.MissingOffsetRange(
                range.topic,
                range.partition,
                range.endOffsetExclusive - range.startOffset,
                0L
              )
            )
          case None =>
            val requested = manifest.offsetRanges.map(range => new TopicPartition(range.topic, range.partition))
            val earliest = client.beginningOffsets(requested.asJava)
            val latest = client.endOffsets(requested.asJava)
            manifest.offsetRanges.foldLeft[Either[AnalyticsError, Unit]](Right(())) { (result, range) =>
              val partition = new TopicPartition(range.topic, range.partition)
              result.flatMap { _ =>
                (Option(earliest.get(partition)), Option(latest.get(partition))) match {
                  case (Some(first), Some(last)) =>
                    AnalyticsOffsetRanges.available(range, first.longValue(), last.longValue())
                  case _ =>
                    Left(
                      AnalyticsError.MissingOffsetRange(
                        range.topic,
                        range.partition,
                        range.endOffsetExclusive - range.startOffset,
                        0L
                      )
                    )
                }
              }
            }
        }
      }.adaptError { case NonFatal(cause) => AnalyticsError.SourceReadFailure(cause) }
        .flatMap(IO.fromEither)
    }

  private[analytics] def offsetJson(
      ranges: Vector[PartitionOffsetRange],
      select: PartitionOffsetRange => Long
  ): String = {
    val byTopic = ranges
      .groupBy(_.topic)
      .toSeq
      .sortBy(_._1)
      .map { case (topic, topicRanges) =>
        val partitions =
          topicRanges.sortBy(_.partition).map(range => s"\"${range.partition}\":${select(range)}").mkString(",")
        s"${jsonString(topic)}:{$partitions}"
      }
      .mkString(",")
    s"{$byTopic}"
  }

  private[analytics] def assignJson(ranges: Vector[PartitionOffsetRange]): String =
    ranges
      .groupBy(_.topic)
      .toSeq
      .sortBy(_._1)
      .map { case (topic, topicRanges) =>
        val partitions = topicRanges.map(_.partition).distinct.sorted.mkString(",")
        s"${jsonString(topic)}:[$partitions]"
      }
      .mkString("{", ",", "}")

  private def jsonString(value: String): String =
    value.iterator
      .map {
        case '"'                => "\\\""
        case '\\'               => "\\\\"
        case '\b'               => "\\b"
        case '\f'               => "\\f"
        case '\n'               => "\\n"
        case '\r'               => "\\r"
        case '\t'               => "\\t"
        case char if char < ' ' => f"\\u${char.toInt}%04x"
        case char               => char.toString
      }
      .mkString("\"", "", "\"")
}

/** Test and backfill adapter. Its frame must have Kafka's topic, partition, offset, timestamp and value columns. */
final case class DataFrameBatchSource(records: DataFrame) extends BoundedOperationalEventSource {
  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[DataFrame] =
    IO.fromEither(AnalyticsRunManifest.validate(manifest).toEither.leftMap(AnalyticsError.InvalidInput.apply)) *>
      AnalyticsOffsetRanges.requireNonEmpty(manifest) *>
      IO.blocking(records.schema)
        .adaptError { case NonFatal(cause) => AnalyticsError.SourceReadFailure(cause) }
        .flatMap(KafkaRecordColumns.validate) *>
      IO.blocking {
        val inManifest = manifest.offsetRanges.foldLeft(lit(false): Column) { (condition, range) =>
          condition || (
            col("topic") === lit(range.topic) &&
              col("partition") === lit(range.partition) &&
              col("offset") >= lit(range.startOffset) &&
              col("offset") < lit(range.endOffsetExclusive)
          )
        }
        records.filter(inManifest)
      }.adaptError { case NonFatal(cause) => AnalyticsError.SourceReadFailure(cause) }
}

final case class AnalyticsLakehousePaths(root: String) {
  private val normalizedRoot = Option(root).getOrElse("").stripSuffix("/")
  val bronze: String = s"$normalizedRoot/bronze/operational_events"
  val silver: String = s"$normalizedRoot/silver/operational_events"
  val quarantine: String = s"$normalizedRoot/quarantine/operational_events"
  val funnelGold: String = s"$normalizedRoot/gold/application_funnel"
  val timeToHireGold: String = s"$normalizedRoot/gold/time_to_hire"
  val skillsGold: String = s"$normalizedRoot/gold/job_skills"
  val manifests: String = s"$normalizedRoot/control/run_manifests"
  val hmacKeyRegistry: String = s"$normalizedRoot/control/hmac_key_registry"
}

object AnalyticsLakehousePaths {
  def validate(paths: AnalyticsLakehousePaths): ValidatedNec[String, AnalyticsLakehousePaths] =
    if (paths.root != null && paths.root.trim.nonEmpty) paths.validNec
    else "lakehouse root must be non-empty".invalidNec
}

sealed trait AnalyticsRunOutcome

object AnalyticsRunOutcome {
  case object QualityBlocked extends AnalyticsRunOutcome
  case object ErasurePending extends AnalyticsRunOutcome
  case object Published extends AnalyticsRunOutcome
}

final case class AnalyticsPublication(
    runId: RunId,
    outcome: AnalyticsRunOutcome,
    completedAt: Instant,
    funnelGoldPath: String,
    timeToHireGoldPath: String,
    skillPostingGoldPath: String,
    bronzeRecords: Long,
    validRecords: Long,
    suppressedRecords: Long,
    quarantinedRecords: Long,
    conflictingEventIds: Long
)

/** Delta batch writer with idempotent natural keys. A completed manifest is written only after Bronze, Silver,
  * quarantine, and both rebuildable Gold datasets have been durably updated.
  */
final class HiringAnalyticsBatch(
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    deletionMarkers: ActiveDeletionMarkerSource,
    clock: Clock[IO] = Clock[IO],
    reportPublisher: Option[AnalyticsReportPublisher] = None,
    manifestWriter: Option[(SparkSession, AnalyticsRunManifest, String, String) => IO[Unit]] = None,
    lakehouseLock: AnalyticsLakehouseLock = AnalyticsLakehouseLock.processLocal,
    retirementStore: HmacKeyRetirementAuthorizationStore = HmacKeyRetirementAuthorizationStore.unavailable
) extends LakehouseOperation {
  private val logger = Slf4jLogger.getLogger[IO]
  private val MaximumErasureEvidenceFiles = 100000
  private def now: IO[Instant] = clock.realTime.map(duration => Instant.ofEpochMilli(duration.toMillis))

  private def persistManifest(
      spark: SparkSession,
      manifest: AnalyticsRunManifest,
      status: String,
      updatedAt: String
  ): IO[Unit] = manifestWriter.fold(writeManifest(spark, manifest, status, updatedAt))(writer =>
    writer(spark, manifest, status, updatedAt)
  )

  private def cachedMarkers(spark: SparkSession): Resource[IO, DataFrame] =
    Resource.make(
      deletionMarkers
        .activeSubjectTokens(spark)
        .flatMap(frame => lakehouse(frame.persist(StorageLevel.MEMORY_AND_DISK)))
    )(frame => lakehouse(frame.unpersist(blocking = true)).void)

  def run(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest
  ): IO[AnalyticsPublication] =
    for {
      _ <- IO.fromEither(AnalyticsLakehousePaths.validate(paths).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      _ <- IO.fromEither(AnalyticsRunManifest.validate(manifest).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      _ <- AnalyticsOffsetRanges.requireNonEmpty(manifest)
      publication <- lakehouseLock.resource(paths.root).use { _ =>
        cachedMarkers(spark).use { markerTokens =>
          for {
            _ <- validateMarkerColumns(markerTokens)
            activeMarkerCount <- lakehouse(markerTokens.count())
            _ <- validateHmacConfigurationLocked(spark)
            reservedAt <- now
            reservation <- reportPublisher.traverse(
              _.reserve(manifest.runId.value, rangeFingerprint(manifest), reservedAt)
            )
            result <-
              for {
                _ <- configureRawTablePrivacy(spark)
                _ <- if (activeMarkerCount > 0L) applyActiveDeletions(spark, markerTokens) else IO.unit
                result <- runWithMarkers(
                  spark,
                  source,
                  manifest,
                  markerTokens,
                  activeMarkerCount > 0L,
                  reservation
                )
              } yield result
          } yield result
        }
      }
    } yield publication

  private def validateMarkerColumns(frame: DataFrame): IO[Unit] =
    lakehouse(frame.columns.toVector).flatMap { columns =>
      if (columns.contains("subjectToken")) IO.unit
      else IO.raiseError(AnalyticsError.InvalidSourceSchema(Vector("subjectToken")))
    }

  /** A new primary HMAC key cannot split contributor identity while unexpired Silver rows use an older key. */
  /** Raw Bronze and quarantine keep replay data, but their Delta logs must not index raw values. */
  private def configureRawTablePrivacy(spark: SparkSession): IO[Unit] = lakehouse {
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "0")
    spark.conf.set(
      "spark.databricks.delta.properties.defaults.logRetentionDuration",
      s"interval ${AnalyticsRetention.DeltaLogRetentionDays} days"
    )
    Vector(paths.bronze, paths.quarantine).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) {
        val escaped = path.replace("`", "``")
        val properties = DeltaTable
          .forPath(spark, path)
          .detail()
          .select("properties")
          .head()
          .getAs[scala.collection.Map[String, String]]("properties")
        val desiredLogRetention = s"interval ${AnalyticsRetention.DeltaLogRetentionDays} days"
        if (
          properties.get("delta.dataSkippingNumIndexedCols").forall(_ != "0") ||
          properties.get("delta.logRetentionDuration").forall(_ != desiredLogRetention)
        )
          spark.sql(
            s"ALTER TABLE delta.`$escaped` SET TBLPROPERTIES " +
              s"('delta.dataSkippingNumIndexedCols' = '0', 'delta.logRetentionDuration' = '$desiredLogRetention')"
          )
      }
    }
  }

  private val stageRuntime = AnalyticsBatchStageRuntime(
    paths = paths,
    pseudonymizer = pseudonymizer,
    blocking = new AnalyticsBatchBlocking {
      override def apply[A](work: => A): IO[A] = lakehouse(work)
      override def either[A](work: => Either[AnalyticsError, A]): IO[A] = lakehouseEither(work)
    },
    currentTime = () => now,
    persistManifest = persistManifest,
    merge = merge,
    readOrEmpty = readOrEmpty,
    withExpiry = withExpiry,
    quarantineId = () => quarantineId,
    configureRawTablePrivacy = configureRawTablePrivacy,
    maximumErasureEvidenceFiles = MaximumErasureEvidenceFiles,
    retirementAuthorizations = retirementStore.list
  )

  private val keyContinuityStage = new AnalyticsKeyContinuityStage(stageRuntime)

  private[analytics] def validateStoredTokenKeys(spark: SparkSession): IO[Unit] =
    keyContinuityStage.validateStoredTokenKeys(spark)

  private def validateHmacConfigurationLocked(spark: SparkSession): IO[Unit] =
    keyContinuityStage.validateHmacConfiguration(spark)

  private[analytics] def validateKeyMaterialContinuity(spark: SparkSession): IO[Unit] =
    lakehouseLock.resource(paths.root).use(_ => keyContinuityStage.validateKeyMaterialContinuity(spark))

  private[analytics] def validateHmacConfiguration(spark: SparkSession): IO[Unit] =
    lakehouseLock.resource(paths.root).use(_ => validateHmacConfigurationLocked(spark))

  private def applyActiveDeletions(spark: SparkSession, markerTokens: DataFrame): IO[Unit] =
    for {
      deletionTime <- now
      _ <- expire(spark, paths.bronze, deletionTime)
      _ <- expire(spark, paths.quarantine, deletionTime)
      _ <- expire(spark, paths.silver, deletionTime)
      _ <- erasureStage.purgeMarkedSubjectRows(spark, paths.bronze, markerTokens)
      _ <- erasureStage.purgeMarkedSubjectRows(spark, paths.quarantine, markerTokens)
      _ <- erasureStage.purgeMarkedSubjectRows(spark, paths.silver, markerTokens)
      _ <- rebuildGoldFromStoredSilver(spark)
      _ <- vacuumExpiredFiles(spark).void
    } yield ()

  private[analytics] def applyDeletionMarkers(spark: SparkSession, markerTokens: DataFrame): IO[Unit] =
    configureRawTablePrivacy(spark) *> applyActiveDeletions(spark, markerTokens)

  private[analytics] def reclaimRetainedFiles(spark: SparkSession): IO[Long] =
    configureRawTablePrivacy(spark) *> vacuumExpiredFiles(spark).flatTap(_ => checkpointRawTableLogs(spark))

  private[analytics] def rebuildGoldAndExtractReport(spark: SparkSession, asOf: Instant): IO[AnalyticsReportOutput] =
    rebuildGoldFromStoredSilver(spark) *> extractReport(spark, asOf)

  private[analytics] def extractCurrentReport(spark: SparkSession, asOf: Instant): IO[AnalyticsReportOutput] =
    extractReport(spark, asOf)

  private val erasureStage = new AnalyticsBatchErasureStage(stageRuntime)

  private[analytics] def verifyMarkedSubjectsAbsent(spark: SparkSession, markerTokens: DataFrame): IO[Unit] =
    erasureStage.verifyMarkedSubjectsAbsent(spark, markerTokens)

  private[analytics] def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): IO[Long] =
    erasureStage.countMarkedRows(spark, markerTokens)

  private[analytics] def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): IO[Vector[String]] =
    erasureStage.captureMarkedFiles(spark, markerTokens)

  private[analytics] def checkpointPurgedRawLogs(spark: SparkSession): IO[Vector[String]] =
    erasureStage.checkpointPurgedRawLogs(spark)

  private[analytics] def verifyFilesAbsent(spark: SparkSession, files: Vector[String]): IO[Unit] =
    erasureStage.verifyFilesAbsent(spark, files)

  private[analytics] def checkpointRawTableLogs(spark: SparkSession): IO[Unit] =
    erasureStage.checkpointRawTableLogs(spark)

  /** Deletion is applied to rebuildable Gold immediately, even if the new Kafka range later quality-blocks. */
  private def rebuildGoldFromStoredSilver(spark: SparkSession): IO[Unit] =
    lakehouse(DeltaTable.isDeltaTable(spark, paths.silver)).flatMap {
      case true =>
        lakehouse(spark.read.format("delta").load(paths.silver)).flatMap(AnalyticsGoldStage.rebuild(paths, _))
      case false => AnalyticsGoldStage.clear(spark, paths)
    }

  private val ingestionStage = new AnalyticsBatchIngestionStage(stageRuntime)
  private val silverStage = new AnalyticsBatchSilverStage(stageRuntime)

  private def runWithMarkers(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean,
      reservation: Option[AnalyticsReportReservation]
  ): IO[AnalyticsPublication] =
    for {
      bronze <- ingestionStage.ingest(spark, source, manifest, markerTokens)
      prepared <- silverStage.separateQuarantine(spark, bronze, markerTokens, activeMarkersPresent)
      silver <- silverStage.mergeSilver(prepared, bronze.startedAt)
      silverSchema <- lakehouse(silver.schema)
      completedAt <- now
      _ <- expireStored(spark, bronze.startedAt)
      outcome <- finishRun(
        spark,
        manifest,
        silverSchema,
        prepared.quarantinedRecords,
        completedAt,
        activeMarkersPresent,
        reservation
      )
    } yield AnalyticsPublication(
      manifest.runId,
      outcome,
      completedAt,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold,
      bronze.records,
      prepared.validRecords,
      prepared.suppressedRecords,
      prepared.quarantinedRecords,
      prepared.conflictingEventIds
    )

  private def expireStored(spark: SparkSession, startedAt: Instant): IO[Unit] =
    for {
      _ <- expire(spark, paths.bronze, startedAt)
      _ <- expire(spark, paths.quarantine, startedAt)
      _ <- expire(spark, paths.silver, startedAt)
    } yield ()

  private def vacuumExpiredFiles(spark: SparkSession): IO[Long] =
    Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold
    ).foldLeft(IO.pure(0L)) { (removedFiles, path) =>
      removedFiles.flatMap { count =>
        lakehouse(DeltaTable.isDeltaTable(spark, path)).flatMap {
          case false => IO.pure(count)
          case true  =>
            val temporaryPath = s"${paths.root.stripSuffix("/")}/control/purge-rewrite-${UUID.randomUUID()}"
            DeltaPurgeRewrite.temporaryPath(spark, temporaryPath).use { _ =>
              lakehouse {
                spark.read.format("delta").load(path).write.format("delta").mode("overwrite").save(temporaryPath)
                spark.read
                  .format("delta")
                  .load(temporaryPath)
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true")
                  .save(path)
                // Respect Delta's retention safety horizon. Erasure completes only after this reclaim horizon passes.
                count + DeltaTable.forPath(spark, path).vacuum().count()
              }
            }
        }
      }
    }.handleErrorWith { error =>
      logger.error(s"lakehouse expired-file vacuum failed (${error.getClass.getSimpleName})") *>
        IO.raiseError(error)
    }

  private def finishRun(
      spark: SparkSession,
      manifest: AnalyticsRunManifest,
      silverSchema: StructType,
      quarantinedRecords: Long,
      completedAt: Instant,
      activeMarkersPresent: Boolean,
      reservation: Option[AnalyticsReportReservation]
  ): IO[AnalyticsRunOutcome] =
    if (quarantinedRecords > 0L)
      persistManifest(spark, manifest, "QUALITY_BLOCKED", completedAt.toString).as(AnalyticsRunOutcome.QualityBlocked)
    else if (activeMarkersPresent && reportPublisher.nonEmpty)
      persistManifest(spark, manifest, "ERASURE_PENDING", completedAt.toString).as(AnalyticsRunOutcome.ErasurePending)
    else
      for {
        allSilver <- readOrEmpty(spark, paths.silver, silverSchema)
        _ <- AnalyticsGoldStage.rebuild(paths, allSilver)
        report <- extractReport(spark, completedAt)
        _ <- (reportPublisher, reservation) match {
          case (Some(publisher), Some(value)) =>
            publisher.publish(
              value,
              report,
              completedAt.plusSeconds(AnalyticsRetention.PublishedSnapshotDays.toLong * 86400L)
            )
          case _ => IO.unit
        }
        _ <- persistManifest(spark, manifest, "PUBLISHED", completedAt.toString)
      } yield AnalyticsRunOutcome.Published

  private def extractReport(spark: SparkSession, asOf: Instant): IO[AnalyticsReportOutput] =
    AnalyticsGoldStage.extract(spark, paths, asOf)

  private def rangeFingerprint(manifest: AnalyticsRunManifest): String = {
    val canonical = manifest.offsetRanges
      .sortBy(range => (range.topic, range.partition))
      .map(range => s"${range.topic}:${range.partition}:${range.startOffset}:${range.endOffsetExclusive}")
      .mkString("\n")
    AnalyticsDigest.sha256Hex(canonical.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  }

  private[analytics] def writeManifest(
      spark: SparkSession,
      manifest: AnalyticsRunManifest,
      status: String,
      updatedAt: String
  ): IO[Unit] = lakehouse {
    import scala.jdk.CollectionConverters.*
    val rows = manifest.offsetRanges.map(range =>
      Row(
        manifest.runId.value,
        range.topic,
        range.partition,
        range.startOffset,
        range.endOffsetExclusive,
        status,
        updatedAt
      )
    )
    val schema = StructType(
      Seq(
        StructField("runId", StringType, nullable = false),
        StructField("topic", StringType, nullable = false),
        StructField("partition", IntegerType, nullable = false),
        StructField("startOffset", LongType, nullable = false),
        StructField("endOffsetExclusive", LongType, nullable = false),
        StructField("status", StringType, nullable = false),
        StructField("updatedAt", StringType, nullable = false)
      )
    )
    val frame = spark.createDataFrame(rows.asJava, schema)
    val condition =
      "target.runId = source.runId AND target.topic = source.topic AND target.partition = source.partition"
    if (DeltaTable.isDeltaTable(spark, paths.manifests))
      DeltaTable
        .forPath(spark, paths.manifests)
        .as("target")
        .merge(frame.as("source"), condition)
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    else frame.write.format("delta").mode("errorifexists").save(paths.manifests)
  }

  private def merge(source: DataFrame, path: String, condition: String): IO[Unit] = lakehouse {
    if (DeltaTable.isDeltaTable(source.sparkSession, path))
      DeltaTable
        .forPath(source.sparkSession, path)
        .as("target")
        .merge(source.as("source"), condition)
        .withSchemaEvolution()
        .whenNotMatched()
        .insertAll()
        .execute()
    else source.write.format("delta").mode("errorifexists").save(path)
  }

  private def readOrEmpty(spark: SparkSession, path: String, schema: StructType): IO[DataFrame] =
    lakehouse {
      if (DeltaTable.isDeltaTable(spark, path)) spark.read.format("delta").load(path)
      else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

  private def withExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame =
    frame
      .withColumn("ingestedAt", lit(Timestamp.from(now)))
      .withColumn("expiresAt", lit(Timestamp.from(now.plusSeconds(days.toLong * 24L * 60L * 60L))))

  private def quarantineId: Column =
    when(
      col("rawValue").isNull,
      concat(
        lit("tombstone:"),
        to_json(struct(col("topic").as("topic"), col("partition").as("partition"), col("offset").as("offset")))
      )
    ).otherwise(sha2(col("rawValue"), 256))

  private def expire(spark: SparkSession, path: String, now: Instant): IO[Unit] = lakehouse {
    if (DeltaTable.isDeltaTable(spark, path))
      DeltaTable.forPath(spark, path).delete(col("expiresAt") <= lit(Timestamp.from(now)))
  }

}

private[analytics] object DeltaPurgeRewrite {
  def temporaryPath(spark: SparkSession, temporaryPath: String): Resource[IO, Unit] =
    Resource
      .make(
        IO.blocking {
          val path = new org.apache.hadoop.fs.Path(temporaryPath)
          (path.getFileSystem(spark.sparkContext.hadoopConfiguration), path)
        }
      ) { case (fileSystem, path) =>
        IO.blocking {
          val removed = fileSystem.delete(path, true)
          if (!removed && fileSystem.exists(path))
            throw new java.io.IOException("temporary purge rewrite path remains")
        }
      }
      .void
}

private[analytics] object KafkaRecordColumns {
  def validate(schema: StructType): IO[Unit] = {
    val required = Set("topic", "partition", "offset", "timestamp", "value")
    val missing = required.diff(schema.fieldNames.toSet).toVector.sorted
    if (missing.isEmpty) IO.unit else IO.raiseError(AnalyticsError.InvalidSourceSchema(missing))
  }
}

/** Bounded batch entry point. Runtime settings and the explicit offset range load from HOCON. */
