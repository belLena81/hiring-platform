package com.example.hiring.analytics.batch

import com.example.hiring.analytics.*
import com.example.hiring.analytics.erasure.*
import com.example.hiring.analytics.mongo.*

import cats.data.ValidatedNec
import cats.effect.{Clock, ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{
  array_contains,
  col,
  concat,
  explode,
  input_file_name,
  lit,
  sha2,
  size,
  struct,
  to_json,
  when
}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import com.mongodb.client.{MongoClient, MongoClients}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.time.Instant
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
    saslPassword: Option[String] = None
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
      }
    ).mapN((_, _) => connection)

  private def escaped(value: String): String = value.replace("\\", "\\\\").replace("\"", "\\\"")

  def clientProperties(connection: KafkaConnection): Map[String, String] =
    (connection.saslUsername, connection.saslPassword) match {
      case (Some(username), Some(password)) =>
        Map(
          "security.protocol" -> "SASL_PLAINTEXT",
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
        s"\"$topic\":{$partitions}"
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
        s"\"$topic\":[$partitions]"
      }
      .mkString("{", ",", "}")
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

/** A bounded, publisher-neutral report contract matching the operational analytics projection. */
final case class AnalyticsFunnelDayOutput(
    day: Instant,
    created: Long,
    accepted: Long,
    declined: Long,
    interview: Long,
    hired: Long,
    rejected: Long
)
final case class AnalyticsTimeToHireOutput(
    p50Hours: Double,
    p75Hours: Double,
    p90Hours: Double,
    p95Hours: Double,
    eligibleCount: Long,
    excludedCount: Long
)
final case class AnalyticsSkillPostingDayOutput(day: Instant, skill: String, postings: Long)
final case class AnalyticsReportOutput(
    asOf: Instant,
    funnel: Vector[AnalyticsFunnelDayOutput],
    timeToHire: Option[AnalyticsTimeToHireOutput],
    skillPostingActivity: Vector[AnalyticsSkillPostingDayOutput]
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
    manifestWriter: Option[(SparkSession, AnalyticsRunManifest, String, String) => IO[Unit]] = None
) {
  private val MaximumReportRows = 10000
  private val MaximumErasureEvidenceFiles = 100000
  private def now: IO[Instant] = clock.realTime.map(duration => Instant.ofEpochMilli(duration.toMillis))

  private def lakehouse[A](work: => A): IO[A] =
    IO.blocking(work).adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
    }

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
      publication <- AnalyticsLakehouseLock.resource(paths.root).use { _ =>
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
  private def ensurePrimaryTokenCompatibility(spark: SparkSession, at: Instant): IO[Unit] = lakehouse {
    if (DeltaTable.isDeltaTable(spark, paths.silver)) {
      val stored = spark.read.format("delta").load(paths.silver)
      val columns = stored.columns.toSet
      if (stored.limit(1).count() > 0L && !columns.contains("subjectToken"))
        throw AnalyticsError.InvalidConfiguration("Silver data has no versioned subject tokens")
      if (columns.contains("subjectToken")) {
        val activeData =
          if (columns.contains("expiresAt"))
            col("expiresAt").isNull || col("expiresAt") > lit(Timestamp.from(at))
          else lit(true)
        val expectedPrefix = pseudonymizer.primaryKeyId + "_"
        val incompatible =
          stored
            .filter(
              activeData && (
                col("subjectToken").isNull || !col("subjectToken").startsWith(expectedPrefix)
              )
            )
            .limit(1)
            .count()
        if (incompatible > 0L)
          throw AnalyticsError.InvalidConfiguration(
            "unexpired Silver rows use a different HMAC key; retain the old primary until Silver retention expires"
          )
      }
    }
  }

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

  private[analytics] def validateStoredTokenKeys(spark: SparkSession): IO[Unit] = lakehouse {
    Vector(paths.bronze, paths.quarantine, paths.silver).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) {
        val frame = spark.read.format("delta").load(path)
        val tokenFrames = Vector(
          Option.when(frame.columns.contains("subjectTokens"))(
            frame.select(explode(col("subjectTokens")).as("token"))
          ),
          Option.when(frame.columns.contains("subjectToken"))(
            frame.select(col("subjectToken").as("token"))
          )
        ).flatten
        val tokenValues = tokenFrames
          .reduceOption(_.unionByName(_))
          .getOrElse(
            frame.limit(0).select(lit(null).cast(StringType).as("token"))
          )
        val allowedKeyIds = pseudonymizer.keyIds.toVector.sorted.mkString("(?:", "|", ")")
        val tokenPattern = s"^${allowedKeyIds}_[A-Za-z0-9_-]{43}$$"
        if (tokenValues.filter(col("token").isNotNull && !col("token").rlike(tokenPattern)).limit(1).count() > 0L)
          throw AnalyticsError.InvalidConfiguration(
            "stored analytical rows require an HMAC key that is not configured"
          )
      }
    }
  }

  private def validateKeyMaterialContinuityLocked(spark: SparkSession): IO[Unit] = lakehouse {
    val registryExists = DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry)
    if (!registryExists) {
      val existingAnalyticsData = Vector(
        paths.bronze,
        paths.quarantine,
        paths.silver,
        paths.funnelGold,
        paths.timeToHireGold,
        paths.skillsGold,
        paths.manifests
      ).exists(DeltaTable.isDeltaTable(spark, _))
      if (existingAnalyticsData)
        throw AnalyticsError.InvalidConfiguration(
          "existing lakehouse has no HMAC key continuity registry; startup fails closed, reset or rebuild this local lakehouse explicitly before reuse"
        )
      val initial = spark.createDataFrame(
        pseudonymizer.keyVerifiers.map { case (keyId, verifier) => Row(keyId, verifier) }.asJava,
        StructType(
          Seq(
            StructField("keyId", StringType, nullable = false),
            StructField("verifier", StringType, nullable = false)
          )
        )
      )
      initial.write.format("delta").mode("errorifexists").save(paths.hmacKeyRegistry)
    } else {
      val existingRows = spark.read
        .format("delta")
        .load(paths.hmacKeyRegistry)
        .select("keyId", "verifier")
        .collect()
        .toVector
      val registryRowsAreValid = existingRows.forall { row =>
        val keyId = row.getString(0)
        val verifier = row.getString(1)
        keyId != null && keyId.matches("[A-Za-z0-9-]{1,40}") &&
        verifier != null && verifier.matches("[A-Za-z0-9_-]{43}")
      }
      if (!registryRowsAreValid || existingRows.map(_.getString(0)).distinct.size != existingRows.size)
        throw AnalyticsError.InvalidConfiguration("HMAC key continuity registry is malformed")
      val existing = existingRows.map(row => row.getString(0) -> row.getString(1)).toMap
      val removedKey = existing.keys.find(keyId => !pseudonymizer.keyIds.contains(keyId))
      removedKey.foreach { keyId =>
        throw AnalyticsError.InvalidConfiguration(
          s"HMAC key '$keyId' cannot be removed: audited historical-data cleanup and writer-exclusion verification are not implemented"
        )
      }
      val mismatched = pseudonymizer.keyVerifiers.find { case (keyId, verifier) =>
        existing.get(keyId).exists(_ != verifier)
      }
      mismatched.foreach { case (keyId, _) =>
        throw AnalyticsError.InvalidConfiguration(s"HMAC key material changed without a new key ID: $keyId")
      }
      val added = pseudonymizer.keyVerifiers.filterNot { case (keyId, _) => existing.contains(keyId) }
      val unanchoredStoredKey = added.find { case (keyId, _) => hasStoredTokenForKey(spark, keyId) }
      unanchoredStoredKey.foreach { case (keyId, _) =>
        throw AnalyticsError.InvalidConfiguration(
          s"stored rows use HMAC key ID '$keyId' without a continuity anchor; verify provenance before registering it"
        )
      }
      if (added.nonEmpty) {
        spark
          .createDataFrame(
            added.map { case (keyId, verifier) => Row(keyId, verifier) }.asJava,
            StructType(
              Seq(
                StructField("keyId", StringType, nullable = false),
                StructField("verifier", StringType, nullable = false)
              )
            )
          )
          .write
          .format("delta")
          .mode("append")
          .save(paths.hmacKeyRegistry)
      }
    }
  }

  private def hasStoredTokenForKey(spark: SparkSession, keyId: String): Boolean =
    Vector(paths.bronze, paths.quarantine, paths.silver).exists { path =>
      if (!DeltaTable.isDeltaTable(spark, path)) false
      else {
        val frame = spark.read.format("delta").load(path)
        val tokenFrames = Vector(
          Option.when(frame.columns.contains("subjectTokens"))(
            frame.select(explode(col("subjectTokens")).as("token"))
          ),
          Option.when(frame.columns.contains("subjectToken"))(
            frame.select(col("subjectToken").as("token"))
          )
        ).flatten
        tokenFrames
          .reduceOption(_.unionByName(_))
          .exists(_.filter(col("token").startsWith(keyId + "_")).limit(1).count() > 0L)
      }
    }

  private[analytics] def validateKeyMaterialContinuity(spark: SparkSession): IO[Unit] =
    AnalyticsLakehouseLock.resource(paths.root).use(_ => validateKeyMaterialContinuityLocked(spark))

  private def validateHmacConfigurationLocked(spark: SparkSession): IO[Unit] =
    for {
      _ <- validateKeyMaterialContinuityLocked(spark)
      keyCheckAt <- now
      _ <- validateStoredTokenKeys(spark)
      _ <- ensurePrimaryTokenCompatibility(spark, keyCheckAt)
    } yield ()

  private[analytics] def validateHmacConfiguration(spark: SparkSession): IO[Unit] =
    AnalyticsLakehouseLock.resource(paths.root).use(_ => validateHmacConfigurationLocked(spark))

  private def applyActiveDeletions(spark: SparkSession, markerTokens: DataFrame): IO[Unit] =
    for {
      deletionTime <- now
      _ <- expire(spark, paths.bronze, deletionTime)
      _ <- expire(spark, paths.quarantine, deletionTime)
      _ <- expire(spark, paths.silver, deletionTime)
      _ <- purgeMarkedSubjectRows(spark, paths.bronze, markerTokens)
      _ <- purgeMarkedSubjectRows(spark, paths.quarantine, markerTokens)
      _ <- purgeMarkedSubjectRows(spark, paths.silver, markerTokens)
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

  private[analytics] def verifyMarkedSubjectsAbsent(
      spark: SparkSession,
      markerTokens: DataFrame
  ): IO[Unit] = lakehouse {
    val marker = markerTokens.select(col("subjectToken")).filter(col("subjectToken").isNotNull).distinct()
    val rawPaths = Set(paths.bronze, paths.quarantine)
    Vector(paths.bronze, paths.quarantine, paths.silver).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) {
        val frame = spark.read.format("delta").load(path)
        val columns = frame.columns.toSet
        val matched =
          if (columns.contains("subjectTokens"))
            frame
              .as("stored")
              .join(
                marker.as("marker"),
                array_contains(col("stored.subjectTokens"), col("marker.subjectToken")),
                "left_semi"
              )
          else if (columns.contains("subjectToken"))
            frame
              .as("stored")
              .join(marker.as("marker"), col("stored.subjectToken") === col("marker.subjectToken"), "left_semi")
          else if (rawPaths.contains(path)) frame
          else frame
        if (matched.limit(1).count() > 0L)
          throw AnalyticsError.LakehouseFailure(
            new IllegalStateException(s"marked subject remains in Delta dataset $path")
          )
      }
    }
  }

  /** Counts rows that the erasure rewrite must invalidate, including whole datasets without attribution columns. */
  private[analytics] def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): IO[Long] = lakehouse {
    val marker = markerTokens.select(col("subjectToken")).filter(col("subjectToken").isNotNull).distinct()
    Vector(paths.bronze, paths.quarantine, paths.silver).foldLeft(0L) { (total, path) =>
      if (!DeltaTable.isDeltaTable(spark, path)) total
      else {
        val frame = spark.read.format("delta").load(path)
        val columns = frame.columns.toSet
        val matched =
          if (columns.contains("subjectTokens"))
            frame
              .as("stored")
              .join(
                marker.as("marker"),
                array_contains(col("stored.subjectTokens"), col("marker.subjectToken")),
                "left_semi"
              )
          else if (columns.contains("subjectToken"))
            frame
              .as("stored")
              .join(
                marker.as("marker"),
                col("stored.subjectToken") === col("marker.subjectToken"),
                "left_semi"
              )
          else frame
        total + matched.count()
      }
    }
  }

  /** Captures the exact active Delta files containing rows invalidated by the deletion rewrite. */
  private[analytics] def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): IO[Vector[String]] =
    configureRawTablePrivacy(spark) *> lakehouse {
      val marker = markerTokens.select(col("subjectToken")).filter(col("subjectToken").isNotNull).distinct()
      val rawPaths = Set(paths.bronze, paths.quarantine)
      val files = Vector(paths.bronze, paths.quarantine, paths.silver).flatMap { path =>
        if (!DeltaTable.isDeltaTable(spark, path)) Vector.empty
        else {
          val frame = spark.read.format("delta").load(path)
          val columns = frame.columns.toSet
          val attributed =
            if (columns.contains("subjectTokens"))
              frame
                .as("stored")
                .join(
                  marker.as("marker"),
                  array_contains(col("stored.subjectTokens"), col("marker.subjectToken")),
                  "left_semi"
                )
            else if (columns.contains("subjectToken"))
              frame
                .as("stored")
                .join(marker.as("marker"), col("stored.subjectToken") === col("marker.subjectToken"), "left_semi")
            else frame
          val affected =
            if (rawPaths.contains(path) && !columns.contains("subjectTokens")) frame
            else if (rawPaths.contains(path) && columns.contains("subjectTokens")) {
              val unattributed = frame.filter(col("subjectTokens").isNull || size(col("subjectTokens")) === 0)
              attributed.unionByName(unattributed, allowMissingColumns = true)
            } else attributed
          val dataFiles = affected
            .select(input_file_name().as("filePath"))
            .distinct()
            .limit(MaximumErasureEvidenceFiles + 1)
            .collect()
            .toVector
            .map(_.getString(0))
            .distinct
          val logFiles = if (rawPaths.contains(path)) rawLogFiles(spark, path, None) else Vector.empty
          dataFiles ++ logFiles
        }
      }.distinct
      if (files.size > MaximumErasureEvidenceFiles)
        throw AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
      files
    }

  /** Writes a clean current-state checkpoint immediately after the purge and returns only older log paths. Delta needs
    * a checkpoint at or before the retention cutoff to eventually remove earlier log files.
    */
  private[analytics] def checkpointPurgedRawLogs(spark: SparkSession): IO[Vector[String]] = lakehouse {
    val retiredLogs = Vector(paths.bronze, paths.quarantine).flatMap { path =>
      if (!DeltaTable.isDeltaTable(spark, path)) Vector.empty
      else {
        val log = DeltaLog.forTable(spark, path)
        // A no-op DELETE/MERGE does not create a Delta commit. Advance the table with a
        // harmless, unique metadata commit so every path captured before this checkpoint
        // is strictly older than the clean checkpoint boundary. This also makes a retry
        // after a crash establish a fresh boundary instead of persisting its own baseline
        // checkpoint JSON as evidence that must later disappear.
        val tableIdentifier = path.replace("`", "``")
        val checkpointNonce = java.util.UUID.randomUUID().toString
        spark.sql(
          s"ALTER TABLE delta.`$tableIdentifier` SET TBLPROPERTIES ('analytics.erasureCheckpointNonce' = '$checkpointNonce')"
        )
        val snapshot = log.update()
        val oldLogs = rawLogFiles(spark, path, Some(snapshot.version))
        log.checkpointAndCleanUpDeltaLog(snapshot, None)
        oldLogs
      }
    }.distinct
    if (retiredLogs.size > MaximumErasureEvidenceFiles)
      throw AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
    retiredLogs
  }

  private def rawLogFiles(spark: SparkSession, tablePath: String, beforeVersion: Option[Long]): Vector[String] = {
    val logDirectory = new org.apache.hadoop.fs.Path(s"$tablePath/_delta_log")
    val fileSystem = logDirectory.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(logDirectory)) Vector.empty
    else {
      val paths = Iterator
        .unfold(fileSystem.listFiles(logDirectory, false)) { entries =>
          if (entries.hasNext) Some(entries.next() -> entries) else None
        }
        .filter { status =>
          val prefix = status.getPath.getName.take(20)
          Try(prefix.toLong).exists(version => beforeVersion.forall(version < _))
        }
        .map(_.getPath.toString)
        .take(MaximumErasureEvidenceFiles + 1)
        .toVector
      if (paths.size > MaximumErasureEvidenceFiles)
        throw AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
      paths
    }
  }

  /** Verifies the specific pre-purge files are physically absent after retention-safe VACUUM. */
  private[analytics] def verifyFilesAbsent(spark: SparkSession, files: Vector[String]): IO[Unit] = lakehouse {
    val configuration = spark.sparkContext.hadoopConfiguration
    val remaining = files.filter { value =>
      val path = new org.apache.hadoop.fs.Path(value)
      path.getFileSystem(configuration).exists(path)
    }
    if (remaining.nonEmpty) throw AnalyticsError.PhysicalReclamationUnverified
  }

  private[analytics] def checkpointRawTableLogs(spark: SparkSession): IO[Unit] = lakehouse {
    Vector(paths.bronze, paths.quarantine).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) {
        val log = DeltaLog.forTable(spark, path)
        val snapshot = log.update()
        log.checkpointAndCleanUpDeltaLog(snapshot, None)
      }
    }
  }

  private def purgeMarkedSubjectRows(
      spark: SparkSession,
      path: String,
      markerTokens: DataFrame
  ): IO[Unit] =
    lakehouse {
      if (DeltaTable.isDeltaTable(spark, path)) {
        val markedSubjects = markerTokens.select(col("subjectToken")).filter(col("subjectToken").isNotNull).distinct()
        val columns = spark.read.format("delta").load(path).columns.toSet
        val rawScope = path == paths.bronze || path == paths.quarantine
        if (rawScope) {
          val table = DeltaTable.forPath(spark, path)
          if (columns.contains("subjectTokens"))
            table.delete(col("subjectTokens").isNull || size(col("subjectTokens")) === 0)
          else table.delete()
        }
        val condition =
          if (columns.contains("subjectTokens"))
            "array_contains(target.subjectTokens, source.subjectToken)" +
              (if (columns.contains("subjectToken")) " OR target.subjectToken = source.subjectToken" else "")
          else if (columns.contains("subjectToken")) "target.subjectToken = source.subjectToken"
          else {
            DeltaTable.forPath(spark, path).delete()
            ""
          }
        if (condition.nonEmpty)
          DeltaTable
            .forPath(spark, path)
            .as("target")
            .merge(markedSubjects.as("source"), condition)
            .whenMatched()
            .delete()
            .execute()
      }
    }

  /** Deletion is applied to rebuildable Gold immediately, even if the new Kafka range later quality-blocks. */
  private def rebuildGoldFromStoredSilver(spark: SparkSession): IO[Unit] =
    lakehouse(DeltaTable.isDeltaTable(spark, paths.silver)).flatMap {
      case true =>
        for {
          allSilver <- lakehouse(spark.read.format("delta").load(paths.silver))
          funnel <- lakehouse(HiringGoldTransforms.wideFunnelDay(allSilver))
          _ <- overwrite(funnel, paths.funnelGold)
          timeToHire <- HiringGoldTransforms.timeToHire(allSilver).adaptError { case NonFatal(cause) =>
            AnalyticsError.LakehouseFailure(cause)
          }
          _ <- overwrite(timeToHire, paths.timeToHireGold)
          skills <- lakehouse(HiringGoldTransforms.skillPostingActivity(allSilver))
          _ <- overwrite(skills, paths.skillsGold)
        } yield ()
      case false =>
        lakehouse {
          Vector(paths.funnelGold, paths.timeToHireGold, paths.skillsGold).foreach { path =>
            if (DeltaTable.isDeltaTable(spark, path)) DeltaTable.forPath(spark, path).delete()
          }
        }
    }

  private final case class BronzeInput(frame: DataFrame, startedAt: Instant, records: Long)
  private final case class PreparedEvents(
      incomingSilver: DataFrame,
      conflicts: DataFrame,
      validRecords: Long,
      suppressedRecords: Long,
      quarantinedRecords: Long,
      conflictingEventIds: Long
  )

  private def runWithMarkers(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean,
      reservation: Option[AnalyticsReportReservation]
  ): IO[AnalyticsPublication] =
    for {
      bronze <- ingestBronze(spark, source, manifest, markerTokens)
      prepared <- separateQuarantine(spark, bronze, markerTokens, activeMarkersPresent)
      silver <- mergeSilver(prepared, bronze.startedAt)
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

  private def ingestBronze(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame
  ): IO[BronzeInput] =
    for {
      raw <- source.read(spark, manifest)
      rawSchema <- lakehouse(raw.schema)
      _ <- KafkaRecordColumns.validate(rawSchema)
      _ <- source.verifyOffsets(raw, manifest)
      _ <- validateRunIdentity(spark, manifest)
      parsed <- lakehouse(OperationalEventTransforms.parseKafkaRecords(raw))
      valid <- lakehouse(OperationalEventTransforms.validEvents(parsed))
      pseudonymized <- lakehouse(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safeToPersist <- lakehouse(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(pseudonymized, markerTokens))
      startedAt <- now
      incoming <- lakehouse(
        withExpiry(OperationalEventTransforms.bronze(safeToPersist), startedAt, AnalyticsRetention.BronzeDays)
      )
      recordCount <- lakehouse(raw.count())
      _ <- persistManifest(spark, manifest, "STARTED", startedAt.toString)
      _ <- merge(
        incoming,
        paths.bronze,
        "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
      )
    } yield BronzeInput(parsed, startedAt, recordCount)

  private def validateRunIdentity(spark: SparkSession, manifest: AnalyticsRunManifest): IO[Unit] = lakehouse {
    if (DeltaTable.isDeltaTable(spark, paths.manifests)) {
      val existing = spark.read
        .format("delta")
        .load(paths.manifests)
        .filter(col("runId") === lit(manifest.runId.value))
        .select("topic", "partition", "startOffset", "endOffsetExclusive")
        .distinct()
        .collect()
        .toVector
        .map(row => (row.getString(0), row.getInt(1), row.getLong(2), row.getLong(3)))
        .toSet
      val expected = manifest.offsetRanges
        .map(range => (range.topic, range.partition, range.startOffset, range.endOffsetExclusive))
        .toSet
      if (existing.nonEmpty && existing != expected) throw AnalyticsError.RunIdRangeConflict(manifest.runId.value)
    }
  }

  private def separateQuarantine(
      spark: SparkSession,
      bronze: BronzeInput,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean
  ): IO[PreparedEvents] = {
    val parsed = bronze.frame
    for {
      valid <- lakehouse(OperationalEventTransforms.validEvents(parsed))
      pseudonymizedValid <- lakehouse(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safeValid <- lakehouse(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(pseudonymizedValid, markerTokens))
      malformed <- lakehouse(
        withExpiry(
          AnalyticsSubjectPrivacy
            .withSubjectToken(OperationalEventTransforms.malformedEvents(parsed), pseudonymizer)
            .withColumn("quarantineId", quarantineId)
            .withColumn("quarantineReason", lit("INVALID_OPERATIONAL_EVENT_ENVELOPE")),
          bronze.startedAt,
          AnalyticsRetention.QuarantineDays
        )
      )
      malformedCount <- lakehouse(malformed.count())
      malformedToPersist <-
        if (activeMarkersPresent) lakehouse(malformed.limit(0)) else IO.pure(malformed)
      validRecords <- lakehouse(valid.count())
      safeValidRecords <- lakehouse(safeValid.count())
      suppressedRecords = validRecords - safeValidRecords
      newConflicts <- lakehouse(OperationalEventTransforms.conflictingEventIds(safeValid))
      incomingSilver <- lakehouse(OperationalEventTransforms.silver(safeValid, pseudonymizer, markerTokens))
      incomingSilverSchema <- lakehouse(incomingSilver.schema)
      storedSilver <- readOrEmpty(spark, paths.silver, incomingSilverSchema)
      historicalConflicts <- lakehouse(
        safeValid
          .select("eventId", "rawValue")
          .withColumn("incomingFingerprint", sha2(col("rawValue"), 256))
          .join(
            storedSilver
              .select("eventId", "eventFingerprint")
              .withColumnRenamed("eventFingerprint", "storedFingerprint"),
            Seq("eventId"),
            "inner"
          )
          .filter(col("incomingFingerprint") =!= col("storedFingerprint"))
          .select("eventId")
          .distinct()
      )
      conflicts <- lakehouse(newConflicts.unionByName(historicalConflicts).distinct())
      conflictingEventIds <- lakehouse(conflicts.count())
      conflictQuarantine <- lakehouse(
        withExpiry(
          AnalyticsSubjectPrivacy
            .withSubjectToken(safeValid.join(conflicts, Seq("eventId"), "inner"), pseudonymizer)
            .withColumn("quarantineId", quarantineId)
            .withColumn("quarantineReason", lit("CONFLICTING_EVENT_ID")),
          bronze.startedAt,
          AnalyticsRetention.QuarantineDays
        )
      )
      conflictingRecords <- lakehouse(conflictQuarantine.count())
      quarantine <- lakehouse(
        malformedToPersist
          .unionByName(conflictQuarantine)
          .withColumn("payloadHash", sha2(col("rawValue"), 256))
          .drop("rawValue", "actorId", "payload", "subjectToken")
          .select(
            "topic",
            "partition",
            "offset",
            "payloadHash",
            "subjectTokens",
            "quarantineId",
            "quarantineReason",
            "expiresAt"
          )
      )
      _ <- merge(quarantine, paths.quarantine, "target.quarantineId = source.quarantineId")
    } yield PreparedEvents(
      incomingSilver,
      conflicts,
      validRecords,
      suppressedRecords,
      malformedCount + conflictingRecords,
      conflictingEventIds
    )
  }

  private def mergeSilver(prepared: PreparedEvents, startedAt: Instant): IO[DataFrame] =
    for {
      silver <- lakehouse(
        withExpiry(
          prepared.incomingSilver.join(prepared.conflicts, Seq("eventId"), "left_anti"),
          startedAt,
          AnalyticsRetention.SilverDays
        )
      )
      _ <- merge(silver, paths.silver, "target.eventId = source.eventId")
    } yield silver

  private def expireStored(spark: SparkSession, startedAt: Instant): IO[Unit] =
    for {
      _ <- expire(spark, paths.bronze, startedAt)
      _ <- expire(spark, paths.quarantine, startedAt)
      _ <- expire(spark, paths.silver, startedAt)
    } yield ()

  private def vacuumExpiredFiles(spark: SparkSession): IO[Long] = lakehouse {
    val tables = Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold
    )
    tables.foldLeft(0L) { (removedFiles, path) =>
      if (DeltaTable.isDeltaTable(spark, path)) {
        val temporaryPath = s"${paths.root.stripSuffix("/")}/control/purge-rewrite-${UUID.randomUUID()}"
        val fileSystem =
          new org.apache.hadoop.fs.Path(temporaryPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
        try {
          spark.read.format("delta").load(path).write.format("delta").mode("overwrite").save(temporaryPath)
          spark.read
            .format("delta")
            .load(temporaryPath)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        } finally {
          val _ = fileSystem.delete(new org.apache.hadoop.fs.Path(temporaryPath), true)
        }
        // Respect Delta's retention safety horizon. A deletion is not complete until this reclaim horizon has passed.
        removedFiles + DeltaTable.forPath(spark, path).vacuum().count()
      } else removedFiles
    }
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
        funnel <- lakehouse(HiringGoldTransforms.wideFunnelDay(allSilver))
        _ <- overwrite(funnel, paths.funnelGold)
        timeToHire <- HiringGoldTransforms.timeToHire(allSilver).adaptError { case NonFatal(cause) =>
          AnalyticsError.LakehouseFailure(cause)
        }
        _ <- overwrite(timeToHire, paths.timeToHireGold)
        skills <- lakehouse(HiringGoldTransforms.skillPostingActivity(allSilver))
        _ <- overwrite(skills, paths.skillsGold)
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

  private def extractReport(spark: SparkSession, asOf: Instant): IO[AnalyticsReportOutput] = lakehouse {
    def rows(path: String): Vector[Row] = {
      if (!DeltaTable.isDeltaTable(spark, path)) Vector.empty
      else {
        val result = spark.read.format("delta").load(path).limit(MaximumReportRows + 1).collect().toVector
        if (result.size > MaximumReportRows)
          throw AnalyticsError.LakehouseFailure(
            new IllegalStateException(s"report output exceeds $MaximumReportRows rows")
          )
        result
      }
    }
    val funnel = rows(paths.funnelGold).map(row =>
      AnalyticsFunnelDayOutput(
        row.getAs[Timestamp]("day").toInstant,
        row.getAs[Long]("created"),
        row.getAs[Long]("accepted"),
        row.getAs[Long]("declined"),
        row.getAs[Long]("interview"),
        row.getAs[Long]("hired"),
        row.getAs[Long]("rejected")
      )
    )
    val timeRows = rows(paths.timeToHireGold)
    if (timeRows.size > 1)
      throw AnalyticsError.LakehouseFailure(new IllegalStateException("time-to-hire report is not singular"))
    val timeToHire = timeRows.headOption.map(row =>
      AnalyticsTimeToHireOutput(
        row.getAs[Double]("p50Hours"),
        row.getAs[Double]("p75Hours"),
        row.getAs[Double]("p90Hours"),
        row.getAs[Double]("p95Hours"),
        row.getAs[Long]("eligibleCount"),
        row.getAs[Long]("excludedCount")
      )
    )
    val skills = rows(paths.skillsGold).map(row =>
      AnalyticsSkillPostingDayOutput(
        row.getAs[Timestamp]("day").toInstant,
        row.getAs[String]("skill"),
        row.getAs[Long]("postings")
      )
    )
    AnalyticsReportOutput(asOf, funnel, timeToHire, skills)
  }

  private def rangeFingerprint(manifest: AnalyticsRunManifest): String = {
    val canonical = manifest.offsetRanges
      .sortBy(range => (range.topic, range.partition))
      .map(range => s"${range.topic}:${range.partition}:${range.startOffset}:${range.endOffsetExclusive}")
      .mkString("\n")
    java.security.MessageDigest
      .getInstance("SHA-256")
      .digest(canonical.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
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

  private def overwrite(source: DataFrame, path: String): IO[Unit] =
    lakehouse(source.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))

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

private[analytics] object KafkaRecordColumns {
  def validate(schema: StructType): IO[Unit] = {
    val required = Set("topic", "partition", "offset", "timestamp", "value")
    val missing = required.diff(schema.fieldNames.toSet).toVector.sorted
    if (missing.isEmpty) IO.unit else IO.raiseError(AnalyticsError.InvalidSourceSchema(missing))
  }
}

/** Bounded batch entry point. Runtime settings and the explicit offset range load from HOCON. */
object HiringAnalyticsBatchMain extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  private[analytics] def managedResources(
      acquireSpark: IO[SparkSession],
      acquireMongo: IO[MongoClient]
  ): Resource[IO, (SparkSession, MongoClient)] =
    for {
      spark <- Resource.make(acquireSpark.adaptError { case NonFatal(cause) =>
        AnalyticsError.SparkStartupFailure(cause)
      })(session =>
        IO.blocking(session.stop()).adaptError { case NonFatal(cause) =>
          AnalyticsError.LakehouseFailure(cause)
        }
      )
      mongo <- Resource.make(acquireMongo.adaptError {
        case _: IllegalArgumentException => AnalyticsError.InvalidConfiguration("MONGODB_URI is invalid")
        case NonFatal(cause)             => AnalyticsError.MongoConnectionFailure(cause)
      })(client =>
        IO.blocking(client.close()).adaptError { case NonFatal(cause) =>
          AnalyticsError.MongoConnectionFailure(cause)
        }
      )
    } yield (spark, mongo)

  private[analytics] def resources(mongoUri: String, sparkMaster: String): Resource[IO, (SparkSession, MongoClient)] =
    managedResources(
      IO.blocking(
        org.apache.spark.sql.classic.SparkSession
          .builder()
          .appName("hiring-analytics-batch")
          .master(sparkMaster)
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
      ),
      IO.blocking(MongoClients.create(mongoUri))
    )

  private def program: IO[AnalyticsPublication] =
    AnalyticsRuntimeConfig.loadBatch.flatMap { configured =>
      val common = configured.common
      resources(common.mongoUri, common.sparkMaster).use { case (spark, mongo) =>
        IO.blocking(mongo.getDatabase(common.mongoDatabase))
          .adaptError {
            case _: IllegalArgumentException =>
              AnalyticsError.InvalidConfiguration("analytics.mongo.database is invalid")
            case NonFatal(cause) => AnalyticsError.MongoConnectionFailure(cause)
          }
          .flatMap { database =>
            val markers = new MongoActiveDeletionMarkerSource(database, common.pseudonymizer)
            new HiringAnalyticsBatch(
              common.lakehousePaths,
              common.pseudonymizer,
              markers,
              reportPublisher = Some(new MongoAnalyticsReportPublisher(mongo, database))
            )
              .run(spark, new KafkaOffsetRangeSource(common.kafka), configured.manifest)
          }
      }
    }

  override def run(args: List[String]): IO[ExitCode] =
    (if (args.nonEmpty)
       IO.raiseError[AnalyticsPublication](
         AnalyticsError.InvalidConfiguration(
           "batch inputs are loaded from HOCON; command-line arguments are not accepted"
         )
       )
     else program).attempt.flatMap {
      case Right(publication)          => logger.info(publication.toString).as(ExitCode.Success)
      case Left(error: AnalyticsError) => logger.error(error.getMessage).as(ExitCode.Error)
      case Left(error)                 =>
        logger.error(s"analytics batch failed unexpectedly: ${error.getClass.getSimpleName}").as(ExitCode.Error)
    }
}
