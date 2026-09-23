package com.example.hiring.analytics

import cats.data.ValidatedNec
import cats.effect.{Clock, ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, struct, to_json, when}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import com.mongodb.client.{MongoClient, MongoClients}

import java.time.Instant
import java.sql.Timestamp
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

/** A bounded source is intentionally separate from storage and publication. */
trait BoundedOperationalEventSource {
  def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[DataFrame]
}

/** Supplies only current HMAC tokens, allowing the batch to remain independent of MongoDB. */
trait ActiveDeletionMarkerSource {
  def activeSubjectTokens(spark: SparkSession): IO[DataFrame]
}

final case class DataFrameDeletionMarkerSource(tokens: DataFrame) extends ActiveDeletionMarkerSource {
  override def activeSubjectTokens(spark: SparkSession): IO[DataFrame] = IO.pure(tokens)
}

final case class KafkaConnection(bootstrapServers: String)

object KafkaConnection {
  def validate(connection: KafkaConnection): ValidatedNec[String, KafkaConnection] =
    if (connection.bootstrapServers != null && connection.bootstrapServers.trim.nonEmpty) connection.validNec
    else "Kafka bootstrap servers must be non-empty".invalidNec
}

/** Reads exactly the offsets named by a manifest; it never starts a streaming query. */
final class KafkaOffsetRangeSource(connection: KafkaConnection) extends BoundedOperationalEventSource {
  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[DataFrame] =
    for {
      _ <- IO.fromEither(KafkaConnection.validate(connection).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      _ <- IO.fromEither(AnalyticsRunManifest.validate(manifest).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      frame <- IO.blocking {
        val topic = manifest.offsetRanges.head.topic
        spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", connection.bootstrapServers)
          .option("subscribe", topic)
          .option("startingOffsets", KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.startOffset))
          .option("endingOffsets", KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.endOffsetExclusive))
          .load()
      }.adaptError { case NonFatal(cause) => AnalyticsError.SourceReadFailure(cause) }
    } yield frame
}

object KafkaOffsetRangeSource {
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
}

/** Test and backfill adapter. Its frame must have Kafka's topic, partition, offset, timestamp and value columns. */
final case class DataFrameBatchSource(records: DataFrame) extends BoundedOperationalEventSource {
  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[DataFrame] =
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
}

object AnalyticsLakehousePaths {
  def validate(paths: AnalyticsLakehousePaths): ValidatedNec[String, AnalyticsLakehousePaths] =
    if (paths.root != null && paths.root.trim.nonEmpty) paths.validNec
    else "lakehouse root must be non-empty".invalidNec
}

sealed trait AnalyticsRunOutcome

object AnalyticsRunOutcome {
  case object QualityBlocked extends AnalyticsRunOutcome
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
    clock: Clock[IO] = Clock[IO]
) {
  private def now: IO[Instant] = clock.realTime.map(duration => Instant.ofEpochMilli(duration.toMillis))

  private def lakehouse[A](work: => A): IO[A] =
    IO.blocking(work).adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
    }

  private def cachedMarkers(spark: SparkSession): Resource[IO, DataFrame] =
    Resource.make(
      deletionMarkers.activeSubjectTokens(spark).flatMap(frame => lakehouse(frame.persist(StorageLevel.MEMORY_AND_DISK)))
    )(frame => lakehouse(frame.unpersist(blocking = true)).void)

  def run(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest
  ): IO[AnalyticsPublication] =
    for {
      _ <- IO.fromEither(AnalyticsLakehousePaths.validate(paths).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      _ <- IO.fromEither(AnalyticsRunManifest.validate(manifest).toEither.leftMap(AnalyticsError.InvalidInput.apply))
      publication <- cachedMarkers(spark).use { markerTokens =>
        for {
          _ <- validateMarkerColumns(markerTokens)
          activeMarkerCount <- lakehouse(markerTokens.count())
          _ <- if (activeMarkerCount > 0L) applyActiveDeletions(spark, markerTokens) else IO.unit
          result <- runWithMarkers(spark, source, manifest, markerTokens)
        } yield result
      }
    } yield publication

  private def validateMarkerColumns(frame: DataFrame): IO[Unit] =
    lakehouse(frame.columns.toVector).flatMap { columns =>
      if (columns.contains("subjectToken")) IO.unit
      else IO.raiseError(AnalyticsError.InvalidSourceSchema(Vector("subjectToken")))
    }

  private def applyActiveDeletions(spark: SparkSession, markerTokens: DataFrame): IO[Unit] =
    for {
      deletionTime <- now
      _ <- expire(spark, paths.silver, deletionTime)
      _ <- purgeMarkedSilver(spark, markerTokens)
      _ <- rebuildGoldFromStoredSilver(spark)
    } yield ()

  private def purgeMarkedSilver(spark: SparkSession, markerTokens: DataFrame): IO[Unit] =
    lakehouse {
      if (DeltaTable.isDeltaTable(spark, paths.silver)) {
      val markedSubjects = markerTokens
        .select(col("subjectToken"))
        .filter(col("subjectToken").isNotNull)
        .distinct()
      DeltaTable
        .forPath(spark, paths.silver)
        .as("target")
        .merge(markedSubjects.as("source"), "target.subjectToken = source.subjectToken")
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
          timeToHire <- HiringGoldTransforms.timeToHire(allSilver).adaptError {
            case NonFatal(cause) => AnalyticsError.LakehouseFailure(cause)
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

  private def runWithMarkers(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame
  ): AnalyticsPublication = {
    import spark.implicits._

    val rawRecords = source.read(spark, manifest)
    requireKafkaRecordColumns(rawRecords.schema)
    val startedAt = clock()
    val incomingBronze =
      withExpiry(OperationalEventTransforms.bronze(rawRecords), startedAt, AnalyticsRetention.BronzeDays)
    val bronzeRecords = incomingBronze.count()

    writeManifest(spark, manifest, "STARTED", startedAt.toString)
    merge(
      incomingBronze,
      paths.bronze,
      "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
    )

    val parsed = OperationalEventTransforms.parseKafkaRecords(incomingBronze)
    val valid = OperationalEventTransforms.validEvents(parsed)
    val malformed = withExpiry(
      OperationalEventTransforms
        .malformedEvents(parsed)
        .withColumn("quarantineId", quarantineId)
        .withColumn("quarantineReason", lit("INVALID_OPERATIONAL_EVENT_ENVELOPE")),
      startedAt,
      AnalyticsRetention.QuarantineDays
    )

    val validRecords = valid.count()
    val newConflicts = OperationalEventTransforms.conflictingEventIds(valid)
    val incomingSilver = OperationalEventTransforms.silver(valid, pseudonymizer, markerTokens)
    val storedSilver = readOrEmpty(spark, paths.silver, incomingSilver.schema)
    val historicalConflicts = valid
      .select("eventId", "rawValue")
      .withColumn("incomingFingerprint", sha2(col("rawValue"), 256))
      .join(
        storedSilver.select("eventId", "eventFingerprint").withColumnRenamed("eventFingerprint", "storedFingerprint"),
        Seq("eventId"),
        "inner"
      )
      .filter(col("incomingFingerprint") =!= col("storedFingerprint"))
      .select("eventId")
      .distinct()
    val conflicts = newConflicts.unionByName(historicalConflicts).distinct()
    val conflictingEventIds = conflicts.count()

    val conflictQuarantine = withExpiry(
      valid
        .join(conflicts, Seq("eventId"), "inner")
        .withColumn("quarantineId", quarantineId)
        .withColumn("quarantineReason", lit("CONFLICTING_EVENT_ID")),
      startedAt,
      AnalyticsRetention.QuarantineDays
    )
    val quarantine = malformed.unionByName(conflictQuarantine)
    val quarantinedRecords = quarantine.count()
    merge(quarantine, paths.quarantine, "target.quarantineId = source.quarantineId")

    val silver =
      withExpiry(incomingSilver.join(conflicts, Seq("eventId"), "left_anti"), startedAt, AnalyticsRetention.SilverDays)
    merge(silver, paths.silver, "target.eventId = source.eventId")

    val completedAt = clock()
    expire(spark, paths.bronze, startedAt)
    expire(spark, paths.quarantine, startedAt)
    expire(spark, paths.silver, startedAt)
    val outcome = if (quarantinedRecords > 0L) {
      writeManifest(spark, manifest, "QUALITY_BLOCKED", completedAt.toString)
      AnalyticsRunOutcome.QualityBlocked
    } else {
      val allSilver = readOrEmpty(spark, paths.silver, silver.schema)
      overwrite(HiringGoldTransforms.wideFunnelDay(allSilver), paths.funnelGold)
      overwrite(HiringGoldTransforms.timeToHire(allSilver), paths.timeToHireGold)
      overwrite(HiringGoldTransforms.skillPostingActivity(allSilver), paths.skillsGold)
      writeManifest(spark, manifest, "PUBLISHED", completedAt.toString)
      AnalyticsRunOutcome.Published
    }
    AnalyticsPublication(
      manifest.runId,
      outcome,
      completedAt,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold,
      bronzeRecords,
      validRecords,
      quarantinedRecords,
      conflictingEventIds
    )
  }

  private def writeManifest(
      spark: SparkSession,
      manifest: AnalyticsRunManifest,
      status: String,
      updatedAt: String
  ): Unit = {
    import spark.implicits._
    val rows = manifest.offsetRanges.map(range =>
      (manifest.runId, range.topic, range.partition, range.startOffset, range.endOffsetExclusive, status, updatedAt)
    )
    val frame = rows.toDF("runId", "topic", "partition", "startOffset", "endOffsetExclusive", "status", "updatedAt")
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

  private def merge(source: DataFrame, path: String, condition: String): Unit =
    if (DeltaTable.isDeltaTable(source.sparkSession, path))
      DeltaTable
        .forPath(source.sparkSession, path)
        .as("target")
        .merge(source.as("source"), condition)
        .whenNotMatched()
        .insertAll()
        .execute()
    else source.write.format("delta").mode("errorifexists").save(path)

  private def overwrite(source: DataFrame, path: String): Unit =
    source.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

  private def readOrEmpty(spark: SparkSession, path: String, schema: StructType): DataFrame =
    if (DeltaTable.isDeltaTable(spark, path)) spark.read.format("delta").load(path)
    else spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)

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

  private def expire(spark: SparkSession, path: String, now: Instant): Unit =
    if (DeltaTable.isDeltaTable(spark, path))
      DeltaTable
        .forPath(spark, path)
        .delete(col("expiresAt") <= lit(Timestamp.from(now)))

  private def requireKafkaRecordColumns(schema: StructType): Unit = {
    val required = Set("topic", "partition", "offset", "timestamp", "value")
    val missing = required.diff(schema.fieldNames.toSet)
    require(
      missing.isEmpty,
      s"Kafka batch records are missing required columns: ${missing.toSeq.sorted.mkString(", ")}"
    )
  }
}

/** Bounded batch entry point: `runId bootstrapServers lakehouseRoot topic partition startOffset endOffsetExclusive`.
  * Publication is deliberately represented by [[AnalyticsPublication]] rather than an OLTP write.
  */
object HiringAnalyticsBatchMain {
  def main(args: Array[String]): Unit = args.toList match {
    case runId :: bootstrapServers :: lakehouseRoot :: topic :: partition :: start :: end :: Nil =>
      val manifest =
        AnalyticsRunManifest(runId, Vector(PartitionOffsetRange(topic, partition.toInt, start.toLong, end.toLong)))
      val spark = SparkSession
        .builder()
        .appName("hiring-analytics-batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
      try {
        val hmacSecret = sys.env.getOrElse(
          "HIRING_ANALYTICS_HMAC_SECRET_BASE64",
          sys.error("HIRING_ANALYTICS_HMAC_SECRET_BASE64 is required")
        )
        val pseudonymizer = SubjectPseudonymizer.fromBase64(hmacSecret)
        val mongoUri = sys.env.getOrElse("MONGODB_URI", sys.error("MONGODB_URI is required"))
        val mongoDatabase = sys.env.getOrElse("MONGODB_DATABASE", "hiring")
        val mongoClient = com.mongodb.client.MongoClients.create(mongoUri)
        try {
          val markers = new MongoActiveDeletionMarkerSource(mongoClient.getDatabase(mongoDatabase), pseudonymizer)
          val publication = new HiringAnalyticsBatch(AnalyticsLakehousePaths(lakehouseRoot), pseudonymizer, markers)
            .run(spark, new KafkaOffsetRangeSource(KafkaConnection(bootstrapServers)), manifest)
          println(publication)
        } finally mongoClient.close()
      } finally spark.stop()
    case _ =>
      sys.error(
        "usage: HiringAnalyticsBatchMain runId bootstrapServers lakehouseRoot topic partition startOffset endOffsetExclusive"
      )
  }
}
