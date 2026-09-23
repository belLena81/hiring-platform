package com.example.hiring.analytics

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sha2}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.StructType

import java.time.Instant
import java.sql.Timestamp

/** A bounded source is intentionally separate from storage and publication. */
trait BoundedOperationalEventSource {
  def read(spark: SparkSession, manifest: AnalyticsRunManifest): DataFrame
}

/** Supplies only current HMAC tokens, allowing the batch to remain independent of MongoDB. */
trait ActiveDeletionMarkerSource {
  def activeSubjectTokens(spark: SparkSession): DataFrame
}

final case class DataFrameDeletionMarkerSource(tokens: DataFrame) extends ActiveDeletionMarkerSource {
  override def activeSubjectTokens(spark: SparkSession): DataFrame = tokens
}

final case class KafkaConnection(bootstrapServers: String) {
  require(bootstrapServers.trim.nonEmpty, "Kafka bootstrap servers must be non-empty")
}

/** Reads exactly the offsets named by a manifest; it never starts a streaming query. */
final class KafkaOffsetRangeSource(connection: KafkaConnection) extends BoundedOperationalEventSource {
  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): DataFrame = {
    val topics = manifest.offsetRanges.map(_.topic).distinct
    require(topics.size == 1, "a Kafka batch manifest must contain exactly one topic")

    val startOffsets = KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.startOffset)
    val endOffsets = KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.endOffsetExclusive)
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", connection.bootstrapServers)
      .option("subscribe", topics.head)
      .option("startingOffsets", startOffsets)
      .option("endingOffsets", endOffsets)
      .load()
  }
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
  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): DataFrame = {
    val inManifest = manifest.offsetRanges.foldLeft(lit(false): Column) { (condition, range) =>
      condition || (
        col("topic") === lit(range.topic) &&
          col("partition") === lit(range.partition) &&
          col("offset") >= lit(range.startOffset) &&
          col("offset") < lit(range.endOffsetExclusive)
      )
    }
    records.filter(inManifest)
  }
}

final case class AnalyticsLakehousePaths(root: String) {
  require(root.trim.nonEmpty, "lakehouse root must be non-empty")

  private val normalizedRoot = root.stripSuffix("/")
  val bronze: String = s"$normalizedRoot/bronze/operational_events"
  val silver: String = s"$normalizedRoot/silver/operational_events"
  val quarantine: String = s"$normalizedRoot/quarantine/operational_events"
  val funnelGold: String = s"$normalizedRoot/gold/application_funnel"
  val timeToHireGold: String = s"$normalizedRoot/gold/time_to_hire"
  val skillsGold: String = s"$normalizedRoot/gold/job_skills"
  val manifests: String = s"$normalizedRoot/control/run_manifests"
}

sealed trait AnalyticsRunOutcome

object AnalyticsRunOutcome {
  case object QualityBlocked extends AnalyticsRunOutcome
  case object Published extends AnalyticsRunOutcome
}

final case class AnalyticsPublication(
    runId: String,
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
    clock: () => Instant = () => Instant.now()
) {
  def run(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest
  ): AnalyticsPublication = {
    val markerTokens = deletionMarkers.activeSubjectTokens(spark).persist(StorageLevel.MEMORY_AND_DISK)
    try {
      markerTokens.count()
      runWithMarkers(spark, source, manifest, markerTokens)
    } finally markerTokens.unpersist(blocking = true)
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
        .withColumn("quarantineId", sha2(col("rawValue"), 256))
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
        .withColumn("quarantineId", sha2(col("rawValue"), 256))
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
        val emptyMarkers = new ActiveDeletionMarkerSource {
          override def activeSubjectTokens(spark: SparkSession): DataFrame = {
            import org.apache.spark.sql.types.{StringType, StructField, StructType}
            spark.createDataFrame(
              spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
              StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
            )
          }
        }
        val publication = new HiringAnalyticsBatch(AnalyticsLakehousePaths(lakehouseRoot), pseudonymizer, emptyMarkers)
          .run(spark, new KafkaOffsetRangeSource(KafkaConnection(bootstrapServers)), manifest)
        println(publication)
      } finally spark.stop()
    case _ =>
      sys.error(
        "usage: HiringAnalyticsBatchMain runId bootstrapServers lakehouseRoot topic partition startOffset endOffsetExclusive"
      )
  }
}
