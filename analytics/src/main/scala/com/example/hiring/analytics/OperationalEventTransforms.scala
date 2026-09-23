package com.example.hiring.analytics

import cats.effect.IO
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  col,
  count,
  countDistinct,
  date_trunc,
  explode,
  from_json,
  length,
  lit,
  lower,
  min,
  percentile_approx,
  sha2,
  to_timestamp,
  trim,
  unix_timestamp,
  when
}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object OperationalEventTransforms {
  private val PayloadSchema: StructType = StructType(
    Seq(
      StructField("applicationId", StringType, nullable = true),
      StructField("candidateId", StringType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("newStatus", StringType, nullable = true),
      StructField(
        "job",
        StructType(Seq(StructField("skills", ArrayType(StringType), nullable = true))),
        nullable = true
      )
    )
  )

  val EnvelopeSchema: StructType = StructType(
    Seq(
      StructField("eventId", StringType, nullable = true),
      StructField("eventType", StringType, nullable = true),
      StructField("occurredAt", StringType, nullable = true),
      StructField("aggregateType", StringType, nullable = true),
      StructField("aggregateId", StringType, nullable = true),
      StructField("actorId", StringType, nullable = true),
      StructField("payload", PayloadSchema, nullable = true)
    )
  )

  private val EventTypes = AnalyticsEventType.values.toSeq.map(_.wire)
  private val AggregateTypes = AnalyticsAggregateType.values.toSeq.map(_.wire)

  /** Parses the operational envelope and the analytics payload fields once using Spark's JSON support. The parsed
    * payload exists only before Silver projection; rawValue remains for Bronze, quarantine, and fingerprinting.
    */
  def parseKafkaRecords(records: DataFrame): DataFrame = {
    val parsed = records
      .withColumn("rawValue", col("value").cast(StringType))
      .withColumn("envelope", from_json(col("rawValue"), EnvelopeSchema))
    parsed.select(
      col("topic"),
      col("partition"),
      col("offset"),
      col("timestamp").as("kafkaTimestamp"),
      col("rawValue"),
      col("envelope.eventId"),
      col("envelope.eventType"),
      to_timestamp(col("envelope.occurredAt")).as("occurredAt"),
      col("envelope.aggregateType"),
      col("envelope.aggregateId"),
      col("envelope.actorId"),
      col("envelope.payload").as("payload")
    )
  }

  def bronze(records: DataFrame): DataFrame =
    records.dropDuplicates("topic", "partition", "offset")

  def validEvents(parsed: DataFrame): DataFrame =
    parsed.filter(
      requiredEnvelopeFields &&
        col("eventType").isin(EventTypes: _*) &&
        col("aggregateType").isin(AggregateTypes: _*)
    )

  def malformedEvents(parsed: DataFrame): DataFrame =
    parsed.filter(
      !(requiredEnvelopeFields &&
        col("eventType").isin(EventTypes: _*) &&
        col("aggregateType").isin(AggregateTypes: _*))
    )

  /** Event IDs are idempotency keys. Same bytes are duplicates; different bytes are conflicts. */
  def conflictingEventIds(valid: DataFrame): DataFrame =
    valid
      .withColumn("eventFingerprint", sha2(col("rawValue"), 256))
      .groupBy("eventId")
      .agg(countDistinct(col("eventFingerprint")).as("distinctPayloads"))
      .filter(col("distinctPayloads") > lit(1))
      .select("eventId")

  /** Creates the privacy-safe Silver shape. Callers must supply active deletion marker tokens; marker filtering occurs
    * before this frame reaches a Delta merge.
    */
  def silver(
      valid: DataFrame,
      pseudonymizer: SubjectPseudonymizer,
      activeMarkerTokens: DataFrame
  ): DataFrame = {
    val conflicts = conflictingEventIds(valid)
    val privacySafe = AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(
      AnalyticsSubjectPrivacy.withSubjectToken(valid.join(conflicts, Seq("eventId"), "left_anti"), pseudonymizer),
      activeMarkerTokens
    )
    privacySafe
      .dropDuplicates("eventId")
      .withColumn("applicationId", col("payload.applicationId"))
      .withColumn("jobId", col("payload.jobId"))
      .withColumn("newStatus", col("payload.newStatus"))
      .withColumn("jobSkills", col("payload.job.skills"))
      .withColumn("eventFingerprint", sha2(col("rawValue"), 256))
      // Silver is a derived, 30-day analytics dataset: it does not retain raw envelopes,
      // candidate identifiers, or actor identifiers.
      .select(
        "eventId",
        "eventType",
        "occurredAt",
        "aggregateType",
        "aggregateId",
        "applicationId",
        "jobId",
        "newStatus",
        "jobSkills",
        "subjectToken",
        "eventFingerprint"
      )
  }

  private def requiredEnvelopeFields: Column =
    Seq("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "actorId")
      .map(name => col(name).isNotNull)
      .reduce(_ && _) &&
      Seq("eventId", "aggregateId")
        .map(name => col(name).rlike("\\S"))
        .reduce(_ && _) &&
      length(trim(col("actorId"))) > lit(0) &&
      col("payload").isNotNull
}

object HiringGoldTransforms {
  private val ApplicationCreated = AnalyticsEventType.ApplicationCreated.wire
  private val ApplicationStatusChanged = AnalyticsEventType.ApplicationStatusChanged.wire
  private val JobCreated = AnalyticsEventType.JobCreated.wire
  private val Hired = AnalyticsApplicationStatus.Hired.wire

  private def applicationLifecycle(silver: DataFrame): DataFrame =
    silver.filter(col("eventType").isin(ApplicationCreated, ApplicationStatusChanged))

  /** The CANDIDATE_HIRED event duplicates the Hired status transition and is deliberately excluded. */
  def funnelActivity(silver: DataFrame): DataFrame =
    applicationLifecycle(silver)
      .withColumn("day", date_trunc("day", col("occurredAt")))
      .groupBy("day", "eventType", "newStatus")
      .agg(
        countDistinct(col("applicationId")).as("contributingApplications"),
        countDistinct(col("subjectToken")).as("contributingSubjects")
      )
      .filter(col("contributingSubjects") >= lit(AnalyticsRetention.MinimumContributors))
      .drop("contributingSubjects")

  /** Only JOB_CREATED snapshots contribute; updates and close events never alter this metric. */
  def skillPostingActivity(silver: DataFrame): DataFrame = {
    silver
      .filter(col("eventType") === lit(JobCreated))
      .withColumn("day", date_trunc("day", col("occurredAt")))
      .withColumn("rawSkill", explode(col("jobSkills")))
      .withColumn("skill", lower(trim(col("rawSkill"))))
      .filter(length(col("skill")) > lit(0))
      .dropDuplicates("eventId", "aggregateId", "skill")
      .groupBy("day", "skill")
      .agg(
        countDistinct(col("eventId")).as("postings"),
        countDistinct(col("subjectToken")).as("contributingSubjects")
      )
      .filter(col("contributingSubjects") >= lit(AnalyticsRetention.MinimumContributors))
      .drop("contributingSubjects")
  }

  def suppressSmallGroups(dataset: DataFrame, contributorColumn: String): DataFrame =
    dataset.filter(col(contributorColumn) >= lit(AnalyticsRetention.MinimumContributors))

  /** Wide daily shape consumed by the operational AnalyticsFunnelDay projection. */
  def wideFunnelDay(silver: DataFrame): DataFrame = {
    val statusCells = AnalyticsApplicationStatus.values.toSeq.map { status =>
      status.wire.toLowerCase(java.util.Locale.ROOT) -> (col("newStatus") === lit(status.wire))
    }
    val cells = ("created" -> (col("eventType") === lit(ApplicationCreated))) +: statusCells
    val subjectCounts = cells.map { case (name, matches) =>
      countDistinct(when(matches, col("subjectToken"))).as(s"${name}Subjects")
    }
    val applicationCounts = cells.map { case (name, matches) =>
      countDistinct(when(matches, col("applicationId"))).as(name)
    }
    val counts = subjectCounts ++ applicationCounts
    val subjectColumns = cells.map { case (name, _) => s"${name}Subjects" }

    applicationLifecycle(silver)
      .withColumn("day", date_trunc("day", col("occurredAt")))
      .groupBy("day")
      .agg(counts.head, counts.tail: _*)
      .filter(
        subjectColumns
          .map(name => col(name) === lit(0) || col(name) >= lit(AnalyticsRetention.MinimumContributors))
          .reduce(_ && _)
      )
      .drop(subjectColumns: _*)
  }

  /** One K-anonymous distribution, with hours calculated only from application lifecycle events. */
  def timeToHire(silver: DataFrame): IO[DataFrame] = IO.blocking {
    val lifecycle = applicationLifecycle(silver)
      .groupBy("applicationId", "subjectToken")
      .agg(
        min(when(col("eventType") === lit(ApplicationCreated), col("occurredAt"))).as("createdAt"),
        min(when(col("newStatus") === lit(Hired), col("occurredAt"))).as("hiredAt")
      )
    val eligible = lifecycle
      .filter(col("createdAt").isNotNull && col("hiredAt").isNotNull)
      .withColumn("hours", (unix_timestamp(col("hiredAt")) - unix_timestamp(col("createdAt"))) / lit(3600.0))
      .filter(col("hours") >= lit(0.0))
    val eligibleSubjects = eligible.select("subjectToken").distinct().count()
    val excludedSubjects = lifecycle
      .join(eligible.select("applicationId").distinct(), Seq("applicationId"), "left_anti")
      .select("subjectToken")
      .distinct()
      .count()
    eligible
      .agg(
        percentile_approx(col("hours"), lit(0.5), lit(10000)).as("p50Hours"),
        percentile_approx(col("hours"), lit(0.75), lit(10000)).as("p75Hours"),
        percentile_approx(col("hours"), lit(0.9), lit(10000)).as("p90Hours"),
        percentile_approx(col("hours"), lit(0.95), lit(10000)).as("p95Hours"),
        count(lit(1)).as("eligibleApplications")
      )
      .withColumn("eligibleCount", lit(eligibleSubjects))
      .withColumn("excludedCount", lit(excludedSubjects))
      .filter(
        col("eligibleCount") >= lit(AnalyticsRetention.MinimumContributors) &&
          (col("excludedCount") === lit(0) || col("excludedCount") >= lit(AnalyticsRetention.MinimumContributors))
      )
      .drop("eligibleApplications")
  }
}
