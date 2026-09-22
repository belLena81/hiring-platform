package com.example.hiring.analytics

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, countDistinct, date_trunc, explode, from_json, get_json_object, length, lit, lower, sha2, to_timestamp, trim}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object OperationalEventTransforms {
  val EnvelopeSchema: StructType = StructType(
    Seq(
      StructField("eventId", StringType, nullable = true),
      StructField("eventType", StringType, nullable = true),
      StructField("occurredAt", StringType, nullable = true),
      StructField("aggregateType", StringType, nullable = true),
      StructField("aggregateId", StringType, nullable = true),
      StructField("actorId", StringType, nullable = true)
    )
  )

  private val EventTypes = Seq(
    "JOB_CREATED", "JOB_UPDATED", "JOB_CLOSED", "JOB_VIEWED", "SEARCH_PERFORMED",
    "SEARCH_RESULT_CLICKED", "APPLICATION_CREATED", "APPLICATION_STATUS_CHANGED", "CANDIDATE_HIRED"
  )
  private val AggregateTypes = Seq("Job", "Application", "Search")

  /**
    * Parses the operational envelope using Spark's JSON support. Payload details remain in rawValue
    * so the lakehouse never carries a second application-level JSON decoder.
    */
  def parseKafkaRecords(records: DataFrame): DataFrame = {
    val parsed = records
      .withColumn("rawValue", col("value").cast(StringType))
      .withColumn("envelope", from_json(col("rawValue"), EnvelopeSchema))
    parsed.select(
      col("topic"), col("partition"), col("offset"), col("timestamp").as("kafkaTimestamp"),
      col("rawValue"),
      col("envelope.eventId"), col("envelope.eventType"),
      to_timestamp(col("envelope.occurredAt")).as("occurredAt"),
      col("envelope.aggregateType"), col("envelope.aggregateId"), col("envelope.actorId")
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
    parsed.filter(!(requiredEnvelopeFields &&
      col("eventType").isin(EventTypes: _*) &&
      col("aggregateType").isin(AggregateTypes: _*)))

  /** Event IDs are idempotency keys. Same bytes are duplicates; different bytes are conflicts. */
  def conflictingEventIds(valid: DataFrame): DataFrame =
    valid.withColumn("eventFingerprint", sha2(col("rawValue"), 256))
      .groupBy("eventId")
      .agg(countDistinct(col("eventFingerprint")).as("distinctPayloads"))
      .filter(col("distinctPayloads") > lit(1))
      .select("eventId")

  /**
    * Creates the privacy-safe Silver shape. Callers must supply active deletion marker tokens;
    * marker filtering occurs before this frame reaches a Delta merge.
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
      .withColumn("applicationId", get_json_object(col("rawValue"), "$.payload.applicationId"))
      .withColumn("jobId", get_json_object(col("rawValue"), "$.payload.jobId"))
      .withColumn("newStatus", get_json_object(col("rawValue"), "$.payload.newStatus"))
      .withColumn("jobSkills", from_json(get_json_object(col("rawValue"), "$.payload.job.skills"), ArrayType(StringType)))
      .withColumn("eventFingerprint", sha2(col("rawValue"), 256))
      // Silver is a derived, 30-day analytics dataset: it does not retain raw envelopes,
      // candidate identifiers, or actor identifiers.
      .select("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "applicationId", "jobId", "newStatus", "jobSkills", "subjectToken", "eventFingerprint")
  }

  private def requiredEnvelopeFields: Column =
    Seq("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "actorId")
      .map(name => col(name).isNotNull)
      .reduce(_ && _) &&
      length(trim(col("actorId"))) > lit(0) &&
      get_json_object(col("rawValue"), "$.payload").isNotNull
}

object HiringGoldTransforms {
  /** The CANDIDATE_HIRED event duplicates the Hired status transition and is deliberately excluded. */
  def funnelActivity(silver: DataFrame): DataFrame =
    silver.filter(col("eventType").isin("APPLICATION_CREATED", "APPLICATION_STATUS_CHANGED"))
      .withColumn("day", date_trunc("day", col("occurredAt")))
      .groupBy("day", "eventType", "newStatus")
      .agg(countDistinct(col("applicationId")).as("contributingApplications"))
      .filter(col("contributingApplications") >= lit(AnalyticsRetention.MinimumContributors))

  /** Only JOB_CREATED snapshots contribute; updates and close events never alter this metric. */
  def skillPostingActivity(silver: DataFrame): DataFrame = {
    silver.filter(col("eventType") === lit("JOB_CREATED"))
      .withColumn("day", date_trunc("day", col("occurredAt")))
      .withColumn("rawSkill", explode(col("jobSkills")))
      .withColumn("skill", lower(trim(col("rawSkill"))))
      .filter(length(col("skill")) > lit(0))
      .dropDuplicates("eventId", "aggregateId", "skill")
      .groupBy("day", "skill")
      .agg(countDistinct(col("eventId")).as("contributingPostingEvents"))
      .filter(col("contributingPostingEvents") >= lit(AnalyticsRetention.MinimumContributors))
  }

  def suppressSmallGroups(dataset: DataFrame, contributorColumn: String): DataFrame =
    dataset.filter(col(contributorColumn) >= lit(AnalyticsRetention.MinimumContributors))

  /** Wide daily shape consumed by the operational AnalyticsFunnelDay projection. */
  def wideFunnelDay(silver: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{countDistinct, when}
    silver.filter(col("eventType").isin("APPLICATION_CREATED", "APPLICATION_STATUS_CHANGED"))
      .withColumn("day", date_trunc("day", col("occurredAt")))
      .groupBy("day")
      .agg(
        countDistinct(col("applicationId")).as("contributingApplications"),
        countDistinct(when(col("eventType") === "APPLICATION_CREATED", col("applicationId"))).as("created"),
        countDistinct(when(col("newStatus") === "Accepted", col("applicationId"))).as("accepted"),
        countDistinct(when(col("newStatus") === "Declined", col("applicationId"))).as("declined"),
        countDistinct(when(col("newStatus") === "Interview", col("applicationId"))).as("interview"),
        countDistinct(when(col("newStatus") === "Hired", col("applicationId"))).as("hired"),
        countDistinct(when(col("newStatus") === "Rejected", col("applicationId"))).as("rejected")
      ).filter(col("contributingApplications") >= lit(AnalyticsRetention.MinimumContributors))
        .drop("contributingApplications")
  }

  /** One K-anonymous distribution, with hours calculated only from application lifecycle events. */
  def timeToHire(silver: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{count, min, percentile_approx, unix_timestamp, when}
    val lifecycle = silver.filter(col("eventType").isin("APPLICATION_CREATED", "APPLICATION_STATUS_CHANGED"))
      .groupBy("applicationId")
      .agg(
        min(when(col("eventType") === "APPLICATION_CREATED", col("occurredAt"))).as("createdAt"),
        min(when(col("newStatus") === "Hired", col("occurredAt"))).as("hiredAt")
      )
    val eligible = lifecycle.filter(col("createdAt").isNotNull && col("hiredAt").isNotNull)
      .withColumn("hours", (unix_timestamp(col("hiredAt")) - unix_timestamp(col("createdAt"))) / lit(3600.0))
      .filter(col("hours") >= lit(0.0))
    val excluded = lifecycle.count() - eligible.count()
    eligible.agg(
      percentile_approx(col("hours"), lit(0.5), lit(10000)).as("p50Hours"),
      percentile_approx(col("hours"), lit(0.75), lit(10000)).as("p75Hours"),
      percentile_approx(col("hours"), lit(0.9), lit(10000)).as("p90Hours"),
      percentile_approx(col("hours"), lit(0.95), lit(10000)).as("p95Hours"),
      count(lit(1)).as("eligibleCount")
    ).withColumn("excludedCount", lit(excluded))
      .filter(col("eligibleCount") >= lit(AnalyticsRetention.MinimumContributors))
  }
}
