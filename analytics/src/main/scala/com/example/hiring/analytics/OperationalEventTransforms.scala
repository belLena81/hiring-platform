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

  def silver(valid: DataFrame): DataFrame = {
    val conflicts = conflictingEventIds(valid)
    valid.join(conflicts, Seq("eventId"), "left_anti")
      .dropDuplicates("eventId")
      .withColumn("applicationId", get_json_object(col("rawValue"), "$.payload.applicationId"))
      .withColumn("candidateId", get_json_object(col("rawValue"), "$.payload.candidateId"))
      .withColumn("jobId", get_json_object(col("rawValue"), "$.payload.jobId"))
      .withColumn("newStatus", get_json_object(col("rawValue"), "$.payload.newStatus"))
  }

  private def requiredEnvelopeFields: Column =
    Seq("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "actorId")
      .map(name => col(name).isNotNull)
      .reduce(_ && _) && get_json_object(col("rawValue"), "$.payload").isNotNull
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
    val skills = from_json(get_json_object(col("rawValue"), "$.payload.job.skills"), ArrayType(StringType))
    silver.filter(col("eventType") === lit("JOB_CREATED"))
      .withColumn("day", date_trunc("day", col("occurredAt")))
      .withColumn("rawSkill", explode(skills))
      .withColumn("skill", lower(trim(col("rawSkill"))))
      .filter(length(col("skill")) > lit(0))
      .dropDuplicates("eventId", "aggregateId", "skill")
      .groupBy("day", "skill")
      .agg(countDistinct(col("eventId")).as("contributingPostingEvents"))
      .filter(col("contributingPostingEvents") >= lit(AnalyticsRetention.MinimumContributors))
  }

  def suppressSmallGroups(dataset: DataFrame, contributorColumn: String): DataFrame =
    dataset.filter(col(contributorColumn) >= lit(AnalyticsRetention.MinimumContributors))
}
