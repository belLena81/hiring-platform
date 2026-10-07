package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsAggregateType
import com.example.hiring.analytics.domain.AnalyticsApplicationStatus
import com.example.hiring.analytics.domain.AnalyticsEventType
import com.example.hiring.analytics.domain.AnalyticsRetention
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  array,
  array_distinct,
  array_sort,
  coalesce,
  encode,
  forall,
  get_json_object,
  isnan,
  is_valid_utf8,
  json_object_keys,
  regexp_replace,
  schema_of_variant,
  sequence,
  size,
  transform,
  try_parse_json,
  try_variant_get,
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
  try_to_timestamp,
  trim,
  unix_timestamp,
  when
}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}

object OperationalEventTransforms {
  private[analytics] val payloadSchema: StructType = StructType(
    Seq(
      StructField(Columns.ApplicationId, StringType, nullable = true),
      StructField(Columns.CandidateId, StringType, nullable = true),
      StructField(Columns.JobId, StringType, nullable = true),
      StructField(Columns.NewStatus, StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("previousStatus", StringType, nullable = true),
      StructField("searchId", StringType, nullable = true),
      StructField("rank", IntegerType, nullable = true),
      StructField(Columns.SearchKind, StringType, nullable = true),
      StructField(
        Columns.Results,
        ArrayType(
          StructType(
            Seq(
              StructField(Columns.ResultId, StringType, nullable = true),
              StructField("rank", IntegerType, nullable = true),
              StructField("score", DoubleType, nullable = true)
            )
          )
        ),
        nullable = true
      ),
      StructField(Columns.ResultId, StringType, nullable = true),
      StructField(
        Columns.Job,
        StructType(
          Seq(
            StructField("jobId", StringType, nullable = true),
            StructField("status", StringType, nullable = true),
            StructField(Columns.Skills, ArrayType(StringType), nullable = true)
          )
        ),
        nullable = true
      )
    )
  )

  val EnvelopeSchema: StructType = StructType(
    Seq(
      StructField(Columns.EventId, StringType, nullable = true),
      StructField(Columns.EventType, StringType, nullable = true),
      StructField(Columns.OccurredAt, StringType, nullable = true),
      StructField(Columns.AggregateType, StringType, nullable = true),
      StructField(Columns.AggregateId, StringType, nullable = true),
      StructField(Columns.ActorId, StringType, nullable = true),
      StructField(Columns.Payload, payloadSchema, nullable = true)
    )
  )

  private val EventTypes = AnalyticsEventType.values.toSeq.map(_.wire)
  private val AggregateTypes = AnalyticsAggregateType.values.toSeq.map(_.wire)

  /** Parses the operational envelope and the analytics payload fields once using Spark's JSON support. The parsed
    * payload exists only before Silver projection; rawValue remains for Bronze, quarantine, and fingerprinting.
    */
  def parseKafkaRecords(records: DataFrame): DataFrame = {
    val parsed = records
      .withColumn(Columns.RawValue, col(Columns.Value).cast(StringType))
      .withColumn(Columns.Envelope, from_json(col(Columns.RawValue), EnvelopeSchema))
    parsed.select(
      col(Columns.Topic),
      col(Columns.Partition),
      col(Columns.Offset),
      col(Columns.Timestamp).as(Columns.KafkaTimestamp),
      col(Columns.RawValue),
      lower(col(Columns.EnvelopeEventId)).as(Columns.EventId),
      col(Columns.EnvelopeEventType),
      try_to_timestamp(col(Columns.EnvelopeOccurredAt)).as(Columns.OccurredAt),
      col(Columns.EnvelopeAggregateType),
      col(Columns.EnvelopeAggregateId),
      lower(col(Columns.EnvelopeActorId)).as(Columns.ActorId),
      col(Columns.EnvelopePayload).as(Columns.Payload)
    )
  }

  def bronze(records: DataFrame): DataFrame =
    records.dropDuplicates(Columns.Topic, Columns.Partition, Columns.Offset)

  def validEvents(parsed: DataFrame): DataFrame =
    parsed.filter(isValidEvent)

  def malformedEvents(parsed: DataFrame): DataFrame =
    parsed.filter(!isValidEvent)

  private[analytics] def isValidEvent: Column =
    coalesce(
      requiredEnvelopeFields &&
        col(Columns.EventType).isin(EventTypes*) &&
        col(Columns.AggregateType).isin(AggregateTypes*) && validPayloadContract,
      lit(false)
    )

  private val InstantPattern =
    "^[+-]?[0-9]{4,10}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2}(:[0-9]{2})?)$"
  private val UuidPattern = "(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  private val Statuses = Seq("Created", "Accepted", "Declined", "Interview", "Hired", "Rejected")
  private val SearchKinds = Seq("jobs", "recommendedJobs", "semanticJobSearch", "candidateMatches")
  private val MaxEnvelopeBytes = 256 * 1024

  private def payload(name: String): Column = col(s"payload.$name")
  private def uuid(value: Column): Column = value.rlike(UuidPattern)
  private def keys(json: Column, names: String*): Column =
    array_sort(json_object_keys(json)) === array_sort(array(names.map(lit)*))
  private def payloadKeys(names: String*): Column = keys(get_json_object(col(Columns.RawValue), "$.payload"), names*)
  private def aggregate(kind: String, id: Column): Column =
    col(Columns.AggregateType) === lit(kind) && uuid(id) && col(Columns.AggregateId) === lower(id)
  private def rank(value: Column): Column = value.between(1, 100)
  private def numericVariant(value: Column): Column =
    schema_of_variant(value).rlike("^(BIGINT|DOUBLE|DECIMAL).*")
  private def numericPayloadField(path: String): Column =
    numericVariant(try_variant_get(try_parse_json(col(Columns.RawValue)), path, "variant"))
  private def applicationIds: Column =
    uuid(payload("applicationId")) && uuid(payload("candidateId")) && uuid(payload("jobId"))

  /** Spark expressions validate the same active wire contract before Bronze admission and privacy attribution. */
  private def validPayloadContract: Column = {
    val kind = col(Columns.EventType)
    val job = payload("job")
    val skills = job.getField("skills")
    val jobFact = kind.isin("JOB_CREATED", "JOB_UPDATED", "JOB_CLOSED") &&
      payloadKeys("job") && keys(
        get_json_object(col(Columns.RawValue), "$.payload.job"),
        "jobId",
        "skills",
        "status"
      ) &&
      aggregate("Job", job.getField("jobId")) && job.getField("status").isin("Open", "Closed") &&
      (kind =!= lit("JOB_CLOSED") || job.getField("status") === lit("Closed")) &&
      skills.isNotNull && size(skills) <= lit(100) && size(array_distinct(skills)) === size(skills) &&
      forall(
        skills,
        skill =>
          skill.isNotNull && length(regexp_replace(skill, "^[\\x00-\\x20]+|[\\x00-\\x20]+$", "")) > lit(0) && length(
            encode(skill, "UTF-16BE")
          ) <= lit(512)
      ) &&
      forall(
        try_variant_get(try_parse_json(col(Columns.RawValue)), "$.payload.job.skills", "array<variant>"),
        skill => schema_of_variant(skill) === lit("STRING")
      )
    val created = kind === lit("APPLICATION_CREATED") &&
      payloadKeys("applicationId", "candidateId", "jobId", "status") && applicationIds &&
      aggregate("Application", payload("applicationId")) && payload("status") === lit("Created")
    val changed = kind === lit("APPLICATION_STATUS_CHANGED") &&
      payloadKeys("applicationId", "candidateId", "jobId", "previousStatus", "newStatus") && applicationIds &&
      aggregate("Application", payload("applicationId")) && payload("newStatus").isin(Statuses*) &&
      ((payload("previousStatus").isNull && get_json_object(
        col(Columns.RawValue),
        "$.payload.previousStatus"
      ).isNull) || payload("previousStatus").isin(Statuses*))
    val hired = kind === lit("CANDIDATE_HIRED") &&
      payloadKeys("applicationId", "candidateId", "jobId", "status") && applicationIds &&
      aggregate("Application", payload("applicationId")) && payload("status") === lit("Hired")
    val results = payload("results")
    val resultIds = transform(results, result => result.getField("resultId"))
    val resultRanks = transform(results, result => result.getField("rank"))
    val rawResults = from_json(get_json_object(col(Columns.RawValue), "$.payload.results"), ArrayType(StringType))
    val searched = kind === lit("SEARCH_PERFORMED") && payloadKeys("searchId", "searchKind", "results") &&
      aggregate("Search", payload("searchId")) && payload("searchKind").isin(SearchKinds*) &&
      results.isNotNull && size(results) <= lit(100) && size(array_distinct(resultIds)) === size(results) &&
      (size(results) === lit(0) || resultRanks === sequence(lit(1), size(results))) &&
      forall(
        results,
        result =>
          uuid(result.getField("resultId")) && rank(result.getField("rank")) &&
            result.getField("score").isNotNull && !isnan(result.getField("score")) &&
            result.getField("score") =!= lit(Double.PositiveInfinity) && result.getField("score") =!= lit(
              Double.NegativeInfinity
            )
      ) &&
      forall(rawResults, result => keys(result, "resultId", "rank", "score")) &&
      forall(
        try_variant_get(try_parse_json(col(Columns.RawValue)), "$.payload.results", "array<variant>"),
        result =>
          numericVariant(try_variant_get(result, "$.rank", "variant")) && numericVariant(
            try_variant_get(result, "$.score", "variant")
          )
      )
    val viewed = kind === lit("JOB_VIEWED") && payloadKeys("searchId", "resultId", "searchKind", "rank") &&
      aggregate("Search", payload("resultId")) && payload("searchKind").isNull &&
      get_json_object(col(Columns.RawValue), "$.payload.searchKind").isNull &&
      (!payload("searchId").isNull || get_json_object(col(Columns.RawValue), "$.payload.searchId").isNull) &&
      (!payload("rank").isNull || get_json_object(col(Columns.RawValue), "$.payload.rank").isNull) &&
      payload("searchId").isNull === payload("rank").isNull &&
      (payload("searchId").isNull || uuid(payload("searchId"))) && (payload("rank").isNull || (rank(
        payload("rank")
      ) && numericPayloadField("$.payload.rank")))
    val clicked = kind === lit("SEARCH_RESULT_CLICKED") && payloadKeys("searchId", "resultId", "searchKind", "rank") &&
      aggregate("Search", payload("searchId")) && uuid(payload("resultId")) && payload("searchKind").isin(
        SearchKinds*
      ) && rank(payload("rank")) && numericPayloadField("$.payload.rank")
    get_json_object(col(Columns.RawValue), "$.occurredAt").rlike(InstantPattern) &&
    is_valid_utf8(col(Columns.RawValue)) && uuid(col(Columns.EventId)) && uuid(col(Columns.ActorId)) &&
    length(encode(col(Columns.RawValue), "UTF-8")) <= lit(MaxEnvelopeBytes) &&
    keys(
      col(Columns.RawValue),
      "eventId",
      "eventType",
      "occurredAt",
      "aggregateType",
      "aggregateId",
      "actorId",
      "payload"
    ) &&
    (jobFact || created || changed || hired || searched || viewed || clicked)
  }

  /** Event IDs are idempotency keys. Same bytes are duplicates; different bytes are conflicts. */
  def conflictingEventIds(valid: DataFrame): DataFrame =
    valid
      .withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))
      .groupBy(Columns.EventId)
      .agg(countDistinct(col(Columns.EventFingerprint)).as(Columns.DistinctPayloads))
      .filter(col(Columns.DistinctPayloads) > lit(1))
      .select(Columns.EventId)

  /** Creates the privacy-safe Silver shape. Callers must supply active deletion marker tokens; marker filtering occurs
    * before this frame reaches a Delta merge.
    */
  def silver(
      valid: DataFrame,
      pseudonymizer: SubjectPseudonymizer,
      activeMarkerTokens: DataFrame
  ): Either[AnalyticsError, DataFrame] = {
    val conflicts = conflictingEventIds(valid)
    val privacySafe = AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(
      AnalyticsSubjectPrivacy.withSubjectToken(valid.join(conflicts, Seq(Columns.EventId), "left_anti"), pseudonymizer),
      activeMarkerTokens
    )
    privacySafe.flatMap { safe =>
      val selected = safe
        .dropDuplicates(Columns.EventId)
        .withColumn(Columns.ApplicationId, lower(col(Columns.PayloadApplicationId)))
        .withColumn(Columns.JobId, lower(col(Columns.PayloadJobId)))
        .withColumn(Columns.NewStatus, col(Columns.PayloadNewStatus))
        .withColumn(Columns.JobSkills, col(Columns.PayloadJobSkills))
        .withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))
        // Silver is a derived, 30-day analytics dataset: it does not retain raw envelopes,
        // candidate identifiers, or actor identifiers.
        .select(
          AnalyticsTableSchemas.silver.head._1,
          AnalyticsTableSchemas.silver.tail.map(_._1)*
        )
      validateSilverSchema(selected)
    }
  }

  /** Validates the persisted Silver column order and data types before returning the original DataFrame shape.
    */
  private[analytics] def validateSilverSchema(silver: DataFrame): Either[AnalyticsError, DataFrame] = {
    val validShape = AnalyticsTableSchemas.matches(silver.schema, AnalyticsTableSchemas.silver) ||
      AnalyticsTableSchemas.matches(silver.schema, AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry)
    Either.cond(validShape, silver, AnalyticsError.InvalidSilverSchema)
  }

  private def requiredEnvelopeFields: Column =
    Seq(
      Columns.EventId,
      Columns.EventType,
      Columns.OccurredAt,
      Columns.AggregateType,
      Columns.AggregateId,
      Columns.ActorId
    )
      .map(name => col(name).isNotNull)
      .reduce(_ && _) &&
      Seq(Columns.EventId, Columns.AggregateId)
        .map(name => col(name).rlike("\\S"))
        .reduce(_ && _) &&
      length(trim(col(Columns.ActorId))) > lit(0) &&
      col(Columns.Payload).isNotNull
}

object HiringGoldTransforms {
  private val ApplicationCreated = AnalyticsEventType.ApplicationCreated.wire
  private val ApplicationStatusChanged = AnalyticsEventType.ApplicationStatusChanged.wire
  private val JobCreated = AnalyticsEventType.JobCreated.wire
  private val Hired = AnalyticsApplicationStatus.Hired.wire

  private def applicationLifecycle(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    OperationalEventTransforms
      .validateSilverSchema(silver)
      .map(_.filter(col(Columns.EventType).isin(ApplicationCreated, ApplicationStatusChanged)))

  /** The CANDIDATE_HIRED event duplicates the Hired status transition and is deliberately excluded. */
  def funnelActivity(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    applicationLifecycle(silver).map { validated =>
      validated
        .withColumn(Columns.Day, date_trunc(Columns.Day, col(Columns.OccurredAt)))
        .groupBy(Columns.Day, Columns.EventType, Columns.NewStatus)
        .agg(
          countDistinct(col(Columns.ApplicationId)).as(Columns.ContributingApplications),
          countDistinct(col(Columns.SubjectToken)).as(Columns.ContributingSubjects)
        )
        .filter(col(Columns.ContributingSubjects) >= lit(AnalyticsRetention.MinimumContributors))
        .drop(Columns.ContributingSubjects)
    }

  /** Only JOB_CREATED snapshots contribute; updates and close events never alter this metric. */
  def skillPostingActivity(silver: DataFrame): Either[AnalyticsError, DataFrame] =
    OperationalEventTransforms.validateSilverSchema(silver).map { validated =>
      validated
        .filter(col(Columns.EventType) === lit(JobCreated))
        .withColumn(Columns.Day, date_trunc(Columns.Day, col(Columns.OccurredAt)))
        .withColumn(Columns.RawSkill, explode(col(Columns.JobSkills)))
        .withColumn(Columns.Skill, lower(trim(col(Columns.RawSkill))))
        .filter(length(col(Columns.Skill)) > lit(0))
        .dropDuplicates(Columns.EventId, Columns.AggregateId, Columns.Skill)
        .groupBy(Columns.Day, Columns.Skill)
        .agg(
          countDistinct(col(Columns.EventId)).as(Columns.Postings),
          countDistinct(col(Columns.SubjectToken)).as(Columns.ContributingSubjects)
        )
        .filter(col(Columns.ContributingSubjects) >= lit(AnalyticsRetention.MinimumContributors))
        .drop(Columns.ContributingSubjects)
    }

  def suppressSmallGroups(dataset: DataFrame, contributorColumn: String): DataFrame =
    dataset.filter(col(contributorColumn) >= lit(AnalyticsRetention.MinimumContributors))

  /** Wide daily shape consumed by the operational AnalyticsFunnelDay projection. */
  def wideFunnelDay(silver: DataFrame): Either[AnalyticsError, DataFrame] = {
    val statusCells = AnalyticsApplicationStatus.values.toSeq.map { status =>
      status.wire.toLowerCase(java.util.Locale.ROOT) -> (col(Columns.NewStatus) === lit(status.wire))
    }
    val cells = (Columns.Created -> (col(Columns.EventType) === lit(ApplicationCreated))) +: statusCells
    val subjectCounts = cells.map { case (name, matches) =>
      countDistinct(when(matches, col(Columns.SubjectToken))).as(s"${name}Subjects")
    }
    val applicationCounts = cells.map { case (name, matches) =>
      countDistinct(when(matches, col(Columns.ApplicationId))).as(name)
    }
    val counts = subjectCounts ++ applicationCounts
    val subjectColumns = cells.map { case (name, _) => s"${name}Subjects" }

    applicationLifecycle(silver).map { validated =>
      validated
        .withColumn(Columns.Day, date_trunc(Columns.Day, col(Columns.OccurredAt)))
        .groupBy(Columns.Day)
        .agg(counts.head, counts.tail*)
        .filter(
          subjectColumns
            .map(name => col(name) === lit(0) || col(name) >= lit(AnalyticsRetention.MinimumContributors))
            .reduce(_ && _)
        )
        .drop(subjectColumns*)
    }
  }

  /** One K-anonymous distribution, with hours calculated only from application lifecycle events. */
  def timeToHireAction[F[_]: Async](
      silver: DataFrame,
      sparkExecution: SparkExecution[F]
  ): F[DataFrame] = sparkExecution.either(applicationLifecycle(silver)).flatMap { valid =>
    sparkExecution {
      val lifecycle = valid
        .groupBy(Columns.ApplicationId, Columns.SubjectToken)
        .agg(
          min(when(col(Columns.EventType) === lit(ApplicationCreated), col(Columns.OccurredAt))).as(Columns.CreatedAt),
          min(when(col(Columns.NewStatus) === lit(Hired), col(Columns.OccurredAt))).as(Columns.HiredAt)
        )
      val eligible = lifecycle
        .filter(col(Columns.CreatedAt).isNotNull && col(Columns.HiredAt).isNotNull)
        .withColumn(
          Columns.Hours,
          (unix_timestamp(col(Columns.HiredAt)) - unix_timestamp(col(Columns.CreatedAt))) / lit(3600.0)
        )
        .filter(col(Columns.Hours) >= lit(0.0))
      val eligibleSubjects = eligible.select(Columns.SubjectToken).distinct().count()
      val excludedSubjects = lifecycle
        .join(eligible.select(Columns.ApplicationId).distinct(), Seq(Columns.ApplicationId), "left_anti")
        .select(Columns.SubjectToken)
        .distinct()
        .count()
      val result = eligible
        .agg(
          percentile_approx(col(Columns.Hours), lit(0.5), lit(10000)).as(Columns.P50Hours),
          percentile_approx(col(Columns.Hours), lit(0.75), lit(10000)).as(Columns.P75Hours),
          percentile_approx(col(Columns.Hours), lit(0.9), lit(10000)).as(Columns.P90Hours),
          percentile_approx(col(Columns.Hours), lit(0.95), lit(10000)).as(Columns.P95Hours),
          count(lit(1)).as(Columns.EligibleApplications)
        )
        .withColumn(Columns.EligibleCount, lit(eligibleSubjects))
        .withColumn(Columns.ExcludedCount, lit(excludedSubjects))
        .filter(
          col(Columns.EligibleCount) >= lit(AnalyticsRetention.MinimumContributors) &&
            (col(Columns.ExcludedCount) === lit(0) || col(Columns.ExcludedCount) >= lit(
              AnalyticsRetention.MinimumContributors
            ))
        )
        .drop(Columns.EligibleApplications)
      result
    }
  }
}
