package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsAggregateType
import com.example.hiring.analytics.domain.AnalyticsApplicationStatus
import com.example.hiring.analytics.domain.AnalyticsEventType
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError

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
  countDistinct,
  from_json,
  length,
  lit,
  lower,
  sha2,
  try_to_timestamp,
  trim
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

  /** Projects the tokenized in-memory frame to the persisted Bronze shape (without expiry columns). Raw envelope text,
    * actor and candidate identifiers and search results stop here: Bronze keeps the envelope fingerprint, the
    * normalized fields Silver uses, and subject tokens. The fingerprint equals the one Silver and late facts derive.
    */
  def bronze(tokenized: DataFrame): DataFrame =
    withFingerprint(tokenized)
      .dropDuplicates(Columns.Topic, Columns.Partition, Columns.Offset)
      .withColumn(Columns.ApplicationId, lower(col(Columns.PayloadApplicationId)))
      .withColumn(Columns.JobId, lower(col(Columns.PayloadJobId)))
      .withColumn(Columns.NewStatus, col(Columns.PayloadNewStatus))
      .withColumn(Columns.JobSkills, col(Columns.PayloadJobSkills))
      .select(
        AnalyticsTableSchemas.bronze.map(_._1).filterNot(AnalyticsTableSchemas.expiry.map(_._1).contains).map(col)*
      )

  /** The persisted parsed frame of one batch: [[parseKafkaRecords]] output plus `isValidEvent`, `eventFingerprint`,
    * `subjectToken` and `subjectTokens`, each computed once. Every consumer reads these columns instead of recomputing
    * them. A column that is already present is kept, so the transform is idempotent.
    */
  def analyzed(parsed: DataFrame, pseudonymizer: SubjectPseudonymizer): DataFrame =
    AnalyticsSubjectPrivacy.ensureSubjectTokens(withFingerprint(withValidity(parsed)), pseudonymizer)

  def validEvents(parsed: DataFrame): DataFrame =
    if (parsed.columns.contains(Columns.IsValidEvent)) parsed.filter(col(Columns.IsValidEvent))
    else withValidity(parsed).filter(col(Columns.IsValidEvent)).drop(Columns.IsValidEvent)

  def malformedEvents(parsed: DataFrame): DataFrame =
    if (parsed.columns.contains(Columns.IsValidEvent)) parsed.filter(!col(Columns.IsValidEvent))
    else withValidity(parsed).filter(!col(Columns.IsValidEvent)).drop(Columns.IsValidEvent)

  /** The SHA-256 of the raw envelope. It is computed once into `eventFingerprint` and reused by every consumer. */
  private[analytics] def withFingerprint(frame: DataFrame): DataFrame =
    if (frame.columns.contains(Columns.EventFingerprint)) frame
    else frame.withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))

  // Parse-once intermediates. The raw envelope is parsed into a VARIANT once, and each raw-JSON extraction or exact
  // key-set check that has no VARIANT equivalent runs once; every check below reads these columns. They are dropped
  // again so the persisted frame carries only `isValidEvent`.
  private val VariantColumn = "_hiringVariant"
  private val EnvelopeKeysColumn = "_hiringEnvelopeKeys"
  private val OccurredAtTextColumn = "_hiringOccurredAtText"
  private val PayloadJsonColumn = "_hiringPayloadJson"
  private val PayloadKeysColumn = "_hiringPayloadKeys"
  private val JobJsonColumn = "_hiringJobJson"
  private val JobKeysColumn = "_hiringJobKeys"
  private val ResultsJsonColumn = "_hiringResultsJson"
  private val PreviousStatusAbsentColumn = "_hiringPreviousStatusAbsent"
  private val SearchKindAbsentColumn = "_hiringSearchKindAbsent"
  private val SearchIdAbsentColumn = "_hiringSearchIdAbsent"
  private val RankAbsentColumn = "_hiringRankAbsent"
  private val ValidityHelpers = Vector(
    VariantColumn,
    EnvelopeKeysColumn,
    OccurredAtTextColumn,
    PayloadJsonColumn,
    PayloadKeysColumn,
    JobJsonColumn,
    JobKeysColumn,
    ResultsJsonColumn,
    PreviousStatusAbsentColumn,
    SearchKindAbsentColumn,
    SearchIdAbsentColumn,
    RankAbsentColumn
  )

  private def sortedKeys(json: Column): Column = array_sort(json_object_keys(json))

  /** Adds `isValidEvent` unless present. Contract: an existing `isValidEvent` column is trusted, so it must come from
    * this pipeline's own `analyzed` frame, never from external input.
    */
  private[analytics] def withValidity(frame: DataFrame): DataFrame =
    if (frame.columns.contains(Columns.IsValidEvent)) frame
    else {
      val raw = col(Columns.RawValue)
      frame
        .withColumn(VariantColumn, try_parse_json(raw))
        .withColumn(EnvelopeKeysColumn, sortedKeys(raw))
        .withColumn(OccurredAtTextColumn, get_json_object(raw, "$.occurredAt"))
        .withColumn(PayloadJsonColumn, get_json_object(raw, "$.payload"))
        .withColumn(PayloadKeysColumn, sortedKeys(col(PayloadJsonColumn)))
        .withColumn(JobJsonColumn, get_json_object(col(PayloadJsonColumn), "$.job"))
        .withColumn(JobKeysColumn, sortedKeys(col(JobJsonColumn)))
        .withColumn(ResultsJsonColumn, get_json_object(col(PayloadJsonColumn), "$.results"))
        .withColumn(PreviousStatusAbsentColumn, get_json_object(col(PayloadJsonColumn), "$.previousStatus").isNull)
        .withColumn(SearchKindAbsentColumn, get_json_object(col(PayloadJsonColumn), "$.searchKind").isNull)
        .withColumn(SearchIdAbsentColumn, get_json_object(col(PayloadJsonColumn), "$.searchId").isNull)
        .withColumn(RankAbsentColumn, get_json_object(col(PayloadJsonColumn), "$.rank").isNull)
        .withColumn(Columns.IsValidEvent, validityExpression)
        .drop(ValidityHelpers*)
    }

  private def validityExpression: Column =
    coalesce(
      requiredEnvelopeFields &&
        col(Columns.EventType).isin(EventTypes*) &&
        col(Columns.AggregateType).isin(AggregateTypes*) && validPayloadContract,
      lit(false)
    )

  private val InstantPattern =
    "^[+-]?[0-9]{4,10}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2}(:[0-9]{2})?)$"
  private val UuidPattern = "(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  private val Statuses = "Created" +: AnalyticsApplicationStatus.values.toSeq.map(_.wire)
  private val SearchKinds = Seq("jobs", "recommendedJobs", "semanticJobSearch", "candidateMatches")
  private val MaxEnvelopeBytes = 256 * 1024

  private def payload(name: String): Column = col(s"payload.$name")
  private def uuid(value: Column): Column = value.rlike(UuidPattern)
  private def expectedKeys(names: String*): Column = array_sort(array(names.map(lit)*))
  private def keys(json: Column, names: String*): Column = sortedKeys(json) === expectedKeys(names*)
  private def payloadKeys(names: String*): Column = col(PayloadKeysColumn) === expectedKeys(names*)
  private def aggregate(kind: String, id: Column): Column =
    col(Columns.AggregateType) === lit(kind) && uuid(id) && col(Columns.AggregateId) === lower(id)
  private def rank(value: Column): Column = value.between(1, 100)
  private def numericVariant(value: Column): Column =
    schema_of_variant(value).rlike("^(BIGINT|DOUBLE|DECIMAL).*")
  private def numericPayloadField(path: String): Column =
    numericVariant(try_variant_get(col(VariantColumn), path, "variant"))
  private def applicationIds: Column =
    uuid(payload("applicationId")) && uuid(payload("candidateId")) && uuid(payload("jobId"))

  /** Spark expressions validate the same active wire contract before Bronze admission and privacy attribution. */
  private def validPayloadContract: Column = {
    val kind = col(Columns.EventType)
    val job = payload("job")
    val skills = job.getField("skills")
    val jobFact = kind.isin(
      AnalyticsEventType.JobCreated.wire,
      AnalyticsEventType.JobUpdated.wire,
      AnalyticsEventType.JobClosed.wire
    ) &&
      payloadKeys("job") && (col(JobKeysColumn) === expectedKeys("jobId", "skills", "status")) &&
      aggregate("Job", job.getField("jobId")) && job.getField("status").isin("Open", "Closed") &&
      (kind =!= lit(AnalyticsEventType.JobClosed.wire) || job.getField("status") === lit("Closed")) &&
      skills.isNotNull && size(skills) <= lit(100) && size(array_distinct(skills)) === size(skills) &&
      forall(
        skills,
        skill =>
          skill.isNotNull && length(regexp_replace(skill, "^[\\x00-\\x20]+|[\\x00-\\x20]+$", "")) > lit(0) && length(
            encode(skill, "UTF-16BE")
          ) <= lit(512)
      ) &&
      forall(
        try_variant_get(col(VariantColumn), "$.payload.job.skills", "array<variant>"),
        skill => schema_of_variant(skill) === lit("STRING")
      )
    val created = kind === lit(AnalyticsEventType.ApplicationCreated.wire) &&
      payloadKeys("applicationId", "candidateId", "jobId", "status") && applicationIds &&
      aggregate("Application", payload("applicationId")) && payload("status") === lit("Created")
    val changed = kind === lit(AnalyticsEventType.ApplicationStatusChanged.wire) &&
      payloadKeys("applicationId", "candidateId", "jobId", "previousStatus", "newStatus") && applicationIds &&
      aggregate("Application", payload("applicationId")) && payload("newStatus").isin(Statuses*) &&
      ((payload("previousStatus").isNull && col(PreviousStatusAbsentColumn)) || payload(
        "previousStatus"
      ).isin(Statuses*))
    val hired = kind === lit(AnalyticsEventType.CandidateHired.wire) &&
      payloadKeys("applicationId", "candidateId", "jobId", "status") && applicationIds &&
      aggregate("Application", payload("applicationId")) && payload("status") === lit("Hired")
    val results = payload("results")
    val resultIds = transform(results, result => result.getField("resultId"))
    val resultRanks = transform(results, result => result.getField("rank"))
    val rawResults = from_json(col(ResultsJsonColumn), ArrayType(StringType))
    val searched =
      kind === lit(AnalyticsEventType.SearchPerformed.wire) && payloadKeys("searchId", "searchKind", "results") &&
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
          try_variant_get(col(VariantColumn), "$.payload.results", "array<variant>"),
          result =>
            numericVariant(try_variant_get(result, "$.rank", "variant")) && numericVariant(
              try_variant_get(result, "$.score", "variant")
            )
        )
    val viewed =
      kind === lit(AnalyticsEventType.JobViewed.wire) && payloadKeys("searchId", "resultId", "searchKind", "rank") &&
        aggregate("Search", payload("resultId")) && payload("searchKind").isNull &&
        col(SearchKindAbsentColumn) &&
        (!payload("searchId").isNull || col(SearchIdAbsentColumn)) &&
        (!payload("rank").isNull || col(RankAbsentColumn)) &&
        payload("searchId").isNull === payload("rank").isNull &&
        (payload("searchId").isNull || uuid(payload("searchId"))) && (payload("rank").isNull || (rank(
          payload("rank")
        ) && numericPayloadField("$.payload.rank")))
    val clicked = kind === lit(AnalyticsEventType.SearchResultClicked.wire) && payloadKeys(
      "searchId",
      "resultId",
      "searchKind",
      "rank"
    ) &&
      aggregate("Search", payload("searchId")) && uuid(payload("resultId")) && payload("searchKind").isin(
        SearchKinds*
      ) && rank(payload("rank")) && numericPayloadField("$.payload.rank")
    val recordingFailed = kind === lit(AnalyticsEventType.SearchRecordingFailed.wire) &&
      payloadKeys("searchId", "searchKind", "failure") && aggregate("Search", payload("searchId")) &&
      payload("searchKind").isin(
        SearchKinds*
      ) && // Read from the raw payload so the retained payload schema is unchanged.
      col(PayloadJsonColumn).rlike(
        "\"failure\"\\s*:\\s*\"[A-Za-z0-9_.$]{1,100}\"[,}]"
      )
    col(OccurredAtTextColumn).rlike(InstantPattern) &&
    is_valid_utf8(col(Columns.RawValue)) && uuid(col(Columns.EventId)) && uuid(col(Columns.ActorId)) &&
    length(encode(col(Columns.RawValue), "UTF-8")) <= lit(MaxEnvelopeBytes) &&
    (col(EnvelopeKeysColumn) === expectedKeys(
      Columns.EventId,
      "eventType",
      "occurredAt",
      "aggregateType",
      "aggregateId",
      "actorId",
      "payload"
    )) &&
    (jobFact || created || changed || hired || searched || viewed || clicked || recordingFailed)
  }

  /** Event IDs are idempotency keys. Same bytes are duplicates; different bytes are conflicts. */
  def conflictingEventIds(valid: DataFrame): DataFrame =
    withFingerprint(valid)
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
    // Frames from the parsed pipeline already carry their tokens; only a frame without them is tokenized here.
    val privacySafe = AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(
      AnalyticsSubjectPrivacy
        .ensureSubjectTokens(valid.join(conflicts, Seq(Columns.EventId), "left_anti"), pseudonymizer),
      activeMarkerTokens
    )
    privacySafe.flatMap { safe =>
      val selected = withFingerprint(safe)
        .dropDuplicates(Columns.EventId)
        .withColumn(Columns.ApplicationId, lower(col(Columns.PayloadApplicationId)))
        .withColumn(Columns.JobId, lower(col(Columns.PayloadJobId)))
        .withColumn(Columns.NewStatus, col(Columns.PayloadNewStatus))
        .withColumn(Columns.JobSkills, col(Columns.PayloadJobSkills))
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

  /** Event IDs whose raw payload fingerprint differs from a previously stored fingerprint for the same event ID. */
  def historicalFingerprintConflicts(incoming: DataFrame, storedFingerprints: DataFrame): DataFrame =
    withFingerprint(incoming)
      .select(col(Columns.EventId), col(Columns.EventFingerprint).as("incomingFingerprint"))
      .join(
        storedFingerprints.withColumnRenamed(Columns.EventFingerprint, "storedFingerprint"),
        Seq(Columns.EventId),
        "inner"
      )
      .filter(col("incomingFingerprint") =!= col("storedFingerprint"))
      .select(Columns.EventId)
      .distinct()
}
