package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{
  AnalyticsAggregateType,
  AnalyticsApplicationStatus,
  AnalyticsEventType,
  AnalyticsHiringStageMetric,
  AnalyticsRetention
}
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, AnalyticsManifestStatus}

import org.apache.spark.sql.types.*

/** One typed column of a Delta table. `nullable = false` becomes a Delta NOT NULL that the writer enforces. */
private[analytics] final case class DeltaColumn(name: String, dataType: DataType, nullable: Boolean)

/** A named Delta CHECK constraint. `expression` is SQL over the table's own columns. */
private[analytics] final case class DeltaCheck(name: String, expression: String) {
  // Delta lowercases constraint names in the table properties, so only lowercase names round-trip.
  require(name.matches("[a-z][a-z0-9_]*"), s"constraint name must be lowercase snake case: $name")
}

/** The single declaration of a Delta table: ordered columns, constraints, properties and privacy classification.
  *
  * `statsColumns` is the explicit data-skipping allow-list of a subject-attributed table. It is never empty, names
  * declared columns and holds no token or raw identifier column; the other tables leave it empty.
  */
private[analytics] final case class DeltaTableSpec(
    name: String,
    columns: Vector[DeltaColumn],
    checks: Vector[DeltaCheck],
    properties: Map[String, String],
    statsColumns: Vector[String],
    subjectAttributed: Boolean
) {
  require(
    !subjectAttributed || statsColumns.nonEmpty,
    s"subject-attributed table $name must declare its statistics columns"
  )
  require(
    statsColumns.forall(stat => columns.exists(_.name == stat) && !DeltaStatsPolicy.isForbidden(stat)),
    s"table $name declares a statistics column that is unknown or holds a subject token or raw identifier"
  )

  def shape: Vector[(String, DataType)] = columns.map(column => column.name -> column.dataType)

  /** Schema with the declared nullability, used to create the table. */
  def schema: StructType = StructType(columns.map(column => StructField(column.name, column.dataType, column.nullable)))
}

/** Every Delta table the analytics build creates, declared once. Producers that rely on a NOT NULL or CHECK below
  * guarantee it: Bronze, Silver, late facts and event-derived quarantine rows only hold events that passed envelope
  * validation, and Gold counts are aggregate counts.
  */
private[analytics] object AnalyticsDeltaCatalog {
  val DeletionVectorsProperty = "delta.enableDeletionVectors"
  val ConstraintPropertyPrefix = "delta.constraints."

  private def required(name: String, dataType: DataType) = DeltaColumn(name, dataType, nullable = false)
  private def optional(name: String, dataType: DataType) = DeltaColumn(name, dataType, nullable = true)

  private def literals(values: Seq[String]): String =
    values.map(value => s"'${value.replace("'", "''")}'").mkString(", ")
  private def quoted(column: String): String = s"`$column`"
  private def in(column: String, values: Seq[String]): String = s"${quoted(column)} IN (${literals(values)})"

  /** Derived from the domain enums so the wire values are never copied by hand. */
  private val EventTypes = AnalyticsEventType.values.toVector.map(_.wire)
  private val AggregateTypes = AnalyticsAggregateType.values.toVector.map(_.wire)
  private val ApplicationStatuses = "Created" +: AnalyticsApplicationStatus.values.toVector.map(_.wire)
  private val ManifestStatuses = AnalyticsManifestStatus.values.toVector.map(_.persistedName)

  private def eventTypeValid = DeltaCheck("event_type_valid", in(Columns.EventType, EventTypes))
  private def aggregateTypeValid = DeltaCheck("aggregate_type_valid", in(Columns.AggregateType, AggregateTypes))
  private def newStatusValid = DeltaCheck(
    "new_status_valid",
    s"${quoted(Columns.NewStatus)} IS NULL OR ${in(Columns.NewStatus, ApplicationStatuses)}"
  )
  private def partitionNonNegative = DeltaCheck("partition_non_negative", s"${quoted(Columns.Partition)} >= 0")
  private def offsetNonNegative = DeltaCheck("offset_non_negative", s"${quoted(Columns.Offset)} >= 0")
  private def expiresAfterIngestion =
    DeltaCheck("expires_after_ingestion", s"${quoted(Columns.ExpiresAt)} > ${quoted(Columns.IngestedAt)}")
  private def table(
      name: String,
      columns: Vector[DeltaColumn],
      checks: Vector[DeltaCheck] = Vector.empty,
      subjectAttributed: Boolean = false,
      statsColumns: Vector[String] = Vector.empty
  ): DeltaTableSpec =
    DeltaTableSpec(
      name,
      columns,
      checks,
      // Deletion vectors would keep purged rows in files that only a rewrite removes. Subject-attributed tables gather
      // data-skipping statistics only on their explicit allow-list, because token values would otherwise be copied
      // into the Delta log.
      Map(DeletionVectorsProperty -> "false") ++
        (if (subjectAttributed) Map(DeltaStatsPolicy.StatsColumnsProperty -> DeltaStatsPolicy.render(statsColumns))
         else Map.empty),
      statsColumns,
      subjectAttributed
    )

  private val tokenArray = ArrayType(StringType, containsNull = false)

  /** Normalized event fields shared by Silver and Bronze (every row is a validated event). */
  val SilverEventColumns: Vector[DeltaColumn] = Vector(
    required(Columns.EventId, StringType),
    required(Columns.EventType, StringType),
    required(Columns.OccurredAt, TimestampType),
    required(Columns.AggregateType, StringType),
    required(Columns.AggregateId, StringType),
    optional(Columns.ApplicationId, StringType),
    optional(Columns.JobId, StringType),
    optional(Columns.NewStatus, StringType),
    optional(Columns.JobSkills, ArrayType(StringType)),
    required(Columns.SubjectToken, StringType),
    required(Columns.SubjectTokens, tokenArray),
    required(Columns.EventFingerprint, StringType)
  )
  val ExpiryColumns: Vector[DeltaColumn] =
    Vector(required(Columns.IngestedAt, TimestampType), required(Columns.ExpiresAt, TimestampType))

  private val eventChecks = Vector(eventTypeValid, aggregateTypeValid, newStatusValid)

  val Silver: DeltaTableSpec = table(
    "silver_operational_events",
    SilverEventColumns ++ ExpiryColumns,
    eventChecks :+ expiresAfterIngestion,
    subjectAttributed = true,
    // MERGE key, plus the time columns read by retention expiry and the event-time windows.
    statsColumns = Vector(Columns.EventId, Columns.OccurredAt, Columns.IngestedAt, Columns.ExpiresAt)
  )

  /** One row per application: the earliest time it entered each status, pseudonymized. `subjectToken` is the candidate
    * token pinned per application and `subjectTokens` also holds the actors' tokens. `terminalAt` and `expiresAt` are
    * always derived from the stage columns, never merged on their own.
    */
  val ApplicationStageTimes: DeltaTableSpec = table(
    "silver_application_stage_times",
    Vector(
      required(Columns.ApplicationId, StringType),
      required(Columns.SubjectToken, StringType),
      required(Columns.SubjectTokens, tokenArray),
      optional(Columns.CreatedAt, TimestampType),
      optional(Columns.AcceptedAt, TimestampType),
      optional(Columns.DeclinedAt, TimestampType),
      optional(Columns.InterviewAt, TimestampType),
      optional(Columns.HiredAt, TimestampType),
      optional(Columns.RejectedAt, TimestampType),
      required(Columns.LastEventAt, TimestampType),
      optional(Columns.TerminalAt, TimestampType),
      required(Columns.ExpiresAt, TimestampType)
    ),
    Vector(
      DeltaCheck(
        "expires_after_terminal",
        s"${quoted(Columns.TerminalAt)} IS NULL OR ${quoted(Columns.ExpiresAt)} > ${quoted(Columns.TerminalAt)}"
      )
    ),
    subjectAttributed = true,
    // MERGE key (a random application UUID, not a subject) and the time columns read by retention expiry.
    statsColumns = Vector(Columns.ApplicationId, Columns.LastEventAt, Columns.TerminalAt, Columns.ExpiresAt)
  )

  /** Persisted Bronze: Kafka coordinates, envelope fingerprint, normalized fields and tokens; no raw identifiers. */
  val Bronze: DeltaTableSpec = table(
    "bronze_operational_events",
    Vector(
      required(Columns.Topic, StringType),
      required(Columns.Partition, IntegerType),
      required(Columns.Offset, LongType),
      required(Columns.KafkaTimestamp, TimestampType),
      required(Columns.EventId, StringType),
      required(Columns.EventType, StringType),
      required(Columns.OccurredAt, TimestampType),
      required(Columns.AggregateType, StringType),
      required(Columns.AggregateId, StringType),
      required(Columns.EventFingerprint, StringType),
      optional(Columns.ApplicationId, StringType),
      optional(Columns.JobId, StringType),
      optional(Columns.NewStatus, StringType),
      optional(Columns.JobSkills, ArrayType(StringType)),
      required(Columns.SubjectToken, StringType),
      required(Columns.SubjectTokens, tokenArray)
    ) ++ ExpiryColumns,
    eventChecks ++ Vector(partitionNonNegative, offsetNonNegative, expiresAfterIngestion),
    subjectAttributed = true,
    // MERGE key (Kafka coordinates) and the time columns read by retention expiry and event-time windows.
    statsColumns = Vector(
      Columns.Topic,
      Columns.Partition,
      Columns.Offset,
      Columns.OccurredAt,
      Columns.IngestedAt,
      Columns.ExpiresAt
    )
  )

  /** Closed-day facts retain only normalized, pseudonymized fields needed for bounded replay. */
  val LateFacts: DeltaTableSpec = table(
    "silver_late_operational_events",
    Vector(
      required(Columns.EventId, StringType),
      required(Columns.EventFingerprint, StringType),
      required(Columns.EventType, StringType),
      required(Columns.Topic, StringType),
      required(Columns.Partition, IntegerType),
      required(Columns.Offset, LongType),
      required(Columns.OccurredAt, TimestampType),
      required(Columns.AggregateType, StringType),
      required(Columns.AggregateId, StringType),
      optional(Columns.ApplicationId, StringType),
      optional(Columns.JobId, StringType),
      optional(Columns.NewStatus, StringType),
      optional(Columns.JobSkills, ArrayType(StringType)),
      required(Columns.SubjectToken, StringType),
      required(Columns.SubjectTokens, tokenArray),
      required(Columns.AdmissionReason, StringType),
      required(Columns.IngestedAt, TimestampType),
      required(Columns.ExpiresAt, TimestampType)
    ),
    eventChecks ++ Vector(partitionNonNegative, offsetNonNegative, expiresAfterIngestion),
    subjectAttributed = true,
    // MERGE key (Kafka coordinates), the event-ID lookup of replay and the time columns read by retention expiry.
    statsColumns = Vector(
      Columns.Topic,
      Columns.Partition,
      Columns.Offset,
      Columns.EventId,
      Columns.OccurredAt,
      Columns.IngestedAt,
      Columns.ExpiresAt
    )
  )

  /** Rejected records keep only coordinates, a payload hash and tokens. A tombstone has no payload to hash. */
  val Quarantine: DeltaTableSpec = table(
    "quarantine_operational_events",
    Vector(
      required(Columns.Topic, StringType),
      required(Columns.Partition, IntegerType),
      required(Columns.Offset, LongType),
      optional(Columns.PayloadHash, StringType),
      required(Columns.SubjectTokens, tokenArray),
      required(Columns.QuarantineId, StringType),
      required(Columns.QuarantineReason, StringType),
      required(Columns.ExpiresAt, TimestampType)
    ),
    Vector(partitionNonNegative, offsetNonNegative),
    subjectAttributed = true,
    // Kafka coordinates (offset-range lookups) and expiry. `quarantineId` is a SHA-256 of the raw record and spreads
    // uniformly, so min/max skipping on it would not prune; it is not indexed.
    statsColumns = Vector(Columns.Topic, Columns.Partition, Columns.Offset, Columns.ExpiresAt)
  )

  private def nonNegativeCount(column: String) =
    DeltaCheck(s"${column.toLowerCase(java.util.Locale.ROOT)}_non_negative", s"${quoted(column)} >= 0")

  val FunnelGold: DeltaTableSpec = {
    val counts = Vector(
      Columns.Created,
      Columns.Accepted,
      Columns.Declined,
      Columns.Interview,
      Columns.Hired,
      Columns.Rejected
    )
    table(
      "gold_application_funnel",
      required(Columns.Day, DateType) +: counts.map(required(_, LongType)),
      counts.map(nonNegativeCount)
    )
  }

  val TimeToHireGold: DeltaTableSpec = table(
    "gold_time_to_hire",
    Vector(
      required(Columns.P50Hours, DoubleType),
      required(Columns.P75Hours, DoubleType),
      required(Columns.P90Hours, DoubleType),
      required(Columns.P95Hours, DoubleType),
      required(Columns.EligibleCount, LongType),
      required(Columns.ExcludedCount, LongType)
    ),
    Vector(
      DeltaCheck(
        "percentiles_ordered",
        s"${quoted(Columns.P50Hours)} <= ${quoted(Columns.P75Hours)} AND " +
          s"${quoted(Columns.P75Hours)} <= ${quoted(Columns.P90Hours)} AND " +
          s"${quoted(Columns.P90Hours)} <= ${quoted(Columns.P95Hours)}"
      ),
      nonNegativeCount(Columns.EligibleCount),
      nonNegativeCount(Columns.ExcludedCount)
    )
  )

  private val StageMetrics = AnalyticsHiringStageMetric.values.toVector.map(_.wire)

  val HiringStageDurationsGold: DeltaTableSpec = table(
    "gold_hiring_stage_durations",
    Vector(
      required(Columns.Metric, StringType),
      required(Columns.P50Hours, DoubleType),
      required(Columns.P75Hours, DoubleType),
      required(Columns.P90Hours, DoubleType),
      required(Columns.P95Hours, DoubleType),
      required(Columns.EligibleCount, LongType)
    ),
    Vector(
      DeltaCheck("metric_valid", in(Columns.Metric, StageMetrics)),
      DeltaCheck(
        "percentiles_ordered",
        s"${quoted(Columns.P50Hours)} <= ${quoted(Columns.P75Hours)} AND " +
          s"${quoted(Columns.P75Hours)} <= ${quoted(Columns.P90Hours)} AND " +
          s"${quoted(Columns.P90Hours)} <= ${quoted(Columns.P95Hours)}"
      ),
      nonNegativeCount(Columns.EligibleCount),
      // K-anonymity: a published metric rests on at least K distinct candidate subjects.
      DeltaCheck(
        "eligible_count_minimum",
        s"${quoted(Columns.EligibleCount)} >= ${AnalyticsRetention.MinimumContributors}"
      )
    )
  )

  val SkillsGold: DeltaTableSpec = table(
    "gold_job_skills",
    Vector(
      required(Columns.Day, DateType),
      required(Columns.Skill, StringType),
      required(Columns.Postings, LongType)
    ),
    Vector(nonNegativeCount(Columns.Postings))
  )

  val RunManifests: DeltaTableSpec = table(
    "control_run_manifests",
    Vector(
      required(Columns.RunId, StringType),
      required(Columns.Topic, StringType),
      required(Columns.Partition, IntegerType),
      required(Columns.StartOffset, LongType),
      required(Columns.EndOffsetExclusive, LongType),
      required(Columns.Status, StringType),
      required(Columns.UpdatedAt, StringType)
    ),
    Vector(
      partitionNonNegative,
      DeltaCheck("start_offset_non_negative", s"${quoted(Columns.StartOffset)} >= 0"),
      DeltaCheck(
        "end_offset_not_before_start",
        s"${quoted(Columns.EndOffsetExclusive)} >= ${quoted(Columns.StartOffset)}"
      ),
      DeltaCheck("status_valid", in(Columns.Status, ManifestStatuses))
    )
  )

  // Delta cannot enforce NOT NULL inside arrays, so nested offset fields stay nullable and the typed journal decoder
  // rejects absent values.
  private val OffsetType = StructType(
    Vector(
      StructField(Columns.Topic, StringType, nullable = true),
      StructField(Columns.Partition, IntegerType, nullable = true),
      StructField(Columns.MinimumDeliveredOffset, LongType, nullable = true),
      StructField(Columns.MaximumDeliveredOffset, LongType, nullable = true),
      StructField(Columns.DeliveredRecordCount, LongType, nullable = true)
    )
  )
  private val EndOffsetType = StructType(
    Vector(
      StructField(Columns.Topic, StringType, nullable = true),
      StructField(Columns.Partition, IntegerType, nullable = true),
      StructField(Columns.EndOffset, LongType, nullable = true)
    )
  )

  val StreamingProgress: DeltaTableSpec = table(
    "control_streaming_progress",
    Vector(
      required(Columns.Lineage, StringType),
      required(Columns.BatchId, LongType),
      required(Columns.ObservedAt, TimestampType),
      optional(Columns.PriorWatermark, TimestampType),
      required(Columns.InputFingerprint, StringType),
      required(Columns.SourceEndOffsets, ArrayType(EndOffsetType, containsNull = true)),
      required(Columns.DeliveredOffsets, ArrayType(OffsetType, containsNull = true)),
      required(Columns.Outcome, StringType),
      optional(Columns.CandidateWatermark, TimestampType),
      optional(Columns.CompletedAt, TimestampType)
    )
  )

  val StreamingDecisions: DeltaTableSpec = table(
    "control_streaming_decisions",
    Vector(
      required(Columns.Lineage, StringType),
      required(Columns.BatchId, LongType),
      required(Columns.Revision, LongType),
      required(Columns.DeletionMarkerFingerprint, StringType),
      optional(Columns.CandidateWatermark, TimestampType),
      required(Columns.PublicationRunId, StringType),
      required(Columns.PublicationRangeFingerprint, StringType),
      required(Columns.PublicationGeneration, LongType),
      required(Columns.PublicationRevision, LongType)
    )
  )

  val HmacKeyRegistry: DeltaTableSpec = table(
    "control_hmac_key_registry",
    Vector(required(Columns.KeyId, StringType), required(Columns.Verifier, StringType))
  )

  val all: Vector[DeltaTableSpec] = Vector(
    Bronze,
    Quarantine,
    Silver,
    ApplicationStageTimes,
    LateFacts,
    FunnelGold,
    TimeToHireGold,
    HiringStageDurationsGold,
    SkillsGold,
    RunManifests,
    StreamingProgress,
    StreamingDecisions,
    HmacKeyRegistry
  )

  /** Tables whose every row is one event record, so a row without subject tokens is purged with the erasure sweep. */
  def holdsEventRecords(spec: DeltaTableSpec): Boolean = spec == Bronze || spec == Quarantine || spec == LateFacts

  /** Each table's spec at its location in the given lakehouse. */
  def locations(paths: AnalyticsLakehousePaths): Vector[(String, DeltaTableSpec)] = Vector(
    paths.bronze -> Bronze,
    paths.quarantine -> Quarantine,
    paths.silver -> Silver,
    paths.applicationStageTimes -> ApplicationStageTimes,
    paths.lateFacts -> LateFacts,
    paths.funnelGold -> FunnelGold,
    paths.timeToHireGold -> TimeToHireGold,
    paths.hiringStageDurationsGold -> HiringStageDurationsGold,
    paths.skillsGold -> SkillsGold,
    paths.manifests -> RunManifests,
    paths.streamingProgress -> StreamingProgress,
    paths.streamingDecisions -> StreamingDecisions,
    paths.hmacKeyRegistry -> HmacKeyRegistry
  )

  def specAt(paths: AnalyticsLakehousePaths, location: String): Option[DeltaTableSpec] =
    locations(paths).collectFirst { case (path, spec) if path == location => spec }
}
