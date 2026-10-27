package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsAggregateType, AnalyticsApplicationStatus, AnalyticsEventType}
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
  * `statsColumns` is reserved for data-skipping statistics columns and is not applied yet.
  */
private[analytics] final case class DeltaTableSpec(
    name: String,
    columns: Vector[DeltaColumn],
    checks: Vector[DeltaCheck],
    properties: Map[String, String],
    statsColumns: Vector[String],
    subjectAttributed: Boolean
) {
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
  val SkippingNumIndexedColumnsProperty = "delta.dataSkippingNumIndexedCols"
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
      subjectAttributed: Boolean = false
  ): DeltaTableSpec =
    DeltaTableSpec(
      name,
      columns,
      checks,
      // Deletion vectors would keep purged rows in files that only a rewrite removes. Subject-attributed tables also
      // gather no data-skipping statistics, because token values would otherwise be copied into the Delta log.
      Map(DeletionVectorsProperty -> "false") ++
        (if (subjectAttributed) Map(SkippingNumIndexedColumnsProperty -> "0") else Map.empty),
      Vector.empty,
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
    subjectAttributed = true
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
    subjectAttributed = true
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
    subjectAttributed = true
  )

  /** Rejected records keep only coordinates, a payload hash and tokens. A tombstone has no payload to hash. */
  val Quarantine: DeltaTableSpec = table(
    "quarantine_operational_events",
    Vector(
      required(Columns.Topic, StringType),
      required(Columns.Partition, IntegerType),
      required(Columns.Offset, LongType),
      optional("payloadHash", StringType),
      required(Columns.SubjectTokens, tokenArray),
      required("quarantineId", StringType),
      required("quarantineReason", StringType),
      required(Columns.ExpiresAt, TimestampType)
    ),
    Vector(partitionNonNegative, offsetNonNegative),
    subjectAttributed = true
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
      required("runId", StringType),
      required(Columns.Topic, StringType),
      required(Columns.Partition, IntegerType),
      required("startOffset", LongType),
      required("endOffsetExclusive", LongType),
      required("status", StringType),
      required("updatedAt", StringType)
    ),
    Vector(
      partitionNonNegative,
      DeltaCheck("start_offset_non_negative", s"${quoted("startOffset")} >= 0"),
      DeltaCheck("end_offset_not_before_start", s"${quoted("endOffsetExclusive")} >= ${quoted("startOffset")}"),
      DeltaCheck("status_valid", in("status", ManifestStatuses))
    )
  )

  // Delta cannot enforce NOT NULL inside arrays, so nested offset fields stay nullable and the typed journal decoder
  // rejects absent values.
  private val OffsetType = StructType(
    Vector(
      StructField(Columns.Topic, StringType, nullable = true),
      StructField(Columns.Partition, IntegerType, nullable = true),
      StructField("minimumDeliveredOffset", LongType, nullable = true),
      StructField("maximumDeliveredOffset", LongType, nullable = true),
      StructField("deliveredRecordCount", LongType, nullable = true)
    )
  )
  private val EndOffsetType = StructType(
    Vector(
      StructField(Columns.Topic, StringType, nullable = true),
      StructField(Columns.Partition, IntegerType, nullable = true),
      StructField("endOffset", LongType, nullable = true)
    )
  )

  val StreamingProgress: DeltaTableSpec = table(
    "control_streaming_progress",
    Vector(
      required("lineage", StringType),
      required("batchId", LongType),
      required("observedAt", TimestampType),
      optional("priorWatermark", TimestampType),
      required("inputFingerprint", StringType),
      required("sourceEndOffsets", ArrayType(EndOffsetType, containsNull = true)),
      required("deliveredOffsets", ArrayType(OffsetType, containsNull = true)),
      required("outcome", StringType),
      optional("candidateWatermark", TimestampType),
      optional("completedAt", TimestampType)
    )
  )

  val StreamingDecisions: DeltaTableSpec = table(
    "control_streaming_decisions",
    Vector(
      required("lineage", StringType),
      required("batchId", LongType),
      required("revision", LongType),
      required("deletionMarkerFingerprint", StringType),
      optional("candidateWatermark", TimestampType),
      required("publicationRunId", StringType),
      required("publicationRangeFingerprint", StringType),
      required("publicationGeneration", LongType),
      required("publicationRevision", LongType)
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
    LateFacts,
    FunnelGold,
    TimeToHireGold,
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
    paths.lateFacts -> LateFacts,
    paths.funnelGold -> FunnelGold,
    paths.timeToHireGold -> TimeToHireGold,
    paths.skillsGold -> SkillsGold,
    paths.manifests -> RunManifests,
    paths.streamingProgress -> StreamingProgress,
    paths.streamingDecisions -> StreamingDecisions,
    paths.hmacKeyRegistry -> HmacKeyRegistry
  )

  def specAt(paths: AnalyticsLakehousePaths, location: String): Option[DeltaTableSpec] =
    locations(paths).collectFirst { case (path, spec) if path == location => spec }
}
