package com.example.hiring.analytics.adapter.mongo

/** Stable Mongo namespaces and BSON field names shared by analytics adapters. */
private[analytics] object AnalyticsCollections {
  val ReportSnapshots = "analytics_report_snapshots"
  val ReportRuns = "analytics_report_runs"
  val ReportControl = "analytics_report_control"
  val ErasureRequests = "analytics_erasure_requests"
  val ErasureCompletions = "analytics_erasure_completions"
  val ErasureDeltaFiles = "analytics_erasure_delta_files"
  val ErasureHeartbeats = "analytics_worker_heartbeats"
  val EventOutbox = "event_outbox"
  val HiringMigrationLedger = "hiring_migration_ledger"
  val OutboxSubjectFences = "outbox_subject_fences"
  val Users = "users"
  val ProducerRegistrations = "producer_registrations"

  /** Index names the analytics build creates and owns, including those on collections shared with the application. */
  object IndexNames {
    val ReportRunsRetention = "analytics_report_runs_retention_v1"
    val ErasureDeltaFilesRequest = "analytics_erasure_delta_files_request_v1"
    val LateFactReplayDetailsRetention = "analytics_late_fact_replay_details_retention_v1"
    val LateFactReplayReceiptDependencies = "analytics_late_fact_replay_receipt_dependencies_v1"
  }

  object Fields {
    val Id = "_id"
    val State = "state"
    val Phase = "phase"
    val SubjectToken = "subjectToken"
    val SubjectIds = "subjectIds"
    val SubjectRefsVersion = "subjectRefsVersion"
    val LeaseToken = "leaseToken"
    val LeaseUntil = "leaseUntil"
    val ResumeAfter = "resumeAfter"
    val FailureCategory = "failureCategory"
    val AttemptCount = "attemptCount"
    val RepairRequired = "repairRequired"
    val RequestId = "requestId"
    val FilePath = "filePath"
    val Progress = "progress"
    val ProgressKey = "progressKey"
    val UpdatedAt = "updatedAt"
    val CreatedAt = "createdAt"
    val ExpiresAt = "expiresAt"
    val DeltaPurgedAt = "deltaPurgedAt"
    val DeltaGeneration = "deltaGeneration"
    val DeltaAffectedRows = "deltaAffectedRows"
    val DeltaEvidenceRevision = "deltaEvidenceRevision"
    val KafkaRetentionBarrier = "kafkaRetentionBarrier"
    val ReceiptId = "receiptId"
    val AccountStatus = "accountStatus"
    val RequestedAt = "requestedAt"
    val FencingVersion = "fencingVersion"
    val TransactionalIds = "transactionalIds"
    val Deleted = "deleted"
    val Topic = "topic"
    val Partitions = "partitions"
    val PartitionNumber = "number"
    val EndOffsetExclusive = "endOffsetExclusive"
    val RangeFingerprint = "rangeFingerprint"
    val Generation = "generation"
    val Revision = "revision"
    val NextRevision = "nextRevision"
    val LastPublishedRevision = "lastPublishedRevision"
    val LastRunId = "lastRunId"
    val RunId = "runId"
    val HiddenAt = "hiddenAt"
    val CompletedAt = "completedAt"
    val AsOf = "asOf"
    val SubjectId = "subjectId"
    val Kind = "kind"
    val TransactionalId = "transactionalId"
    val GenerationId = "generationId"
    val FencedAt = "fencedAt"
    val ProducerRegistry = "producerRegistry"
  }

  /** Persisted values of the main-owned producer registration `kind` and `state` fields. */
  object ProducerRegistration {
    val OperationalKind = "Operational"
    val InterviewKind = "Interview"
    val ActiveState = "Active"
    val FencedState = "Fenced"
  }

  object MigrationIds {
    val AttributableProducerRegistrations = "012_attributable_producer_registrations"
    val ProducerGenerationRegistrations = "036_producer_generation_registrations"
    val OutboxSubjectReferences = "003_event_outbox_subject_references"
  }
}
