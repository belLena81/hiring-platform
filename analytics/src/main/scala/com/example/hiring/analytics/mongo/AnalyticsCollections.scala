package com.example.hiring.analytics.mongo

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
  }

  object MigrationIds {
    val OutboxSubjectReferences = "003_event_outbox_subject_references"
  }
}
