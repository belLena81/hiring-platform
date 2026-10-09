package com.example.graphQL.cats.repository.mongo

import munit.FunSuite
import org.bson.Document

/** Pure ledger decisions and the typed error mapping used by the migration runner. */
final class MongoMigrationLedgerSpec extends FunSuite {
  private val id = MigrationIds.UserJobRevisions

  private def row(version: AnyRef, state: AnyRef): Document =
    new Document(MongoFields.Id, id.value).append(MongoFields.Version, version).append(MongoFields.State, state)

  test("an absent row means the step has never started") {
    assertEquals(MongoMigrationLedger.decode(id, None), Right(MigrationLedgerState.Absent))
  }

  test("a version-one Complete row without a checkpoint is trusted") {
    assertEquals(
      MongoMigrationLedger.decode(id, Some(row(Long.box(1L), "Complete"))),
      Right(MigrationLedgerState.Complete)
    )
  }

  test("a version-one Running row resumes from its raw checkpoint") {
    val running = row(Long.box(1L), "Running").append(MongoFields.LastId, "0000-last")
    assertEquals(
      MongoMigrationLedger.decode(id, Some(running)),
      Right(MigrationLedgerState.Running(Some("0000-last")))
    )
    assertEquals(
      MongoMigrationLedger.decode(id, Some(row(Long.box(1L), "Running"))),
      Right(MigrationLedgerState.Running(None))
    )
  }

  test("BSON Int32 versions, other versions and unknown states are corrupt proofs, not numeric matches") {
    val corrupt = List(
      row(Int.box(1), "Running") -> "unsupported version",
      row(Long.box(2L), "Complete") -> "unsupported version",
      new Document(MongoFields.Id, id.value).append(MongoFields.State, "Complete") -> "unsupported version",
      row(Long.box(1L), "Unknown") -> "unsupported state",
      new Document(MongoFields.Id, id.value).append(MongoFields.Version, Long.box(1L)) -> "unsupported state",
      row(Long.box(1L), "Complete").append(MongoFields.LastId, "stale") -> "completed proof retains a checkpoint"
    )
    corrupt.foreach { case (document, detail) =>
      assertEquals(MongoMigrationLedger.decode(id, Some(document)), Left(MigrationError.LedgerCorrupt(id, detail)))
    }
  }

  test("text checkpoints must be non-empty strings") {
    assertEquals(MongoMigrationLedger.textCheckpoint(id, None), Right(None))
    assertEquals(MongoMigrationLedger.textCheckpoint(id, Some("abc")), Right(Some("abc")))
    assertEquals(
      MongoMigrationLedger.textCheckpoint(id, Some(Int.box(2))),
      Left(MigrationError.LedgerCorrupt(id, "unsupported checkpoint"))
    )
    assertEquals(
      MongoMigrationLedger.textCheckpoint(id, Some("")),
      Left(MigrationError.LedgerCorrupt(id, "unsupported checkpoint"))
    )
  }

  test("migration identities are the persisted ledger literals") {
    val expected = List(
      "001_user_job_revisions",
      "002_candidate_search_profile_verification",
      "003_event_outbox_subject_references",
      "004_analytics_report_control",
      "005_analytics_deletion_receipts",
      "006_job_geo_points",
      "007_interview_workflow_storage",
      "008_interview_subject_cleanup",
      "009_interview_inbox_identity",
      "010_interview_workflow_attempts",
      "011_interview_publication_fencing",
      "012_attributable_producer_registrations",
      "013_hiring_workflow_integrity",
      "014_deleted_account_embeddings",
      "015_interview_cleanup_integrity",
      "016_candidate_residence_integrity",
      "017_interview_ledger_collections",
      "018_interview_cancellation_reschedule",
      "019_interview_request_receipt_index"
    )
    val actual = List(
      MigrationIds.UserJobRevisions,
      MigrationIds.CandidateSearchProfileVerification,
      MigrationIds.EventOutboxSubjectReferences,
      MigrationIds.AnalyticsReportControl,
      MigrationIds.AnalyticsDeletionReceipts,
      MigrationIds.JobGeoPoints,
      MigrationIds.InterviewWorkflowStorage,
      MigrationIds.InterviewSubjectCleanup,
      MigrationIds.InterviewInboxIdentity,
      MigrationIds.InterviewWorkflowAttempts,
      MigrationIds.InterviewPublicationFencing,
      MigrationIds.AttributableProducerRegistrations,
      MigrationIds.HiringWorkflowIntegrity,
      MigrationIds.DeletedAccountEmbeddings,
      MigrationIds.InterviewCleanupIntegrity,
      MigrationIds.CandidateResidenceIntegrity,
      MigrationIds.InterviewLedgerCollections,
      MigrationIds.InterviewCancellationReschedule,
      MigrationIds.InterviewRequestReceiptIndex
    ).map(_.value)
    assertEquals(actual, expected)
    assertEquals(MongoDeletedAccountEmbeddingMigrations.MigrationId, MigrationIds.DeletedAccountEmbeddings.value)
    assertEquals(MongoInterviewCleanupMigrations.MigrationId, MigrationIds.InterviewPublicationFencing.value)
    assertEquals(MongoProducerRegistrationMigrations.MigrationId, MigrationIds.AttributableProducerRegistrations.value)
    assertEquals(MongoWorkflowIntegrityMigrations.MigrationId, MigrationIds.HiringWorkflowIntegrity.value)
    assertEquals(MongoInterviewCleanupIntegrityMigrations.MigrationId, MigrationIds.InterviewCleanupIntegrity.value)
    assertEquals(MongoCandidateResidenceIntegrityMigrations.MigrationId, MigrationIds.CandidateResidenceIntegrity.value)
    assertEquals(MongoInterviewLedgerCollectionMigrations.MigrationId, MigrationIds.InterviewLedgerCollections.value)
    assertEquals(MongoInterviewLifecycleMigrations.MigrationId, MigrationIds.InterviewCancellationReschedule.value)
    assertEquals(MongoInterviewRequestReceiptMigrations.MigrationId, MigrationIds.InterviewRequestReceiptIndex.value)
  }

  test("typed migration errors are stack-free throwables whose messages carry identities only") {
    val errors: List[MigrationError] = List(
      MigrationError.LedgerAbsent(id),
      MigrationError.LedgerCorrupt(id, "unsupported version"),
      MigrationError.UnacknowledgedWrite(id),
      MigrationError.StepFailed(id, "found an invalid point at job 1"),
      MigrationError.ValidatorMismatch(MongoCollections.Users),
      MigrationError.IndexMismatch(MongoCollections.Jobs, MongoIndexNames.JobsCreated, "ordered keys")
    )
    errors.foreach { error =>
      assert(error.isInstanceOf[RuntimeException])
      assertEquals(error.getStackTrace.length, 0, clues(error))
      assert(error.getMessage.nonEmpty, clues(error))
    }
    assertEquals(MigrationError.LedgerAbsent(id).getMessage, "Migration 001_user_job_revisions proof is absent")
    assert(
      MigrationError
        .IndexMismatch(MongoCollections.Jobs, MongoIndexNames.JobsCreated, "ordered keys")
        .getMessage
        .contains("index definition mismatch")
    )
  }

  test("the strict validator check requires strict level, error action and the exact validator") {
    val expected = MongoHiringValidators.outboxValidator
    val installed = MongoHiringValidators
      .strictValidation(MongoCollections.EventOutbox, expected)
      .toBsonDocument(classOf[Document], com.mongodb.MongoClientSettings.getDefaultCodecRegistry)
    val _ = installed.remove("collMod")
    assert(MongoHiringValidators.strictValidatorMatches(Some(installed), expected))
    assert(!MongoHiringValidators.strictValidatorMatches(None, expected))
    assert(!MongoHiringValidators.strictValidatorMatches(Some(installed), MongoHiringValidators.userValidator))
    val moderate = installed.clone()
    val _ = moderate.put("validationLevel", new org.bson.BsonString("moderate"))
    assert(!MongoHiringValidators.strictValidatorMatches(Some(moderate), expected))
  }
}
