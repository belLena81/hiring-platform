package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, UpdateOptions, Updates}
import com.mongodb.client.result.UpdateResult
import org.bson.Document
import scala.util.control.NoStackTrace

/** Persisted identity of one migration step: the literal is the `hiring_migration_ledger` `_id`. */
final case class MigrationId(value: String) extends AnyVal

/** Every applied migration identity, in application order. The literals are ledger history and never change. */
private[mongo] object MigrationIds {
  val UserJobRevisions = MigrationId("001_user_job_revisions")
  val CandidateSearchProfileVerification = MigrationId("002_candidate_search_profile_verification")
  val EventOutboxSubjectReferences = MigrationId("003_event_outbox_subject_references")
  val AnalyticsReportControl = MigrationId("004_analytics_report_control")
  val AnalyticsDeletionReceipts = MigrationId("005_analytics_deletion_receipts")
  val JobGeoPoints = MigrationId("006_job_geo_points")
  val InterviewWorkflowStorage = MigrationId("007_interview_workflow_storage")
  val InterviewSubjectCleanup = MigrationId("008_interview_subject_cleanup")
  val InterviewInboxIdentity = MigrationId("009_interview_inbox_identity")
  val InterviewWorkflowAttempts = MigrationId("010_interview_workflow_attempts")
  val InterviewPublicationFencing = MigrationId("011_interview_publication_fencing")
  val AttributableProducerRegistrations = MigrationId("012_attributable_producer_registrations")
  val HiringWorkflowIntegrity = MigrationId("013_hiring_workflow_integrity")
  val DeletedAccountEmbeddings = MigrationId("014_deleted_account_embeddings")
  val InterviewCleanupIntegrity = MigrationId("015_interview_cleanup_integrity")
  val CandidateResidenceIntegrity = MigrationId("016_candidate_residence_integrity")
}

/** Typed setup failures. They remain Throwables so the setup lifecycle boundary keeps one failure channel, while
  * callers and tests can match on the case instead of a message. Messages carry identities only, never row data.
  */
sealed abstract class MigrationError(message: String) extends RuntimeException(message) with NoStackTrace

object MigrationError {
  final case class LedgerAbsent(id: MigrationId) extends MigrationError(s"Migration ${id.value} proof is absent")
  final case class LedgerCorrupt(id: MigrationId, detail: String)
      extends MigrationError(s"Migration ${id.value} proof is unsupported: $detail")
  final case class UnacknowledgedWrite(id: MigrationId)
      extends MigrationError(s"Migration ${id.value} write was not acknowledged")
  final case class StepFailed(id: MigrationId, detail: String)
      extends MigrationError(s"Migration ${id.value} failed: $detail")
  final case class ValidatorMismatch(collection: String)
      extends MigrationError(s"Collection '$collection' validator changed; explicit maintenance repair required")
  final case class IndexMismatch(collection: String, index: String, reason: String)
      extends MigrationError(s"Mongo index definition mismatch for collection '$collection' index '$index': $reason")
}

/** Decoded ledger row. `Running` carries the raw resumable checkpoint (`lastId`) when one was recorded. */
private[mongo] enum MigrationLedgerState {
  case Absent
  case Running(checkpoint: Option[AnyRef])
  case Complete
}

/** The one ledger protocol: version-1 rows in `Running` or `Complete`, upserted once, completed in place. */
private[mongo] object MongoMigrationLedger {
  import MongoHiringSetup.{SetupCollection, SetupDatabase}

  val Version: Long = 1L
  val RunningState = "Running"
  val CompleteState = "Complete"

  /** Pure decision over a stored row. Version must be BSON int64 one; Int32 or other states fail closed. */
  def decode(id: MigrationId, row: Option[Document]): Either[MigrationError, MigrationLedgerState] = row match {
    case None           => Right(MigrationLedgerState.Absent)
    case Some(document) =>
      val supportedVersion = Option(document.get(MongoFields.Version)).exists {
        case value: java.lang.Long => value.longValue() == Version
        case _                     => false
      }
      val checkpoint = Option(document.get(MongoFields.LastId))
      if (!supportedVersion) Left(MigrationError.LedgerCorrupt(id, "unsupported version"))
      else
        Option(document.get(MongoFields.State)) match {
          case Some(RunningState)                        => Right(MigrationLedgerState.Running(checkpoint))
          case Some(CompleteState) if checkpoint.isEmpty => Right(MigrationLedgerState.Complete)
          case Some(CompleteState)                       =>
            Left(MigrationError.LedgerCorrupt(id, "completed proof retains a checkpoint"))
          case _ => Left(MigrationError.LedgerCorrupt(id, "unsupported state"))
        }
  }

  /** String checkpoints are the common `_id` cursor shape; anything else is a corrupt proof, not a restart point. */
  def textCheckpoint(id: MigrationId, checkpoint: Option[AnyRef]): Either[MigrationError, Option[String]] =
    checkpoint.traverse {
      case value: String if value.nonEmpty => Right(value)
      case _                               => Left(MigrationError.LedgerCorrupt(id, "unsupported checkpoint"))
    }

  def read(database: SetupDatabase, id: MigrationId): IO[MigrationLedgerState] =
    ledger(database).find(identity(id)).first.flatMap(row => IO.fromEither(decode(id, row)))

  /** Creates the `Running` row once. An existing row is never downgraded: a concurrent runner may already have
    * completed the step, and a duplicate-key race on the upsert is the same no-op.
    */
  def markRunning(database: SetupDatabase, id: MigrationId): IO[Unit] =
    ledger(database)
      .updateOne(
        identity(id),
        Updates.combine(
          Updates.setOnInsert(MongoFields.Version, Long.box(Version)),
          Updates.setOnInsert(MongoFields.State, RunningState)
        ),
        new UpdateOptions().upsert(true)
      )
      .flatMap(acknowledged(id, _))
      .recoverWith { case error: MongoWriteException if error.getError.getCode == 11000 => IO.unit }
      // A concurrently inserted row is re-decoded so a corrupt one cannot proceed to completion.
      .productR(read(database, id).void)

  /** Reopens a completed proof whose covered data regressed; the step is then re-run from the beginning. */
  def reopen(database: SetupDatabase, id: MigrationId): IO[Unit] =
    ledger(database)
      .updateOne(
        identity(id),
        Updates
          .combine(Updates.set(MongoFields.State, RunningState), Updates.set(MongoFields.Version, Long.box(Version)))
      )
      .flatMap(acknowledged(id, _))

  /** Advances the resumable cursor only while the step is still `Running`; `$max` keeps concurrent runners monotonic. A
    * row completed by another runner in the meantime is accepted as the final state.
    */
  def checkpoint(database: SetupDatabase, id: MigrationId, lastId: AnyRef): IO[Unit] =
    ledger(database)
      .updateOne(
        Filters.and(identity(id), Filters.eq(MongoFields.State, RunningState)),
        Updates.max(MongoFields.LastId, lastId)
      )
      .flatMap { result =>
        acknowledged(id, result) *> (if (result.getMatchedCount == 1L) IO.unit else requireComplete(database, id))
      }

  /** Completes in place and drops the checkpoint, then reads the proof back so a lost row cannot pass silently. */
  def markComplete(database: SetupDatabase, id: MigrationId): IO[Unit] =
    ledger(database)
      .updateOne(
        Filters.and(identity(id), Filters.eq(MongoFields.Version, Long.box(Version))),
        Updates.combine(
          Updates.set(MongoFields.Version, Long.box(Version)),
          Updates.set(MongoFields.State, CompleteState),
          Updates.unset(MongoFields.LastId)
        )
      )
      .flatMap { result =>
        acknowledged(id, result) *>
          (if (result.getMatchedCount == 1L) IO.unit else IO.raiseError(MigrationError.LedgerAbsent(id)))
      } *> requireComplete(database, id)

  /** A proof another step depends on must be `Complete`; absent or running proofs fail closed. */
  def requireComplete(database: SetupDatabase, id: MigrationId): IO[Unit] = read(database, id).flatMap {
    case MigrationLedgerState.Complete   => IO.unit
    case MigrationLedgerState.Absent     => IO.raiseError(MigrationError.LedgerAbsent(id))
    case MigrationLedgerState.Running(_) =>
      IO.raiseError(MigrationError.LedgerCorrupt(id, "required proof is not complete"))
  }

  def requireComplete(database: SetupDatabase, ids: List[MigrationId]): IO[Unit] =
    ids.traverse_(requireComplete(database, _))

  private def identity(id: MigrationId) = Filters.eq(MongoFields.Id, id.value)

  private def ledger(database: SetupDatabase): SetupCollection =
    database.getCollection(MongoCollections.HiringMigrationLedger)

  private def acknowledged(id: MigrationId, result: UpdateResult): IO[Unit] =
    IO.raiseUnless(result.wasAcknowledged())(MigrationError.UnacknowledgedWrite(id))
}
