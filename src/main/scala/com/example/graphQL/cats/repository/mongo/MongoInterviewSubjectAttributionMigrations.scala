package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Indexes, Updates}
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*

/** `022_interview_subject_attribution`: hiring receipts become discriminated, subject-attributed inbox documents.
  *
  * Expand-then-backfill. Writers emit `documentType: "hiringReceipt"` and `subjectIds` (candidate and recruiter) and no
  * longer store the two parties as separate `candidateId`/`recruiterId` fields; this step rewrites the stored receipts
  * into the same shape. The subjects come from the receipt itself, never from its workflow, so a receipt whose workflow
  * already expired is attributed and purgeable like any other. The scan walks `_id` in bounded batches and checkpoints
  * after each one, so a restart resumes where it stopped; each update is conditional on the receipt still lacking
  * `documentType`, so two processes (or a replay) converge. An inbox row that lacks `documentType` and is not a hiring
  * receipt, or a hiring receipt without both parties, fails the step closed with its identity.
  *
  * The completed proof is read-only: no inbox row may lack `documentType`; a regression (an earlier binary still
  * writing receipts) reopens the step. The check is one `_id`-ordered pass over a TTL-bounded collection; revisit it if
  * the inbox grows beyond the completed-evidence retention. The supporting indexes are ordinary additive specifications
  * in `MongoHiringIndexSetup` and are verified on every start.
  *
  * Recovery is forward-only. Stop writers that run the earlier binary before upgrading (it writes receipts without the
  * new fields, which the reopened proof repairs on the next start); application rollback does not undo the rewrite.
  */
private[mongo] object MongoInterviewSubjectAttributionMigrations {
  private val Id: MigrationId = MigrationIds.InterviewSubjectAttribution
  val MigrationId: String = Id.value

  /** The `documentType` this step wrote; `029_interview_hiring_receipt_removal` later deleted these documents. */
  private val HiringReceiptType = MongoInterviewFields.RetiredHiringReceiptType

  private val Unattributed: Bson = Filters.exists(MongoFields.DocumentType, false)

  private def inbox(database: MongoHiringSetup.SetupDatabase) =
    database.getCollection(MongoCollections.InterviewWorkflowInbox)

  private def page(run: MigrationRun, after: Option[String]): IO[List[Document]] =
    MongoBoundedResults.collect(
      inbox(run.database)
        .find(after.fold(Unattributed)(id => Filters.and(Unattributed, Filters.gt(MongoFields.Id, id))))
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(MongoBatchScan.BatchSize)
        .boundedStream(32),
      MongoBatchScan.BatchSize,
      MigrationError.StepFailed(Id, s"setup read exceeded its maximum of ${MongoBatchScan.BatchSize} elements")
    )

  private def attribute(run: MigrationRun, row: Document): IO[Unit] = {
    val id = Option(row.getString(MongoFields.Id))
    val parties = (
      Option(row.getString(MongoFields.CandidateId)),
      Option(row.getString(MongoFields.RecruiterId))
    ).mapN((candidate, recruiter) => List(candidate, recruiter).distinct)
    (id, parties) match {
      case (Some(key), Some(subjects)) if key.endsWith(":hiring") && Option(row.getString("workflowId")).isDefined =>
        inbox(run.database)
          .updateOne(
            Filters.and(Filters.eq(MongoFields.Id, key), Unattributed),
            Updates.combine(
              Updates.set(MongoFields.DocumentType, HiringReceiptType),
              Updates.set(MongoFields.SubjectIds, subjects.asJava),
              Updates.unset(MongoFields.CandidateId),
              Updates.unset(MongoFields.RecruiterId)
            )
          )
          .void
      case (Some(key), _) => run.fail(s"found an unattributable inbox row at ${key.takeWhile(_ != ':').take(36)}")
      case _              => run.fail("found an invalid inbox key")
    }
  }

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    inbox(database).find(Unattributed).limit(1).first.map {
      case None    => CompletedProof.Trusted
      case Some(_) => CompletedProof.Reopen
    }

  /** A receipt written by an earlier binary while this step was Running can sort below the saved checkpoint; one rescan
    * from the start (without moving the checkpoint) repairs it instead of wedging the step.
    */
  private def rescan(run: MigrationRun): IO[Unit] =
    MongoBatchScan.byId[String](MongoBatchScan.BatchSize, None)(page(run, _))(row =>
      Option(row.getString(MongoFields.Id)).fold(run.fail[String]("invalid inbox key"))(IO.pure)
    )(_.traverse_(attribute(run, _)))

  private def backfill(run: MigrationRun): IO[Unit] =
    run.scanByStringId(MongoBatchScan.BatchSize, "invalid inbox key")(page(run, _))(_.traverse_(attribute(run, _))) *>
      verifyCompleted(run.database).flatMap {
        case CompletedProof.Trusted => IO.unit
        case CompletedProof.Reopen  =>
          rescan(run) *> verifyCompleted(run.database).flatMap {
            case CompletedProof.Trusted => IO.unit
            case CompletedProof.Reopen  => run.fail("inbox rows without documentType remain")
          }
      }

  val step: MongoMigrationStep = MongoMigrationStep(Id, backfill, verifyCompleted)
}
