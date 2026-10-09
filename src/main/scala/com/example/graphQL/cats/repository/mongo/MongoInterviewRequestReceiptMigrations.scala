package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoCommandException
import org.bson.Document

/** `019_interview_request_receipt_index`: the sparse `requestWorkflowId` index of the interview workflow collection.
  *
  * Request receipts are stored beside their workflow and name it in `requestWorkflowId`. Retention refreshes and
  * subject cleanup look receipts up by that field, so without the index every lifecycle transition would scan the
  * collection. Existing documents are not rewritten. The step is idempotent and safe under concurrent starts (index
  * creation with an identical definition is a no-op); an existing index of the same name with another definition fails
  * closed, and a completed proof is trusted only while the exact definition still holds.
  */
private[mongo] object MongoInterviewRequestReceiptMigrations {
  private val Id: MigrationId = MigrationIds.InterviewRequestReceiptIndex
  val MigrationId: String = Id.value
  private val CoveredProofs = List(MigrationIds.InterviewCancellationReschedule)
  private val spec = MongoHiringIndexSetup.interviewRequestReceiptIndex

  private def listIndexes(database: MongoHiringSetup.SetupDatabase): IO[List[Document]] =
    database
      .getCollection(spec.collection)
      .listIndexes[Document]
      .map(_.toList)
      .recoverWith { case error: MongoCommandException if error.getErrorCode == 26 => IO.pure(Nil) }

  private def indexProof(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    listIndexes(database).flatMap { existing =>
      existing.find(_.getString("name") == spec.options.getName) match {
        case None        => IO.pure(CompletedProof.Reopen)
        case Some(index) =>
          MongoHiringIndexSetup
            .definitionMismatch(spec, index)
            .traverse_(reason =>
              IO.raiseError[Unit](MigrationError.IndexMismatch(spec.collection, spec.options.getName, reason))
            )
            .as(CompletedProof.Trusted)
      }
    }

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoMigrationLedger.requireComplete(database, CoveredProofs) *> indexProof(database)

  private def cutover(run: MigrationRun): IO[Unit] =
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, CoveredProofs)
      _ <- MongoHiringIndexSetup.ensure(run.database.underlying, List(spec))
      proof <- verifyCompleted(run.database)
      _ <- IO.raiseUnless(proof == CompletedProof.Trusted)(
        MigrationError.StepFailed(run.id, "proof was not established")
      )
    } yield ()

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
