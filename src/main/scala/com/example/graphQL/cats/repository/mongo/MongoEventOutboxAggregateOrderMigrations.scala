package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoCommandException
import org.bson.Document

/** `035_event_outbox_aggregate_order`: the index behind the outbox claim's per-aggregate order check.
  *
  * The claim leaves an event unclaimed while an earlier `Retryable` or `InFlight` event with the same `partitionKey`
  * exists, ordered by `(occurredAt, _id)`. The check reads the earliest such event per key through the partial index
  * `(partitionKey, occurredAt, _id)` on `state` in (`Retryable`, `InFlight`), a `DISTINCT_SCAN` without a sort stage.
  * The step is expand-only: no document is rewritten, the index build is idempotent and safe under concurrent starts
  * (an identical definition is a no-op), and an existing index of the same name with another definition fails closed. A
  * completed proof is trusted only while the exact definition holds, so a dropped index reopens the step. Recovery is
  * forward-only: rerun the start to recreate the index; dropping it is safe for correctness but makes the claim scan
  * the aggregate's pending rows.
  */
private[mongo] object MongoEventOutboxAggregateOrderMigrations {
  private val Id: MigrationId = MigrationIds.EventOutboxAggregateOrder
  val MigrationId: String = Id.value
  private val CoveredProofs = List(MigrationIds.InterviewWorkflowInitiatorRole)
  private val spec = MongoHiringIndexSetup.eventOutboxAggregateOrderIndex

  private def listIndexes(database: MongoHiringSetup.SetupDatabase): IO[List[Document]] =
    database
      .getCollection(spec.collection)
      .listIndexes[Document]
      .map(_.toList)
      .recoverWith { case error: MongoCommandException if error.getErrorCode == 26 => IO.pure(Nil) }

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoMigrationLedger.requireComplete(database, CoveredProofs) *>
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
