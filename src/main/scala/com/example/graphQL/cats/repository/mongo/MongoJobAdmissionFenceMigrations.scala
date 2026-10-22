package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** `021_job_admission_fence`: separates the job write-conflict fence from the content revision.
  *
  * Expand-only. The jobs validator gains an optional `admissionFence` long >= 0; absent means 0, so there is no
  * backfill and no existing document is rewritten. Application submission and interview scheduling `$inc` the fence
  * instead of `version`, and embedding writes no longer advance `version`. Every stored `admissionFence` is audited
  * before the proof completes (`collMod` does not validate existing rows); an invalid value fails closed. The completed
  * proof is read-only: it trusts the step while the exact successor validator holds. The validator installed at every
  * startup is the same document, so a restart writes nothing to the ledger.
  *
  * Recovery is forward-only. Stop writers that run the earlier binary before upgrading (it rejects the new field and
  * would keep advancing `version` as a fence); application rollback does not remove `admissionFence`, and the earlier
  * validator would reject the field, so repair forward by running the current binary.
  */
private[mongo] object MongoJobAdmissionFenceMigrations {
  private val Id: MigrationId = MigrationIds.JobAdmissionFence
  val MigrationId: String = Id.value

  /** The proof accepts the validator this step installed and its documented successors (`027`, `028`). */
  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators
      .assertAnyStrictValidator(database, MongoCollections.Jobs, MongoHiringValidators.jobValidatorHistory)
      .as(CompletedProof.Trusted)

  private def cutover(run: MigrationRun): IO[Unit] = {
    val jobs = run.database.getCollection(MongoCollections.Jobs)
    for {
      _ <- MongoHiringValidators.install(run.database, MongoCollections.Jobs, MongoHiringValidators.jobValidatorV2)
      // Only the new field is audited: other legacy job defects keep their own typed errors (the geo-point proof).
      invalid <- jobs
        .find(
          new Document(MongoFields.AdmissionFence, new Document("$exists", true)).append(
            "$nor",
            List(new Document(MongoFields.AdmissionFence, new Document("$type", "long").append("$gte", 0L))).asJava
          )
        )
        .limit(1)
        .first
      _ <- IO.raiseUnless(invalid.isEmpty)(MigrationError.StepFailed(run.id, "stored job verification failed"))
      _ <- MongoHiringValidators.assertStrictValidators(
        run.database,
        List(MongoCollections.Jobs -> MongoHiringValidators.jobValidatorV2)
      )
    } yield ()
  }

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
