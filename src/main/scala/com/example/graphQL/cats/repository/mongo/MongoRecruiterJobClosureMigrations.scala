package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** Store-enforced shape of one pending recruiter job closure: the deleted recruiter and the deletion time, plus the
  * retry state (`attempts`, and `notBefore` = earliest next try). Enqueue always writes both, so both are required.
  */
private[mongo] object MongoRecruiterJobClosureValidator {
  import MongoValidatorSchemas.{objectSchema, stringUuid, date, nonNegativeInt}

  val definition: Document = new Document(
    "$jsonSchema",
    objectSchema(
      List(MongoFields.Id, MongoFields.RequestedAt, MongoFields.NotBefore, MongoFields.Attempts),
      new Document(MongoFields.Id, stringUuid)
        .append(MongoFields.RequestedAt, date)
        .append(MongoFields.NotBefore, date)
        .append(MongoFields.Attempts, nonNegativeInt)
    ).append("additionalProperties", false)
  )
}

/** `020_recruiter_job_closure_work`: the durable follow-up queue for the open jobs of a deleted recruiter that did not
  * fit the first page closed by the deletion transaction.
  *
  * Expand-only: it creates one new, initially empty collection and installs its strict validator, so no existing
  * document is rewritten and an older application binary never reads or writes it. The step is idempotent and safe
  * under concurrent starts. Rows that already exist (a re-run after partial completion) are checked against the
  * validator before the proof completes; an invalid row fails closed. Recovery is forward repair: rows are transient
  * work items, and a lost row is repaired by re-enqueueing the recruiter, since open jobs of a deleted recruiter stay
  * discoverable through the job collection's `(recruiterId, status)` index. A completed proof is trusted only while the
  * exact validator holds.
  *
  * Amended in place to add the required retry fields: the migration is uncommitted and unreleased, so no shared
  * environment holds a completed 020 proof with the earlier validator (pre-MVP policy: one active shape). A developer
  * database that ran the earlier local draft fails closed with a validator mismatch; drop the
  * `recruiter_job_closure_work` collection and the `020_recruiter_job_closure_work` ledger row (or reset the local
  * database) and restart. After release, any further change is a new numbered migration.
  */
private[mongo] object MongoRecruiterJobClosureMigrations {
  private val Id: MigrationId = MigrationIds.RecruiterJobClosureWork
  val MigrationId: String = Id.value
  private val Expected = List(MongoCollections.RecruiterJobClosureWork -> MongoRecruiterJobClosureValidator.definition)

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators.assertStrictValidators(database, Expected).as(CompletedProof.Trusted)

  private def cutover(run: MigrationRun): IO[Unit] = {
    val work = run.database.getCollection(MongoCollections.RecruiterJobClosureWork)
    for {
      _ <- MongoHiringValidators.install(
        run.database,
        MongoCollections.RecruiterJobClosureWork,
        MongoRecruiterJobClosureValidator.definition
      )
      // collMod does not retroactively validate existing rows.
      invalid <- work
        .find(new Document("$nor", List(MongoRecruiterJobClosureValidator.definition).asJava))
        .limit(1)
        .first
      _ <- IO.raiseUnless(invalid.isEmpty)(MigrationError.StepFailed(run.id, "stored closure work verification failed"))
      _ <- verifyCompleted(run.database)
    } yield ()
  }

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
