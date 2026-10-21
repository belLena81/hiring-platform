package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** `025_operational_collection_validators`: every application-owned operational collection gets a strict validator.
  *
  * Expand-then-verify, in three phases. First every stored row is audited against its validator with `$nor` (a strict
  * `collMod` is not retroactive and would otherwise leave invalid rows behind); the first collection with a violating
  * row fails the step closed, naming the collection only. Then the validators are installed (strict level, error
  * action, `ensureCollection` first so a fresh database works). Finally the audit repeats, which closes the window in
  * which a writer could have stored an invalid row between the first audit and the install. Nothing is rewritten, so
  * there is no checkpoint; the whole step is idempotent and a restart reruns the same three phases.
  *
  * The completed proof is read-only and exact: every definition in `MongoOperationalValidators.all` (users V2 included)
  * must be installed unchanged (or, for search-session-work, its successor from `026`), so later drift fails closed
  * with the drifted collection and is never overwritten. The users validator is also installed at every start by
  * `MongoHiringSetup`; the earlier proof of `016_candidate_residence_integrity` accepts it as the documented successor
  * of its own definition.
  *
  * Recovery is forward-only. Stop every writer running an earlier binary before upgrading: it stores `null` optional
  * values, `subjectIds` on workflows and notification receipts, an inbox receipt without `documentType` and a user
  * without `updatedAt`, all of which the validators reject. Application rollback does not remove the validators; an
  * earlier binary cannot write to a validated database, so repair forward with the current binary.
  */
private[mongo] object MongoOperationalCollectionValidatorMigrations {
  private val Id: MigrationId = MigrationIds.OperationalCollectionValidators
  val MigrationId: String = Id.value

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators
      .assertStrictValidators(database, MongoOperationalValidators.all)
      .handleErrorWith {
        case _: MigrationError.ValidatorMismatch =>
          // Migration 026 replaces the search-session-work definition with its documented successor.
          MongoHiringValidators.assertStrictValidators(database, MongoOperationalValidators.withEnvelopeSource)
        case error => IO.raiseError(error)
      }
      .as(CompletedProof.Trusted)

  /** Fails with the first collection holding a row its validator rejects; the error names the collection only. */
  private def audit(run: MigrationRun): IO[Unit] =
    MongoOperationalValidators.all.traverse_ { case (name, validator) =>
      run.database
        .getCollection(name)
        .find(new Document("$nor", List(validator).asJava))
        .limit(1)
        .first
        .flatMap(invalid => IO.whenA(invalid.nonEmpty)(run.fail[Unit](s"found invalid stored data in $name")))
    }

  private def cutover(run: MigrationRun): IO[Unit] =
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, MigrationIds.OptionalFieldAbsence)
      _ <- audit(run)
      _ <- MongoOperationalValidators.all.traverse_ { case (name, validator) =>
        MongoHiringValidators.install(run.database, name, validator)
      }
      _ <- audit(run)
      proof <- verifyCompleted(run.database)
      _ <- IO.raiseUnless(proof == CompletedProof.Trusted)(
        MigrationError.StepFailed(run.id, "proof was not established")
      )
    } yield ()

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
