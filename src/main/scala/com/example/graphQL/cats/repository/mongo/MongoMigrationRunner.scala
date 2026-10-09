package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*

/** Outcome of re-verifying a completed proof: trust it, or reopen the step because covered data regressed. */
private[mongo] enum CompletedProof {
  case Trusted
  case Reopen
}

/** Effect handle a step receives: the database, its own identity and the resumable checkpoint, plus the only two ledger
  * writes a step may perform while running (advance the checkpoint, raise a typed step failure).
  */
private[mongo] final case class MigrationRun(
    database: MongoHiringSetup.SetupDatabase,
    id: MigrationId,
    checkpoint: Option[AnyRef]
) {
  def textCheckpoint: IO[Option[String]] = IO.fromEither(MongoMigrationLedger.textCheckpoint(id, checkpoint))
  def advance(lastId: AnyRef): IO[Unit] = MongoMigrationLedger.checkpoint(database, id, lastId)
  def fail[A](detail: String): IO[A] = IO.raiseError(MigrationError.StepFailed(id, detail))
}

/** One ledger-tracked migration. `run` must be idempotent and restart-safe from its checkpoint; `whenComplete`
  * re-verifies a completed proof on every later startup (validators, covered proofs, absence checks).
  */
private[mongo] final case class MongoMigrationStep(
    id: MigrationId,
    run: MigrationRun => IO[Unit],
    whenComplete: MongoHiringSetup.SetupDatabase => IO[CompletedProof] = _ => IO.pure(CompletedProof.Trusted)
)

/** The single ledger protocol: read → trust a completed proof (or reopen it) → mark Running once → run from the
  * checkpoint → mark Complete and read the proof back. Two processes starting the same step both run its idempotent
  * work; the ledger upsert never downgrades a row, checkpoints only advance while Running, and the second completion is
  * an in-place no-op. Unsupported rows fail closed with a typed error before any step work.
  */
private[mongo] object MongoMigrationRunner {
  import MongoHiringSetup.SetupDatabase

  def run(database: SetupDatabase, step: MongoMigrationStep): IO[Unit] =
    MongoMigrationLedger.read(database, step.id).flatMap {
      case MigrationLedgerState.Complete =>
        step.whenComplete(database).flatMap {
          case CompletedProof.Trusted => IO.unit
          case CompletedProof.Reopen  => MongoMigrationLedger.reopen(database, step.id) *> execute(database, step, None)
        }
      case MigrationLedgerState.Running(checkpoint) => execute(database, step, checkpoint)
      case MigrationLedgerState.Absent              =>
        MongoMigrationLedger.markRunning(database, step.id) *> execute(database, step, None)
    }

  def runAll(database: SetupDatabase, steps: List[MongoMigrationStep]): IO[Unit] =
    steps.traverse_(run(database, _))

  /** `true` once the proof is complete and its re-verification passed; absent or running proofs are not trusted. */
  def trusted(database: SetupDatabase, step: MongoMigrationStep): IO[Boolean] =
    MongoMigrationLedger.read(database, step.id).flatMap {
      case MigrationLedgerState.Complete =>
        step.whenComplete(database).map {
          case CompletedProof.Trusted => true
          case CompletedProof.Reopen  => false
        }
      case _ => IO.pure(false)
    }

  private def execute(database: SetupDatabase, step: MongoMigrationStep, checkpoint: Option[AnyRef]): IO[Unit] =
    step.run(MigrationRun(database, step.id, checkpoint)) *> MongoMigrationLedger.markComplete(database, step.id)
}
