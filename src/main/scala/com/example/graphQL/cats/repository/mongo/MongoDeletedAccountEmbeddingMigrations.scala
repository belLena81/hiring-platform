package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Projections, Sorts, Updates}

/** Privacy maintenance: stop older writers before cutover. Removed fields are the replay checkpoint; aggregate
  * revisions deliberately remain unchanged because this operation only erases retained tombstone data.
  */
private[mongo] object MongoDeletedAccountEmbeddingMigrations {
  private val Id: MigrationId = MigrationIds.DeletedAccountEmbeddings

  /** Ledger literal kept as `String` for existing callers; the typed identity is
    * `MigrationIds.DeletedAccountEmbeddings`.
    */
  val MigrationId: String = Id.value
  private val BatchSize = 500

  private val affected = Filters.and(
    Filters.eq(MongoFields.AccountStatus, "Deleted"),
    Filters.or(Filters.exists(MongoFields.Embedding, true), Filters.exists(MongoFields.EmbeddingMeta, true))
  )

  /** A completed proof with reintroduced fields requires explicit maintenance repair, never a silent re-run. */
  private def verifyAbsent(run: MigrationRun): IO[Unit] =
    run.database
      .getCollection(MongoCollections.Users)
      .find(affected)
      .projection(Projections.include(MongoFields.Id))
      .limit(1)
      .first
      .flatMap {
        case None => IO.unit
        case _    => run.fail("deleted account embeddings remain; maintenance repair required")
      }

  private def erase(run: MigrationRun): IO[Unit] = {
    val users = run.database.getCollection(MongoCollections.Users)
    def batch: IO[Unit] =
      users
        .find(affected)
        .projection(Projections.include(MongoFields.Id))
        .sort(Sorts.ascending(MongoFields.Id))
        .limit(BatchSize)
        .all
        .flatMap { rows =>
          rows.toList.traverse_ { row =>
            Option(row.get(MongoFields.Id)).fold(run.fail[Unit]("tombstone identity is absent")) { id =>
              users
                .updateOne(
                  Filters.and(Filters.eq(MongoFields.Id, id), affected),
                  Updates.combine(Updates.unset(MongoFields.Embedding), Updates.unset(MongoFields.EmbeddingMeta))
                )
                .flatMap(result => IO.raiseUnless(result.wasAcknowledged())(MigrationError.UnacknowledgedWrite(run.id)))
            }
          } *> (if (rows.nonEmpty) IO.defer(batch) else IO.unit)
        }
    batch *> verifyAbsent(run)
  }

  val step: MongoMigrationStep = MongoMigrationStep(
    Id,
    erase,
    database => verifyAbsent(MigrationRun(database, Id, None)).as(CompletedProof.Trusted)
  )

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = MongoMigrationRunner.run(database, step)
}
