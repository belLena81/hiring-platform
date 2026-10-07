package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Projections, Sorts, UpdateOptions, Updates}
import com.mongodb.client.result.UpdateResult
import org.bson.Document

/** Privacy maintenance: stop older writers before cutover. Removed fields are the replay checkpoint; aggregate
  * revisions deliberately remain unchanged because this operation only erases retained tombstone data.
  */
private[mongo] object MongoDeletedAccountEmbeddingMigrations {
  val MigrationId = "014_deleted_account_embeddings"
  private val BatchSize = 500

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = {
    val users = database.getCollection(MongoCollections.Users)
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val marker = Filters.eq("_id", MigrationId)
    val affected = Filters.and(
      Filters.eq("accountStatus", "Deleted"),
      Filters.or(Filters.exists("embedding", true), Filters.exists("embeddingMeta", true))
    )

    def acknowledged(result: UpdateResult): IO[Unit] =
      if (result.wasAcknowledged()) IO.unit
      else IO.raiseError(new IllegalStateException("Deleted account embedding migration write was not acknowledged"))

    def state(row: Document): IO[String] =
      (Option(row.get("version")), Option(row.get("state"))) match {
        case (Some(version: java.lang.Long), Some(value: String))
            if version.longValue() == 1L && Set("Running", "Complete").contains(value) =>
          IO.pure(value)
        case _ => IO.raiseError(new IllegalStateException("Unsupported deleted account embedding migration ledger"))
      }

    def verifyAbsent: IO[Unit] =
      users.find(affected).projection(Projections.include("_id")).limit(1).first.flatMap {
        case None => IO.unit
        case _    =>
          IO.raiseError(new IllegalStateException("Deleted account embeddings remain; maintenance repair required"))
      }

    def verifyComplete: IO[Unit] = ledger.find(marker).first.flatMap {
      case Some(row) =>
        state(row).flatMap {
          case "Complete" => verifyAbsent
          case _ => IO.raiseError(new IllegalStateException("Deleted account embedding migration did not complete"))
        }
      case _ => IO.raiseError(new IllegalStateException("Deleted account embedding migration ledger is absent"))
    }

    def batch: IO[Unit] =
      users
        .find(affected)
        .projection(Projections.include("_id"))
        .sort(Sorts.ascending("_id"))
        .limit(BatchSize)
        .all
        .flatMap { rows =>
          rows.toList.traverse_ { row =>
            Option(row.get("_id")).fold(
              IO.raiseError[Unit](new IllegalStateException("Deleted account embedding migration identity is absent"))
            ) { id =>
              users
                .updateOne(
                  Filters.and(Filters.eq("_id", id), affected),
                  Updates.combine(Updates.unset("embedding"), Updates.unset("embeddingMeta"))
                )
                .flatMap(acknowledged)
            }
          } *> (if (rows.nonEmpty) IO.defer(batch) else IO.unit)
        }

    def run: IO[Unit] = batch *> verifyAbsent *>
      ledger
        .updateOne(
          Filters.and(marker, Filters.eq("version", Long.box(1L)), Filters.eq("state", "Running")),
          Updates.set("state", "Complete")
        )
        .flatMap(acknowledged) *> verifyComplete

    def resume: IO[Unit] = ledger.find(marker).first.flatMap {
      case Some(row) =>
        state(row).flatMap {
          case "Complete" => verifyAbsent
          case _          => run
        }
      case _ => IO.raiseError(new IllegalStateException("Deleted account embedding migration ledger is absent"))
    }

    ledger.find(marker).first.flatMap {
      case Some(_) => resume
      case None    =>
        ledger
          .updateOne(
            marker,
            Updates.combine(Updates.setOnInsert("version", Long.box(1L)), Updates.setOnInsert("state", "Running")),
            new UpdateOptions().upsert(true)
          )
          .flatMap(acknowledged)
          .handleErrorWith {
            case error: MongoWriteException if error.getError.getCode == 11000 => IO.unit
            case error                                                         => IO.raiseError(error)
          } *> resume
    }
  }
}
