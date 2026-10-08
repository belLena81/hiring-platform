package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.auth.AuthenticationFingerprint
import com.example.graphQL.cats.service.port.MutationReceiptFingerprint
import com.mongodb.{MongoWriteException, ReadConcern, ReadPreference, WriteConcern}
import com.mongodb.client.model.{Filters, Projections, Sorts, UpdateOptions, Updates}
import com.mongodb.client.result.UpdateResult
import mongo4cats.database.MongoDatabase
import org.bson.Document

/** Stop old account writers before running. Only authentication fingerprints change; receipt identities and outcomes
  * survive. The running ledger pins the derived key so restart cannot produce mixed-key migrated receipts.
  */
object MongoAuthenticationReceiptMigration {
  val MigrationId = "017_authentication_receipt_protection"
  private val BatchSize = 500
  private val Tag = "hmac-sha256:"
  private val Operations = List("login", "signUp", "bootstrapAdmin")
  private def hex(value: String): Boolean = value.matches("[0-9a-f]{64}")
  private def protectedDigest(value: String): Boolean = value.startsWith(Tag) && hex(value.stripPrefix(Tag))
  private def failure: IllegalStateException = new IllegalStateException(
    "Authentication receipt migration requires repair"
  )
  private def text(row: Document, field: String): IO[String] =
    IO.fromOption(Option(row.get(field)).collect { case value: String if value.nonEmpty => value })(failure)

  def initialize(database: MongoDatabase[IO], fingerprints: AuthenticationFingerprint): IO[Unit] = for {
    receipts <- Mongo4catsCollections
      .documents(database, MongoCollections.MutationReceipts)
      .map(
        _.withReadPreference(ReadPreference.primary())
          .withReadConcern(ReadConcern.MAJORITY)
          .withWriteConcern(WriteConcern.MAJORITY.withJournal(true))
      )
    ledger <- Mongo4catsCollections
      .documents(database, MongoCollections.HiringMigrationLedger)
      .map(
        _.withReadPreference(ReadPreference.primary())
          .withReadConcern(ReadConcern.MAJORITY)
          .withWriteConcern(WriteConcern.MAJORITY.withJournal(true))
      )
    marker = Filters.eq("_id", MigrationId)
    running = Filters.and(
      marker,
      Filters.eq("state", "Running"),
      Filters.eq("keyId", fingerprints.keyId),
      Filters.eq("version", Long.box(1L))
    )
    auth = Filters.in("operation", Operations*)
    _ <- {
      def acknowledged(result: UpdateResult): IO[Unit] = IO.raiseUnless(result.wasAcknowledged())(failure)
      def state: IO[(Boolean, Option[String])] = ledger.find(marker).first.flatMap {
        case None      => IO.raiseError(failure)
        case Some(row) =>
          for {
            version <- IO.fromOption(Option(row.get("version")).collect { case value: java.lang.Long =>
              value.longValue
            })(failure)
            _ <- IO.raiseUnless(version == 1L)(failure)
            keyId <- text(row, "keyId")
            _ <- IO.raiseUnless(hex(keyId))(failure)
            status <- text(row, "state")
            after <- if (row.containsKey("lastId")) text(row, "lastId").map(Some(_)) else IO.pure(None)
            complete <- status match {
              case "Running"                   => IO.raiseUnless(keyId == fingerprints.keyId)(failure).as(false)
              case "Complete" if after.isEmpty => IO.pure(true)
              case _                           => IO.raiseError(failure)
            }
          } yield (complete, after)
      }
      def process(row: Document, convert: Boolean): IO[Unit] = for {
        id <- text(row, "_id")
        operation <- text(row, "operation")
        scope <- text(row, "actorScope")
        digest <- text(row, "fingerprint")
        _ <- IO.raiseUnless(Operations.contains(operation))(failure)
        _ <-
          if (protectedDigest(digest)) IO.unit
          else if (!convert || !hex(digest)) IO.raiseError(failure)
          else {
            val replacement = fingerprints.protect(operation, scope, MutationReceiptFingerprint.stored(digest)).value
            val guard = Filters.and(
              Filters.eq("_id", id),
              Filters.eq("operation", operation),
              Filters.eq("actorScope", scope),
              Filters.eq("fingerprint", digest)
            )
            receipts.updateOne(guard, Updates.set("fingerprint", replacement)).flatMap { result =>
              acknowledged(result) *> (if (result.getMatchedCount == 1L) IO.unit
                                       else
                                         receipts.find(Filters.eq("_id", id)).first.flatMap {
                                           case None =>
                                             IO.unit // Expired receipts may disappear concurrently through TTL.
                                           case Some(current) =>
                                             IO.raiseUnless(
                                               current.get("operation") == operation && current.get(
                                                 "actorScope"
                                               ) == scope && current.get("fingerprint") == replacement
                                             )(failure)
                                         })
            }
          }
      } yield ()
      def scan(after: Option[String], convert: Boolean): IO[Unit] = {
        val filter = after.fold(auth)(id => Filters.and(auth, Filters.gt("_id", id)))
        receipts
          .find(filter)
          .projection(Projections.include("_id", "operation", "actorScope", "fingerprint"))
          .sort(Sorts.ascending("_id"))
          .limit(BatchSize)
          .boundedStream(32)
          .take(BatchSize.toLong)
          .compile
          .toList
          .flatMap { rows =>
            rows.traverse_(process(_, convert)) *> rows.lastOption.traverse_ { last =>
              text(last, "_id").flatMap { id =>
                val checkpoint =
                  if (!convert) IO.unit
                  else
                    ledger
                      .updateOne(running, Updates.max("lastId", id))
                      .flatMap(result =>
                        acknowledged(
                          result
                        ) *> (if (result.getMatchedCount == 1L) IO.unit
                              else state.flatMap { case (complete, _) => IO.raiseUnless(complete)(failure) })
                      )
                checkpoint *> (if (rows.size == BatchSize) IO.defer(scan(Some(id), convert)) else IO.unit)
              }
            }
          }
      }
      def resume: IO[Unit] = state.flatMap {
        case (true, _)      => scan(None, convert = false)
        case (false, after) =>
          scan(after, convert = true) *> scan(None, convert = false) *>
            ledger
              .updateOne(running, Updates.combine(Updates.set("state", "Complete"), Updates.unset("lastId")))
              .flatMap(acknowledged) *> state.flatMap { case (complete, _) => IO.raiseUnless(complete)(failure) }
      }
      ledger
        .updateOne(
          marker,
          Updates.combine(
            Updates.setOnInsert("version", Long.box(1L)),
            Updates.setOnInsert("state", "Running"),
            Updates.setOnInsert("keyId", fingerprints.keyId)
          ),
          new UpdateOptions().upsert(true)
        )
        .flatMap(acknowledged)
        .handleErrorWith {
          case error: MongoWriteException if error.getError.getCode == 11000 => IO.unit
          case error                                                         => IO.raiseError(error)
        } *> resume
    }
  } yield ()
}
