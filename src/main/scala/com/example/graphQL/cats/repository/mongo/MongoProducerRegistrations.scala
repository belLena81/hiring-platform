package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{RepositoryIO, RepositoryError}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import com.mongodb.client.model.{Sorts, UpdateOptions}
import java.time.Instant
import java.util.Date
import scala.concurrent.duration.*

/** Attributable generations are independent rows; only broker-confirmed fencing permits expiry. */
private[mongo] object MongoProducerRegistrations {
  val Collection = "producer_registrations"
  val ClaimCursorsCollection = "outbox_claim_cursors"
  val BatchSize = 64
  val RetentionSeconds = 8.days.toSeconds

  def register(
      database: MongoDatabase[IO],
      session: Option[ClientSession[IO]],
      subject: String,
      transactionalId: String,
      kind: String,
      now: Instant
  ): RepositoryIO[Unit] =
    RepositoryIO
      .lift(
        MongoSessionOperations.updateOne(
          Mongo4catsCollections.documents(database, Collection),
          session,
          MongoFilter.and(MongoFilter.eq("_id", s"$subject:$transactionalId"), MongoFilter.eq("state", "Active")),
          MongoUpdate.combine(
            MongoUpdate.setOnInsert("subjectId", subject),
            MongoUpdate.setOnInsert("transactionalId", transactionalId),
            MongoUpdate.setOnInsert("kind", kind),
            MongoUpdate.setOnInsert("state", "Active"),
            MongoUpdate.setOnInsert("registeredAt", Date.from(now))
          ),
          new UpdateOptions().upsert(true)
        )
      )
      .subflatMap {
        case Some(result) if result.wasAcknowledged() => Right(())
        case _                                        => Left(RepositoryError.MissingWriteResult)
      }

  def batch(database: MongoDatabase[IO], subject: String, kind: String): RepositoryIO[Vector[String]] =
    RepositoryIO
      .lift(
        Mongo4catsCollections
          .documents(database, Collection)
          .flatMap(
            _.find(
              MongoFilter
                .and(
                  MongoFilter.eq("subjectId", subject),
                  MongoFilter.eq("kind", kind),
                  MongoFilter.eq("state", "Active")
                )
                .bson
            ).sort(Sorts.ascending("_id")).limit(BatchSize).all
          )
      )
      .subflatMap(
        _.toVector.traverse(row =>
          Option(row.get("transactionalId"))
            .collect { case id: String => id }
            .toRight(RepositoryError.InvalidStoredData)
        )
      )

  /** Called only after the broker confirms every listed generation is fenced. Repeated marking is safe. */
  def markFenced(
      database: MongoDatabase[IO],
      subject: String,
      kind: String,
      ids: Vector[String],
      now: Instant
  ): RepositoryIO[Unit] =
    if (ids.isEmpty || ids.size > BatchSize) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      RepositoryIO
        .lift(
          MongoSessionOperations.updateMany(
            Mongo4catsCollections.documents(database, Collection),
            None,
            MongoFilter.and(
              MongoFilter.eq("subjectId", subject),
              MongoFilter.eq("kind", kind),
              MongoFilter.in("transactionalId", ids.toList)
            ),
            MongoUpdate.combine(
              MongoUpdate.set("state", "Fenced"),
              MongoUpdate.set("fencedAt", Date.from(now)),
              MongoUpdate.set("expiresAt", Date.from(now.plusSeconds(RetentionSeconds)))
            )
          )
        )
        .subflatMap {
          case Some(result) if result.wasAcknowledged() => Right(())
          case _                                        => Left(RepositoryError.MissingWriteResult)
        }
}
