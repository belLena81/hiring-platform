package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
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
  ): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.updateOne(
          Mongo4catsCollections.documents(database, Collection),
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, s"$subject:$transactionalId"),
            MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active)
          ),
          MongoUpdate.combine(
            MongoUpdate.setOnInsert(MongoFields.SubjectId, subject),
            MongoUpdate.setOnInsert(MongoFields.TransactionalId, transactionalId),
            MongoUpdate.setOnInsert(MongoFields.Kind, kind),
            MongoUpdate.setOnInsert(MongoFields.State, ProducerRegistrationValues.Active),
            MongoUpdate.setOnInsert("registeredAt", now.toDate)
          ),
          new UpdateOptions().upsert(true)
        )
      )
      .void

  def batch(database: MongoDatabase[IO], subject: String, kind: String): RepositoryF[IO, Vector[String]] =
    RepositoryF
      .lift(
        Mongo4catsCollections
          .documents(database, Collection)
          .flatMap(
            _.find(
              MongoFilter
                .and(
                  MongoFilter.eq(MongoFields.SubjectId, subject),
                  MongoFilter.eq(MongoFields.Kind, kind),
                  MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active)
                )
                .bson
            ).sort(Sorts.ascending(MongoFields.Id)).limit(BatchSize).all
          )
      )
      .subflatMap(
        _.toVector.traverse(row =>
          Option(row.get(MongoFields.TransactionalId))
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
  ): RepositoryF[IO, Unit] =
    if (ids.isEmpty || ids.size > BatchSize) RepositoryF.at[IO].raise(RepositoryError.InvalidStoredData)
    else
      RepositoryF
        .lift(
          MongoSessionOperations.updateMany(
            Mongo4catsCollections.documents(database, Collection),
            None,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.SubjectId, subject),
              MongoFilter.eq(MongoFields.Kind, kind),
              MongoFilter.in(MongoFields.TransactionalId, ids.toList)
            ),
            MongoUpdate.combine(
              MongoUpdate.set(MongoFields.State, ProducerRegistrationValues.Fenced),
              MongoUpdate.set(MongoFields.FencedAt, now.toDate),
              MongoUpdate.set("expiresAt", Date.from(now.plusSeconds(RetentionSeconds)))
            )
          )
        )
        .void
}
