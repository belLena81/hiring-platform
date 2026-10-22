package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.MongoF.asRepository
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.infrastructure.kafka.KafkaProducerGenerationRetirement
import com.example.graphQL.cats.service.port.{RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Sorts
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.time.Instant
import java.util.Date

final case class ProducerGenerationInventoryPage(transactionalIds: Vector[String], nextCursor: Option[String])

/** Explicit maintenance for stopped writers. Active registrations expire only after broker-confirmed fencing. */
final class MongoProducerGenerationMaintenance(
    database: MongoDatabase[IO],
    fence: String => RepositoryF[IO, Unit],
    currentTime: IO[Instant] = IO.realTimeInstant,
    diagnostics: Diagnostics[IO] = Diagnostics.noop
) {
  private val rows = Mongo4catsCollections.documents(database, MongoProducerRegistrations.Collection)

  private def boundary[A](effect: IO[A]): RepositoryF[IO, A] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "producerGenerationMaintenance")(RepositoryF.lift(effect))

  private def text(row: Document, field: String): Either[RepositoryError, String] =
    Option(row.get(field))
      .collect { case value: String if value.nonEmpty => value }
      .toRight(RepositoryError.InvalidStoredData)

  /** Each invocation reads at most 64 registrations; IDs may recur on later pages for another subject. */
  def inventory(after: Option[String]): RepositoryF[IO, ProducerGenerationInventoryPage] = {
    val filters = MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active) :: after.toList.map(
      MongoFilter.gt(MongoFields.Id, _)
    )
    boundary(
      rows.flatMap(
        _.find(MongoFilter.and(filters*).bson)
          .sort(Sorts.ascending(MongoFields.Id))
          .limit(MongoProducerRegistrations.BatchSize)
          .all
      )
    ).flatMap { batch =>
      MongoF.fromEither(
        batch.toVector
          .traverse { row =>
            for {
              cursor <- text(row, MongoFields.Id)
              id <- text(row, MongoFields.TransactionalId)
              _ <- KafkaProducerGenerationRetirement.canonicalPrefix(id)
            } yield cursor -> id
          }
          .map { decoded =>
            ProducerGenerationInventoryPage(
              decoded.map(_._2).distinct,
              if (decoded.size == MongoProducerRegistrations.BatchSize) decoded.lastOption.map(_._1) else None
            )
          }
      )
    }
  }

  def retire(transactionalId: String): RepositoryF[IO, Unit] = {
    def checkpoint: RepositoryF[IO, Unit] =
      boundary(
        rows.flatMap(
          _.find(
            MongoFilter
              .and(
                MongoFilter.eq(MongoFields.TransactionalId, transactionalId),
                MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active)
              )
              .bson
          ).sort(Sorts.ascending(MongoFields.Id)).limit(MongoProducerRegistrations.BatchSize).all
        )
      ).flatMap { batch =>
        if (batch.isEmpty) ().asRepository
        else
          for {
            ids <- MongoF.fromEither(batch.toList.traverse(text(_, MongoFields.Id)))
            now <- RepositoryF.lift(currentTime)
            _ <- boundary(
              MongoSessionOperations.updateMany(
                rows,
                None,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.TransactionalId, transactionalId),
                  MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active),
                  MongoFilter.in(MongoFields.Id, ids)
                ),
                MongoUpdate.combine(
                  MongoUpdate.set(MongoFields.State, ProducerRegistrationValues.Fenced),
                  MongoUpdate.set(MongoFields.FencedAt, now.toDate),
                  MongoUpdate.set("expiresAt", Date.from(now.plusSeconds(MongoProducerRegistrations.RetentionSeconds)))
                )
              )
            )
            _ <- checkpoint
          } yield ()
      }
    MongoF
      .fromEither(KafkaProducerGenerationRetirement.canonicalPrefix(transactionalId))
      .flatMap(_ => fence(transactionalId) *> checkpoint)
  }
}
