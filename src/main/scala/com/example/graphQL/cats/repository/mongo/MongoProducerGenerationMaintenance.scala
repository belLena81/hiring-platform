package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.infrastructure.kafka.KafkaProducerGenerationRetirement
import com.example.graphQL.cats.service.port.{RepositoryIO, RepositoryError}
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
    fence: String => RepositoryIO[Unit],
    currentTime: IO[Instant] = IO.realTimeInstant,
    diagnostics: Diagnostics = Diagnostics.noop
) {
  private val rows = Mongo4catsCollections.documents(database, MongoProducerRegistrations.Collection)

  private def boundary[A](effect: IO[A]): RepositoryIO[A] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "producerGenerationMaintenance")(RepositoryIO.lift(effect))

  private def text(row: Document, field: String): Either[RepositoryError, String] =
    Option(row.get(field))
      .collect { case value: String if value.nonEmpty => value }
      .toRight(RepositoryError.InvalidStoredData)

  /** Each invocation reads at most 64 registrations; IDs may recur on later pages for another subject. */
  def inventory(after: Option[String]): RepositoryIO[ProducerGenerationInventoryPage] = {
    val filters = MongoFilter.eq("state", "Active") :: after.toList.map(MongoFilter.gt("_id", _))
    boundary(
      rows.flatMap(
        _.find(MongoFilter.and(filters*).bson)
          .sort(Sorts.ascending("_id"))
          .limit(MongoProducerRegistrations.BatchSize)
          .all
      )
    ).flatMap { batch =>
      RepositoryIO.fromEither(
        batch.toVector
          .traverse { row =>
            for {
              cursor <- text(row, "_id")
              id <- text(row, "transactionalId")
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

  def retire(transactionalId: String): RepositoryIO[Unit] = {
    def checkpoint: RepositoryIO[Unit] =
      boundary(
        rows.flatMap(
          _.find(
            MongoFilter
              .and(
                MongoFilter.eq("transactionalId", transactionalId),
                MongoFilter.eq("state", "Active")
              )
              .bson
          ).sort(Sorts.ascending("_id")).limit(MongoProducerRegistrations.BatchSize).all
        )
      ).flatMap { batch =>
        if (batch.isEmpty) RepositoryIO.fromEither(Right(()))
        else
          for {
            ids <- RepositoryIO.fromEither(batch.toList.traverse(text(_, "_id")))
            now <- RepositoryIO.lift(currentTime)
            _ <- boundary(
              MongoSessionOperations.updateMany(
                rows,
                None,
                MongoFilter.and(
                  MongoFilter.eq("transactionalId", transactionalId),
                  MongoFilter.eq("state", "Active"),
                  MongoFilter.in("_id", ids)
                ),
                MongoUpdate.combine(
                  MongoUpdate.set("state", "Fenced"),
                  MongoUpdate.set("fencedAt", now.toDate),
                  MongoUpdate.set("expiresAt", Date.from(now.plusSeconds(MongoProducerRegistrations.RetentionSeconds)))
                )
              )
            )
            _ <- checkpoint
          } yield ()
      }
    RepositoryIO
      .fromEither(KafkaProducerGenerationRetirement.canonicalPrefix(transactionalId))
      .flatMap(_ => fence(transactionalId) *> checkpoint)
  }
}
