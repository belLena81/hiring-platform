package com.example.graphQL.cats.repository.mongo

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
    val filters = MongoFilter.eq("state", "Active") :: after.toList.map(MongoFilter.gt("_id", _))
    boundary(
      rows.flatMap(
        _.find(MongoFilter.and(filters*).bson)
          .sort(Sorts.ascending("_id"))
          .limit(MongoProducerRegistrations.BatchSize)
          .all
      )
    ).flatMap { batch =>
      RepositoryF
        .at[IO]
        .fromEither(
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

  def retire(transactionalId: String): RepositoryF[IO, Unit] = {
    def checkpoint: RepositoryF[IO, Unit] =
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
        if (batch.isEmpty) ().pure[[A] =>> RepositoryF[IO, A]]
        else
          for {
            ids <- RepositoryF.at[IO].fromEither(batch.toList.traverse(text(_, "_id")))
            now <- RepositoryF.lift(currentTime)
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
    RepositoryF
      .at[IO]
      .fromEither(KafkaProducerGenerationRetirement.canonicalPrefix(transactionalId))
      .flatMap(_ => fence(transactionalId) *> checkpoint)
  }
}
