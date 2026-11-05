package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.MongoF.asRepository
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.infrastructure.kafka.KafkaProducerGenerationRetirement
import com.example.graphQL.cats.service.port.{ProducerGeneration, RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Sorts
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*
import java.time.Instant

final case class ProducerGenerationInventoryPage(generations: Vector[ProducerGeneration], nextCursor: Option[String]) {
  def transactionalIds: Vector[String] = generations.map(_.transactionalId).distinct
}

private[mongo] object MongoProducerGenerationMaintenance {

  /** The Active rows of one transactional id registered strictly before the cut. */
  def retireFilter(transactionalId: String, cut: Instant): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.TransactionalId, transactionalId),
      MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active),
      MongoFilter.lt(MongoProducerRegistrations.RegisteredAt, cut.toDate)
    )

  /** The order of the `producer_registration_generation_state` index after its equality prefix. */
  val RetireSort: Bson = Sorts.ascending(MongoProducerRegistrations.RegisteredAt, MongoFields.Id)
}

/** Explicit maintenance for stopped writers. Active registrations expire only after broker-confirmed fencing. */
final class MongoProducerGenerationMaintenance(
    database: MongoDatabase[IO],
    fence: String => RepositoryF[IO, Unit],
    diagnostics: Diagnostics[IO] = Diagnostics.noop
) {
  private val rows = Mongo4catsCollections.documents(database, MongoProducerRegistrations.Collection)

  private def boundary[A](effect: IO[A]): RepositoryF[IO, A] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "producerGenerationMaintenance")(RepositoryF.lift(effect))

  /** The database clock: the clock every `registeredAt` is stamped with, so the cut-off compares like with like. */
  private val databaseTime: RepositoryF[IO, Instant] =
    boundary(
      database
        .runCommand(
          new Document("aggregate", 1)
            .append(
              "pipeline",
              List(
                new Document("$documents", List(new Document()).asJava),
                new Document("$project", new Document("_id", 0).append("now", "$$NOW"))
              ).asJava
            )
            .append("cursor", new Document())
        )
        .flatMap(result =>
          IO.fromOption(
            scala.util.Try {
              val batch = org.bson.BsonDocument.parse(result.toJson).getDocument("cursor").getArray("firstBatch")
              Instant.ofEpochMilli(batch.get(0).asDocument().getDateTime("now").getValue)
            }.toOption
          )(new IllegalStateException("database clock unavailable"))
        )
    )

  private def text(row: Document, field: String): Either[RepositoryError, String] =
    Option(row.get(field))
      .collect { case value: String if value.nonEmpty => value }
      .toRight(RepositoryError.InvalidStoredData)

  /** Each invocation reads at most 64 registrations; a transactional id recurs on later pages for another subject or
    * generation.
    */
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
              generation <- MongoProducerRegistrations.generationOf(row)
              _ <- KafkaProducerGenerationRetirement.canonicalPrefix(generation.transactionalId)
            } yield cursor -> generation
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

  /** Retires the generations of a stopped worker id. The cut is the database clock, read before the first fence and
    * compared with `registeredAt`, which is stamped by the same clock. A generation registers only after its producer
    * initialized, so every Active row registered before the cut belongs to a producer that the broker fence (issued
    * after the cut) has fenced; the Active rows visible to the scan whose `registeredAt` precedes the cut are then
    * marked Fenced in bounded batches, guarded by state Active, with `fencedAt` and `expiresAt` from the same database
    * clock. Rows registered after the cut belong to a successor generation and stay Active, so the loop ends even while
    * a replacement is running; so does a row registered inside a transaction that commits after the scan (the safe
    * direction: it stays Active and a later retirement picks it up). A failed fence marks nothing and the work stays
    * pending.
    */
  def retire(transactionalId: String): RepositoryF[IO, Unit] = {
    def mark(cut: Instant): RepositoryF[IO, Unit] =
      boundary(
        rows.flatMap(
          _.find(MongoProducerGenerationMaintenance.retireFilter(transactionalId, cut).bson)
            .sort(MongoProducerGenerationMaintenance.RetireSort)
            .limit(MongoProducerRegistrations.BatchSize)
            .all
        )
      ).flatMap { batch =>
        if (batch.isEmpty) ().asRepository
        else
          for {
            ids <- MongoF.fromEither(batch.toList.traverse(text(_, MongoFields.Id)))
            _ <- MongoF.fromEither(batch.toList.traverse(MongoProducerRegistrations.generationOf))
            _ <- boundary(
              MongoSessionOperations
                .updateManyPipeline(
                  rows,
                  None,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.TransactionalId, transactionalId),
                    MongoFilter.eq(MongoFields.State, ProducerRegistrationValues.Active),
                    MongoFilter.in(MongoFields.Id, ids)
                  ),
                  MongoProducerRegistrations.fencedStage
                )
            )
            _ <- mark(cut)
          } yield ()
      }
    for {
      _ <- MongoF.fromEither(KafkaProducerGenerationRetirement.canonicalPrefix(transactionalId))
      cut <- databaseTime
      _ <- fence(transactionalId)
      _ <- mark(cut)
    } yield ()
  }
}
