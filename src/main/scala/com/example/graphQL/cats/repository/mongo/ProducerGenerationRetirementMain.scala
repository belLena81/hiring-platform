package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, IOApp, ExitCode}
import cats.syntax.all.*
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.kafka.KafkaProducerGenerationRetirement
import com.example.graphQL.cats.service.port.{RepositoryIO, RepositoryError}
import com.mongodb.client.model.Sorts
import java.util.Date

/** Operator-only maintenance command. The selected generation must be retired; a live publisher will be fenced. */
object ProducerGenerationRetirementMain extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = args match {
    case id :: Nil =>
      AppConfig.load.flatMap {
        case Left(_)       => IO.pure(ExitCode.Error)
        case Right(config) =>
          MongoDatabaseProbe.clientResource(config.mongoUri).use { client =>
            client.getDatabase(config.mongoDatabase).flatMap { database =>
              val rows = Mongo4catsCollections.documents(database, MongoProducerRegistrations.Collection)
              def retire: RepositoryIO[Unit] = RepositoryIO
                .lift(
                  rows.flatMap(
                    _.find(
                      MongoFilter.and(MongoFilter.eq("transactionalId", id), MongoFilter.eq("state", "Active")).bson
                    )
                      .sort(Sorts.ascending("_id"))
                      .limit(MongoProducerRegistrations.BatchSize)
                      .all
                  )
                )
                .flatMap { batch =>
                  if (batch.isEmpty) RepositoryIO.fromEither(Right(()))
                  else
                    for {
                      now <- RepositoryIO.lift(IO.realTimeInstant)
                      result <- RepositoryIO.lift(
                        MongoSessionOperations.updateMany(
                          rows,
                          None,
                          MongoFilter.in("_id", batch.toList.map(_.getString("_id"))),
                          MongoUpdate.combine(
                            MongoUpdate.set("state", "Fenced"),
                            MongoUpdate.set("fencedAt", Date.from(now)),
                            MongoUpdate
                              .set("expiresAt", Date.from(now.plusSeconds(MongoProducerRegistrations.RetentionSeconds)))
                          )
                        )
                      )
                      _ <- RepositoryIO.fromEither(
                        Either.cond(result.exists(_.wasAcknowledged()), (), RepositoryError.MissingWriteResult)
                      )
                      _ <- retire
                    } yield ()
                }
              (KafkaProducerGenerationRetirement.fence(config.kafka, id) *> retire).value.map {
                case Right(_) => ExitCode.Success
                case Left(_)  => ExitCode.Error
              }
            }
          }
      }
    case _ => IO.pure(ExitCode.Error)
  }
}
