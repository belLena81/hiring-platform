package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.kafka.KafkaProducerGenerationRetirement
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import io.circe.Json

/** Inventory is read-only; retirement requires stopping the selected publisher before invoking its exact ID. */
object ProducerGenerationRetirementMain extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val operation: Option[Either[Option[String], String]] = args match {
      case "list" :: Nil                                                              => Some(Left(None))
      case "list" :: cursor :: Nil if cursor.nonEmpty && cursor.length <= 256         => Some(Left(Some(cursor)))
      case id :: Nil if KafkaProducerGenerationRetirement.canonicalPrefix(id).isRight => Some(Right(id))
      case _                                                                          => None
    }
    operation.fold(IO.pure(ExitCode.Error)) { selected =>
      AppConfig.load.flatMap {
        case Left(_)       => IO.pure(ExitCode.Error)
        case Right(config) =>
          (
            Resource.eval(SafeDiagnostics.configure(config.maskSensitive)),
            MongoDatabaseProbe.clientResource(config.mongoUri)
          ).tupled
            .use { case (diagnostics, client) =>
              client.getDatabase(config.mongoDatabase).flatMap { database =>
                val maintenance = new MongoProducerGenerationMaintenance(
                  database,
                  id => KafkaProducerGenerationRetirement.fence(config.kafka, id),
                  diagnostics = diagnostics
                )
                selected match {
                  case Left(cursor) =>
                    maintenance.inventory(cursor).value.flatMap {
                      case Left(_)     => IO.pure(ExitCode.Error)
                      case Right(page) =>
                        IO.println(
                          Json
                            .obj(
                              "transactionalIds" -> Json.arr(page.transactionalIds.map(Json.fromString)*),
                              "nextCursor" -> page.nextCursor.fold(Json.Null)(Json.fromString)
                            )
                            .noSpaces
                        ).as(ExitCode.Success)
                    }
                  case Right(id) =>
                    maintenance.retire(id).value.map {
                      case Right(_) => ExitCode.Success
                      case Left(_)  => ExitCode.Error
                    }
                }
              }
            }
            .handleError(_ => ExitCode.Error)
      }
    }
  }
}
