package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.kafka.KafkaProducerGenerationRetirement
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.runtime.HiringLogValues
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields, RepositoryError}
import io.circe.Json

/** Inventory is read-only; retirement requires stopping the selected publisher before invoking its exact ID. */
object ProducerGenerationRetirementMain extends IOApp {
  private def failed(diagnostics: Diagnostics[IO])(error: RepositoryError): IO[ExitCode] =
    diagnostics.emit(LogEvent.MongoRepositoryFailed, fields = LogFields.repositoryFailure(error)).as(ExitCode.Error)

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
            Resource.eval(SafeDiagnostics.configure(config.maskSensitive, HiringLogValues.allowed)),
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
                    maintenance
                      .inventory(cursor)
                      .foldF(
                        failed(diagnostics),
                        page =>
                          IO.println(
                            Json
                              .obj(
                                "transactionalIds" -> Json.arr(page.transactionalIds.map(Json.fromString)*),
                                "generations" -> Json.arr(
                                  page.generations.map(generation =>
                                    Json.obj(
                                      "transactionalId" -> Json.fromString(generation.transactionalId),
                                      "generationId" -> Json.fromString(generation.generationId.value.toString)
                                    )
                                  )*
                                ),
                                "nextCursor" -> page.nextCursor.fold(Json.Null)(Json.fromString)
                              )
                              .noSpaces
                          ).as(ExitCode.Success)
                      )
                  case Right(id) =>
                    maintenance.retire(id).foldF(failed(diagnostics), _ => IO.pure(ExitCode.Success))
                }
              }
            }
            .handleError(_ => ExitCode.Error)
      }
    }
  }
}
