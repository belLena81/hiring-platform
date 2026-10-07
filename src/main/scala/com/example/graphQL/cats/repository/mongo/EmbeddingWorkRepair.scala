package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.domain.model.Identifiers.{UserId, parse}
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.{EmbeddingWorkKey, EmbeddingWorkKind}

/** Explicit local maintenance entry point; CAS and live singleton Admin authority are enforced by MongoDB. */
object EmbeddingWorkRepair extends IOApp {
  private def request(args: List[String]): Either[String, (UserId, EmbeddingWorkKey, Option[Long])] = args match {
    case admin :: kind :: entity :: tail if tail.isEmpty || tail.size == 1 =>
      for {
        actor <- parse(admin)(UserId.apply).leftMap(_ => "Invalid Admin identifier")
        selected <- EmbeddingWorkKind.values.find(_.toString == kind).toRight("Invalid work kind")
        id <- parse(entity)(UserId.apply).leftMap(_ => "Invalid entity identifier")
        expected <- tail.headOption.traverse(
          _.toLongOption
            .filter(value => value >= 1L && value < Long.MaxValue)
            .toRight("Invalid expected generation")
        )
      } yield (actor, EmbeddingWorkKey(selected, id.value.toString), expected)
    case _ =>
      Left(
        "Usage: EmbeddingWorkRepair <admin-id> <Job|CandidateProfile> <entity-id> [expected-generation; omit to inspect]"
      )
  }

  def run(args: List[String]): IO[ExitCode] = request(args) match {
    case Left(message)                   => IO.println(message).as(ExitCode.Error)
    case Right((admin, key, generation)) =>
      AppConfig.load.flatMap {
        case Left(_)       => IO.println("Invalid application configuration").as(ExitCode.Error)
        case Right(config) =>
          MongoDatabaseProbe
            .clientResource(config.mongoUri)
            .use { client =>
              for {
                db <- client.getDatabase(config.mongoDatabase)
                now <- IO.realTimeInstant
                repository = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
                transactions = MongoTransactionRunner.sessions(
                  client,
                  RepositoryError.Conflict,
                  diagnostics = Diagnostics.noop
                )
                code <- generation.fold(
                  repository.inspectForAdmin(key, admin, transactions).value.flatMap {
                    case Right(Some(status)) =>
                      IO.println(
                        s"generation=${status.generation} state=${status.state} attempts=${status.attempts} failure=${status.failure}"
                      ).as(ExitCode.Success)
                    case Right(None) => IO.println("No durable work").as(ExitCode.Success)
                    case Left(_)     => IO.println("Inspection denied or unavailable").as(ExitCode.Error)
                  }
                )(expected =>
                  repository.repairFailed(key, expected, admin, now, transactions).value.flatMap {
                    case Right(true)  => IO.println("Failed work queued with a new generation").as(ExitCode.Success)
                    case Right(false) => IO.println("No matching failed generation").as(ExitCode.Error)
                    case Left(_)      => IO.println("Repair denied or unavailable").as(ExitCode.Error)
                  }
                )
              } yield code
            }
            .handleErrorWith(_ => IO.println("Repair unavailable").as(ExitCode.Error))
      }
  }
}
