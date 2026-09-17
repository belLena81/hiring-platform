package com.example.graphQL.cats.runtime

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLServices
import com.example.graphQL.cats.application.{DatabaseProbe, Diagnostics, ProbeResult}
import com.example.graphQL.cats.application.service.{ApplicationService, JobService}
import com.example.graphQL.cats.infrastructure.mongo.{
  MongoApplicationRepository, MongoDatabaseProbe, MongoHiringSetup, MongoJobRepository, MongoUserRepository
}

final case class MongoHiringRuntime(
    probe: DatabaseProbe,
    services: HiringGraphQLServices,
    ensureSetup: IO[Boolean]
)

object MongoHiringRuntime {
  def resource(uri: String, databaseName: String, diagnostics: Diagnostics): Resource[IO, MongoHiringRuntime] =
    MongoDatabaseProbe.clientResource(uri).evalMap { client =>
      val database = client.getDatabase(databaseName)
      (Ref.of[IO, Boolean](false), Semaphore[IO](1)).mapN { (setupComplete, setupLock) =>
        val users = new MongoUserRepository(database)
        val jobs = new MongoJobRepository(database)
        val applications = MongoApplicationRepository.transactional(database, client)
        val services = HiringGraphQLServices(
          users,
          jobs,
          applications,
          JobService[IO](users, jobs),
          ApplicationService[IO](users, jobs, applications)
        )
        val setup = ensureSetup(database, setupComplete, setupLock)
        MongoHiringRuntime(probe(uri, databaseName, diagnostics, setup), services, setup)
      }
    }

  private def probe(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      setup: IO[Boolean]
  ): DatabaseProbe = new DatabaseProbe {
    override def check: IO[ProbeResult] =
      check(None)

    override def check(requestId: Option[String]): IO[ProbeResult] =
      MongoDatabaseProbe.resource(uri, databaseName, diagnostics).use(_.check(requestId)).flatMap {
        case ProbeResult.Ready => setup.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
        case other => IO.pure(other)
      }
  }

  private def ensureSetup(
      database: com.mongodb.reactivestreams.client.MongoDatabase,
      done: Ref[IO, Boolean],
      lock: Semaphore[IO]
  ): IO[Boolean] =
    done.get.flatMap {
      case true => IO.pure(true)
      case false =>
        lock.permit.use { _ =>
          done.get.flatMap {
            case true => IO.pure(true)
            case false => MongoHiringSetup.initialize(database).attempt.flatMap {
              case Right(()) => done.set(true).as(true)
              case Left(_) => IO.pure(false)
            }
          }
        }
    }
}
