package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.AppConfigFixtures
import com.example.graphQL.cats.domain.model.{CandidateProfile, RecruiterProfile, UserProfile, UserRole}
import com.example.graphQL.cats.runtime.MongoHiringRuntime
import com.example.graphQL.cats.service.{AccountError, Diagnostics, UseCaseError}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, SignUpInput}
import com.mongodb.client.model.{Filters, InsertOneOptions, UpdateOptions, Updates}
import io.circe.Json
import org.bson.{BsonNull, Document}

import java.util.UUID
import scala.concurrent.duration.*

/** Executes account registration through the normal resource-owned runtime and transactional mutation receipts. */
final class MongoAccountRegistrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private def replicaSet = mongoResource

  private def configuration(
      instance: MongoAccessEvaluationSupport.Fixture,
      database: String
  ): IO[MongoHiringRuntime.RuntimeConfig] = {
    val uri = instance.uri
    val raw = s"""include classpath("application.conf")
                 |http.host="127.0.0.1"
                 |http.port=8080
                 |mongo.uri=${Json.fromString(uri).noSpaces}
                 |mongo.database=${Json.fromString(database).noSpaces}
                 |mongo.reset-on-start=false
                 |auth.jwt.hs256-secret="synthetic-account-registration-key-material"
                 |kafka.enabled=false
                 |vector-search.enabled=false
                 |""".stripMargin
    IO.fromEither(
      AppConfigFixtures
        .fromConfig(raw, Map.empty)
        .leftMap(_ => new AssertionError("Account-registration config rejected"))
    ).map(config =>
      MongoHiringRuntime.RuntimeConfig(
        config.mongoUri,
        config.mongoDatabase,
        Diagnostics.noop,
        config.vectorSearch,
        config.jwtAuth,
        config.passwordHash,
        config.kafka,
        resetOnStart = false
      )
    )
  }

  private def request(value: String): IdempotencyRequest =
    IdempotencyRequest.fromCanonicalInput(UUID.randomUUID(), value)

  test("startup Admin seed enables registration and repeats without changing credentials") {
    replicaSet.use { instance =>
      for {
        base <- configuration(instance, instance.database.underlying.getName)
        seed = com.example.graphQL.cats.config
          .AdminSeedConfig(true, Some("Synthetic admin"), Some("synthetic-admin-password"))
        config = base.copy(adminSeed = seed)
        _ <- MongoHiringRuntime.resource(config).use { runtime =>
          for {
            candidate <- runtime.services.accountService
              .signUp(
                request("candidate"),
                SignUpInput(
                  "Synthetic candidate",
                  UserRole.Candidate,
                  "synthetic-candidate-password",
                  Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
                )
              )
              .value
            recruiter <- runtime.services.accountService
              .signUp(
                request("recruiter"),
                SignUpInput(
                  "Synthetic recruiter",
                  UserRole.Recruiter,
                  "synthetic-recruiter-password",
                  Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None)))
                )
              )
              .value
            _ <- IO { assert(candidate.isRight); assert(recruiter.isRight) }
          } yield ()
        }
        _ <- MongoHiringRuntime
          .resource(config.copy(adminSeed = seed.copy(password = Some("changed-password-unused"))))
          .use { runtime =>
            for {
              old <- runtime.services.accountService
                .login(
                  request("old-password"),
                  com.example.graphQL.cats.service.protocol.LoginInput("Synthetic admin", "synthetic-admin-password")
                )
                .value
              changed <- runtime.services.accountService
                .login(
                  request("changed-password"),
                  com.example.graphQL.cats.service.protocol.LoginInput("Synthetic admin", "changed-password-unused")
                )
                .value
              _ <- IO {
                assert(old.exists(_._1.role == UserRole.Admin));
                assertEquals(changed, Left(UseCaseError.Account(AccountError.InvalidCredentials)))
              }
            } yield ()
          }
        conflict <- MongoHiringRuntime
          .resource(config.copy(adminSeed = seed.copy(name = Some("Another admin"))))
          .use(_ => IO.unit)
          .attempt
        _ <- IO(assert(conflict.isLeft))
      } yield ()
    }
  }

  test("missing, null and unknown persisted account initialization states remain closed") {
    replicaSet.use { instance =>
      for {
        config <- configuration(instance, instance.database.underlying.getName)
        _ <- MongoDatabaseProbe.clientResource(config.uri).use { client =>
          client.getDatabase(config.databaseName).flatMap { database =>
            val users =
              MongoUserRepository.transactional(database, client, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop)
            val filter = Filters.eq(MongoFields.Id, "user-account-registry")
            for {
              absent <- users.initialized.value
              registry <- Mongo4catsCollections.documents(database, MongoCollections.AccountRegistry)
              _ <- registry.insertOne(
                new Document(MongoFields.Id, "user-account-registry").append(MongoFields.State, BsonNull.VALUE),
                new InsertOneOptions()
              )
              nullState <- users.initialized.value
              _ <- registry.updateOne(filter, Updates.unset(MongoFields.State), new UpdateOptions())
              absentState <- users.initialized.value
              _ <- registry.updateOne(filter, Updates.set(MongoFields.State, "Unknown"), new UpdateOptions())
              unknownState <- users.initialized.value
              _ <- IO {
                assertEquals(absent, Right(false))
                assertEquals(nullState, Right(false))
                assertEquals(absentState, Right(false))
                assertEquals(unknownState, Right(false))
              }
            } yield ()
          }
        }
      } yield ()
    }
  }
}
