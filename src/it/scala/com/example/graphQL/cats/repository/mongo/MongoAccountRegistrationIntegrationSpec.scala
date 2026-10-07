package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.domain.model.{CandidateProfile, RecruiterProfile, UserProfile, UserRole}
import com.example.graphQL.cats.runtime.MongoHiringRuntime
import com.example.graphQL.cats.service.{AccountError, Diagnostics, UseCaseError}
import com.example.graphQL.cats.service.protocol.{BootstrapAdminInput, IdempotencyRequest, SignUpInput}
import com.mongodb.client.model.{Filters, InsertOneOptions, UpdateOptions, Updates}
import io.circe.Json
import org.bson.{BsonDocument, BsonNull, Document}

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
      AppConfig.fromConfig(raw, Map.empty).leftMap(_ => new AssertionError("Account-registration config rejected"))
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

  test("atomic Admin bootstrap enables Candidate and Recruiter registration without another Admin") {
    replicaSet.use { instance =>
      for {
        config <- configuration(instance, instance.database.underlying.getName)
        _ <- MongoHiringRuntime.resource(config).use { runtime =>
          MongoDatabaseProbe.clientResource(config.uri).use { client =>
            client.getDatabase(config.databaseName).flatMap { database =>
              val users = MongoUserRepository.transactional(
                database,
                client,
                MongoEmbeddingWorkEnqueuer.disabled,
                Diagnostics.noop
              )
              val candidateInput = SignUpInput(
                "Synthetic candidate",
                UserRole.Candidate,
                "synthetic-password-for-registration",
                Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
              )
              val candidateRequest = request("candidate-registration")
              for {
                uninitialized <- users.initialized.value
                rejected <- runtime.services.accountService.signUp(candidateRequest, candidateInput).value
                beforeUsers <- MongoRepositoryTestSupport.count(database, MongoCollections.Users)
                bootstrapped <- runtime.services.accountService
                  .bootstrapAdmin(
                    request("admin-bootstrap"),
                    BootstrapAdminInput("Synthetic admin", "synthetic-admin-password")
                  )
                  .value
                initialized <- users.initialized.value
                registry <- Mongo4catsCollections.documents(database, MongoCollections.AccountRegistry)
                storedRegistry <- registry.find(Filters.eq(MongoFields.Id, "user-account-registry")).first
                candidate <- runtime.services.accountService
                  .signUp(request("candidate-after-bootstrap"), candidateInput)
                  .value
                recruiter <- runtime.services.accountService
                  .signUp(
                    request("recruiter-registration"),
                    SignUpInput(
                      "Synthetic recruiter",
                      UserRole.Recruiter,
                      "synthetic-recruiter-password",
                      Some(UserProfile.Recruiter(RecruiterProfile("Synthetic hiring organization", None)))
                    )
                  )
                  .value
                anotherAdmin <- runtime.services.accountService
                  .bootstrapAdmin(
                    request("another-admin"),
                    BootstrapAdminInput("Another admin", "synthetic-admin-password")
                  )
                  .value
                documents <- Mongo4catsCollections.documents(database, MongoCollections.Users)
                storedUsers <- documents.find(new BsonDocument()).limit(4).boundedStream(4).compile.toVector
                _ <- IO {
                  assertEquals(uninitialized, Right(false))
                  assertEquals(rejected, Left(UseCaseError.Account(AccountError.BootstrapRequired)))
                  assertEquals(beforeUsers, 0L)
                  val admin = bootstrapped.toOption.getOrElse(fail("Normal Admin bootstrap did not succeed"))
                  assertEquals(admin._1.role, UserRole.Admin)
                  assert(admin._1.adminSingleton)
                  assert(admin._2.value.nonEmpty)
                  assertEquals(initialized, Right(true))
                  assertEquals(storedRegistry.map(_.getString(MongoFields.State)), Some("Initialized"))
                  assertEquals(storedRegistry.map(_.getString(MongoFields.AdminId)), Some(admin._1.id.value.toString))
                  assert(candidate.exists { case (user, token) =>
                    user.role == UserRole.Candidate && token.value.nonEmpty
                  })
                  assert(recruiter.exists { case (user, token) =>
                    user.role == UserRole.Recruiter && token.value.nonEmpty
                  })
                  assertEquals(anotherAdmin, Left(UseCaseError.Account(AccountError.AlreadyBootstrapped)))
                  assertEquals(storedUsers.size, 3)
                  assertEquals(storedUsers.count(_.getString(MongoFields.Role) == "Admin"), 1)
                  assertEquals(storedUsers.count(_.getString(MongoFields.AdminSingletonKey) == "singleton-admin"), 1)
                }
              } yield ()
            }
          }
        }
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
