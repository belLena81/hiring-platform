package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.example.graphQL.cats.config.AdminSeedConfig
import com.example.graphQL.cats.infrastructure.auth.Argon2PasswordHasher
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, UpdateOptions, Updates}

final class MongoAdminSeedIntegrationSpec extends MongoIntegrationSuite {
  test("disabled seed has no writes; concurrent seed preserves singleton and existing password") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        permits <- Semaphore[IO](2)
        _ <- Argon2PasswordHasher.resource(1, 8192, 1, permits).use { hasher =>
          val accounts = MongoUserRepository.transactional(
            fixture.database,
            fixture.client,
            MongoEmbeddingWorkEnqueuer.disabled,
            Diagnostics.noop
          )
          val seed = AdminSeedConfig(true, Some("Seed Admin"), Some("original-password"))
          for {
            _ <- MongoAdminSeed.run(fixture.database, accounts, hasher, AdminSeedConfig())
            empty <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.Users)
            _ <- IO(assertEquals(empty, 0L))
            _ <- (
              MongoAdminSeed.run(fixture.database, accounts, hasher, seed),
              MongoAdminSeed.run(fixture.database, accounts, hasher, seed)
            ).parTupled
            _ <- MongoAdminSeed.run(
              fixture.database,
              accounts,
              hasher,
              seed.copy(name = Some(" SEED ADMIN "), password = Some("different-password"))
            )
            count <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.Users)
            credentials <- accounts.findByCanonicalName("seed admin").value
            original <- credentials.toOption.flatten
              .traverse(value => hasher.verify(value.passwordHash, "original-password"))
            changed <- credentials.toOption.flatten
              .traverse(value => hasher.verify(value.passwordHash, "different-password"))
            conflict <- MongoAdminSeed
              .run(fixture.database, accounts, hasher, seed.copy(name = Some("Other Admin")))
              .attempt
            registry <- Mongo4catsCollections.documents(fixture.database, MongoCollections.AccountRegistry)
            _ <- registry.updateOne(
              Filters.eq("_id", "user-account-registry"),
              Updates.set("adminId", "invalid-identity"),
              new UpdateOptions()
            )
            inconsistent <- MongoAdminSeed.run(fixture.database, accounts, hasher, seed).attempt
            _ <- IO {
              assertEquals(count, 1L)
              assertEquals(original, Some(true))
              assertEquals(changed, Some(false))
              assert(conflict.isRight) // drifted seed name is a no-op once the registry is initialized
              assert(inconsistent.isLeft)
            }
          } yield ()
        }
      } yield ()
    }
  }
}
