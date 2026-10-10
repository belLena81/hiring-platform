package com.example.graphQL.cats.infrastructure.auth

import cats.effect.IO
import cats.effect.std.Semaphore
import com.example.graphQL.cats.config.PasswordHashConfig
import munit.CatsEffectSuite

final class Argon2PasswordHasherSpec extends CatsEffectSuite {
  private val cheap = PasswordHashConfig(1, 8192, 1)

  test("hashes verify, and an unknown-user check completes without revealing a result") {
    for {
      permits <- Semaphore[IO](1)
      hasher <- Argon2PasswordHasher.create(cheap, permits)
      hash <- hasher.hash("synthetic-password")
      right <- hasher.verify(hash, "synthetic-password")
      wrong <- hasher.verify(hash, "another-password")
      _ <- hasher.verifyUnknown("synthetic-password")
      _ <- hasher.verifyUnknown("synthetic-password")
    } yield {
      assert(right)
      assert(!wrong)
    }
  }

  test("an unusable hashing configuration fails at creation, not on the first unknown-user check") {
    Semaphore[IO](1).flatMap(permits => Argon2PasswordHasher.create(PasswordHashConfig(0, 0, 0), permits).attempt).map {
      result => assert(result.isLeft)
    }
  }
}
