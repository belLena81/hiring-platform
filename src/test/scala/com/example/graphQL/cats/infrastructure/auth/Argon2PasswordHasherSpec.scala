package com.example.graphQL.cats.infrastructure.auth

import cats.effect.IO
import cats.effect.std.Semaphore
import io.github.iltotore.iron.autoRefine
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

  // An unusable hashing configuration (zero iterations, memory or parallelism) is unrepresentable: the refined
  // `PasswordHashConfig` fields reject it at decode time, covered by `AppConfigSpec`.
}
