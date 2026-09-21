package com.example.graphQL.cats.service.auth

import cats.effect.IO
import cats.effect.std.Semaphore
import de.mkammerer.argon2.{Argon2, Argon2Factory}

trait PasswordHasher[F[_]] {
  def hash(password: String): F[String]
  def verify(encoded: String, password: String): F[Boolean]
  def verifyUnknown(password: String): F[Unit]
}

final class Argon2PasswordHasher(
    iterations: Int,
    memoryKilobytes: Int,
    parallelism: Int,
    permits: Semaphore[IO]
) extends PasswordHasher[IO] {
  private val DummyPassword = "hiring-platform-invalid-password"
  private val argon2: Argon2 = Argon2Factory.create()
  private lazy val unknownUserHash: String = {
    val chars = DummyPassword.toCharArray
    try argon2.hash(iterations, memoryKilobytes, parallelism, chars)
    finally java.util.Arrays.fill(chars, '\u0000')
  }

  override def hash(password: String): IO[String] =
    permits.permit.use { _ =>
      IO.blocking {
        val chars = password.toCharArray
        try argon2.hash(iterations, memoryKilobytes, parallelism, chars)
        finally java.util.Arrays.fill(chars, '\u0000')
      }
    }

  override def verify(encoded: String, password: String): IO[Boolean] =
    permits.permit.use { _ =>
      IO.blocking {
        val chars = password.toCharArray
        try argon2.verify(encoded, chars)
        finally java.util.Arrays.fill(chars, '\u0000')
      }
    }

  override def verifyUnknown(password: String): IO[Unit] =
    permits.permit.use { _ =>
      IO.blocking {
        val chars = password.toCharArray
        try {
          argon2.verify(unknownUserHash, chars)
          ()
        } finally java.util.Arrays.fill(chars, '\u0000')
      }
    }
}
