package com.example.graphQL.cats.service.auth

import cats.effect.IO
import de.mkammerer.argon2.{Argon2, Argon2Factory}

trait PasswordHasher[F[_]] {
  def hash(password: String): F[String]
  def verify(encoded: String, password: String): F[Boolean]
  def verifyUnknown(password: String): F[Unit]
}

final class Argon2PasswordHasher(
    iterations: Int,
    memoryKilobytes: Int,
    parallelism: Int
) extends PasswordHasher[IO] {
  private val argon2: Argon2 = Argon2Factory.create()

  override def hash(password: String): IO[String] =
    IO.blocking {
      val chars = password.toCharArray
      try argon2.hash(iterations, memoryKilobytes, parallelism, chars)
      finally java.util.Arrays.fill(chars, '\u0000')
    }

  override def verify(encoded: String, password: String): IO[Boolean] =
    IO.blocking {
      val chars = password.toCharArray
      try argon2.verify(encoded, chars)
      finally java.util.Arrays.fill(chars, '\u0000')
    }

  override def verifyUnknown(password: String): IO[Unit] =
    hash(password).map(_ => ())
}
