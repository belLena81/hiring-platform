package com.example.graphQL.cats.infrastructure.auth

import cats.effect.{IO, Resource}
import cats.effect.std.Semaphore
import com.example.graphQL.cats.config.PasswordHashConfig
import com.example.graphQL.cats.domain.model.PasswordHash
import com.example.graphQL.cats.service.auth.PasswordHasher
import de.mkammerer.argon2.{Argon2, Argon2Factory}

final class Argon2PasswordHasher private (
    argon2: Argon2,
    config: PasswordHashConfig,
    permits: Semaphore[IO],
    unknownUserHash: IO[PasswordHash]
) extends PasswordHasher {
  override def hash(password: String): IO[PasswordHash] =
    permits.permit.use(_ => Argon2PasswordHasher.hashWith(argon2, config, password))

  override def verify(encoded: PasswordHash, password: String): IO[Boolean] =
    permits.permit.use(_ => verifying(encoded, password))

  override def verifyUnknown(password: String): IO[Unit] =
    permits.permit.use(_ => unknownUserHash.flatMap(verifying(_, password)).void)

  private def verifying(encoded: PasswordHash, password: String): IO[Boolean] =
    Argon2PasswordHasher.withPasswordChars(password)(chars => IO.blocking(argon2.verify(encoded.encoded, chars)))
}

object Argon2PasswordHasher {
  private val DummyPassword = "hiring-platform-invalid-password"

  /** The dummy hash used by `verifyUnknown` is computed once, on first use, so unknown-user checks cost a real verify.
    */
  def create(config: PasswordHashConfig, permits: Semaphore[IO]): IO[Argon2PasswordHasher] = {
    val argon2 = Argon2Factory.create()
    hashWith(argon2, config, DummyPassword).memoize.map(new Argon2PasswordHasher(argon2, config, permits, _))
  }

  private def hashWith(argon2: Argon2, config: PasswordHashConfig, password: String): IO[PasswordHash] =
    withPasswordChars(password)(chars =>
      IO.blocking(
        PasswordHash.fromEncoded(argon2.hash(config.iterations, config.memoryKilobytes, config.parallelism, chars))
      )
    )

  private def withPasswordChars[A](password: String)(use: Array[Char] => IO[A]): IO[A] =
    Resource
      .make(IO(password.toCharArray))(chars => IO(java.util.Arrays.fill(chars, '\u0000')))
      .use(use)
}
