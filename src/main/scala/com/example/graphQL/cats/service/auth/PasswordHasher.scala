package com.example.graphQL.cats.service.auth

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Semaphore
import com.example.graphQL.cats.domain.model.PasswordHash
import de.mkammerer.argon2.{Argon2, Argon2Factory}

trait PasswordHasher {
  def hash(password: String): IO[PasswordHash]
  def verify(encoded: PasswordHash, password: String): IO[Boolean]
  def verifyUnknown(password: String): IO[Unit]
}

final class Argon2PasswordHasher(
    iterations: Int,
    memoryKilobytes: Int,
    parallelism: Int,
    permits: Semaphore[IO],
    unknownUserHash: Deferred[IO, PasswordHash],
    unknownUserHashLock: Semaphore[IO]
) extends PasswordHasher {
  private val DummyPassword = "hiring-platform-invalid-password"
  private val argon2: Argon2 = Argon2Factory.create()
  private def withPasswordChars[A](password: String)(use: Array[Char] => IO[A]): IO[A] =
    Resource
      .make(IO(password.toCharArray))(chars => IO(java.util.Arrays.fill(chars, '\u0000')))
      .use(use)

  private def cachedUnknownUserHash: IO[PasswordHash] =
    unknownUserHashLock.permit.use { _ =>
      unknownUserHash.tryGet.flatMap {
        case Some(hash) => IO.pure(hash)
        case None       =>
          withPasswordChars(DummyPassword)(chars =>
            IO.blocking(PasswordHash.fromEncoded(argon2.hash(iterations, memoryKilobytes, parallelism, chars)))
          ).flatTap(hash => unknownUserHash.complete(hash).void)
      }
    }

  override def hash(password: String): IO[PasswordHash] =
    permits.permit.use(_ =>
      withPasswordChars(password)(chars =>
        IO.blocking(PasswordHash.fromEncoded(argon2.hash(iterations, memoryKilobytes, parallelism, chars)))
      )
    )

  override def verify(encoded: PasswordHash, password: String): IO[Boolean] =
    permits.permit.use(_ => withPasswordChars(password)(chars => IO.blocking(argon2.verify(encoded.encoded, chars))))

  override def verifyUnknown(password: String): IO[Unit] =
    permits.permit.use(_ =>
      cachedUnknownUserHash.flatMap(dummyHash =>
        withPasswordChars(password)(chars => IO.blocking(argon2.verify(dummyHash.encoded, chars))).void
      )
    )
}

object Argon2PasswordHasher {
  def resource(
      iterations: Int,
      memoryKilobytes: Int,
      parallelism: Int,
      permits: Semaphore[IO]
  ): Resource[IO, Argon2PasswordHasher] =
    for {
      unknownUserHash <- Resource.eval(Deferred[IO, PasswordHash])
      unknownUserHashLock <- Resource.eval(Semaphore[IO](1))
    } yield new Argon2PasswordHasher(
      iterations,
      memoryKilobytes,
      parallelism,
      permits,
      unknownUserHash,
      unknownUserHashLock
    )
}
