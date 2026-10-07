package com.example.graphQL.cats.service.protocol

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.service.port.{MutationReceiptFingerprint, RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.UseCaseError

import java.util.UUID

type UseCaseIO[A] = EitherT[IO, UseCaseError, A]

object UseCaseIO {
  def pure[A](value: A): UseCaseIO[A] = EitherT.rightT(value)

  def left[A](error: UseCaseError): UseCaseIO[A] = EitherT.leftT(error)

  def fromEither[A](value: Either[UseCaseError, A]): UseCaseIO[A] = EitherT.fromEither(value)

  def fromIO[A](value: IO[Either[UseCaseError, A]]): UseCaseIO[A] = EitherT(value)

  def liftIO[A](value: IO[A]): UseCaseIO[A] = EitherT.liftF(value)

  def repository[A](value: RepositoryIO[A]): UseCaseIO[A] =
    value.leftMap {
      case RepositoryError.AuthorityRevoked =>
        UseCaseError.Domain(com.example.graphQL.cats.domain.error.DomainError.Forbidden)
      case error => UseCaseError.Repository(error)
    }

  def repository[A](value: IO[Either[RepositoryError, A]]): UseCaseIO[A] =
    repository(RepositoryIO.fromIOEither(value))
}

final case class IdempotencyRequest private[service] (
    idempotencyKey: UUID,
    fingerprint: MutationReceiptFingerprint
)

object IdempotencyRequest {
  def fromCanonicalInput(idempotencyKey: UUID, canonicalInput: String): IdempotencyRequest =
    IdempotencyRequest(idempotencyKey, MutationReceiptFingerprint.fromCanonicalInput(canonicalInput))
}
