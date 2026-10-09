package com.example.graphQL.cats.service.protocol

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.service.port.{MutationReceiptFingerprint, RepositoryError, RepositoryIO}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.UseCaseError

import java.util.UUID

type UseCaseIO[A] = EitherT[IO, UseCaseError, A]

object UseCaseIO {

  /** Fails with `error` unless `condition` holds. */
  def ensure(condition: Boolean, error: => UseCaseError): UseCaseIO[Unit] =
    EitherT.cond[IO](condition, (), error)

  /** A repository lookup whose absence is the domain `NotFound(entity)` outcome. */
  def found[A](value: RepositoryIO[Option[A]], entity: String): UseCaseIO[A] =
    repository(value).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound(entity))))

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
