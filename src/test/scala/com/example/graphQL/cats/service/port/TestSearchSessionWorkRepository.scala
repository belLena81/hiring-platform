package com.example.graphQL.cats.service.port

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId

import java.time.Instant
import java.util.UUID

private[cats] object TestSearchSessionWorkRepository extends SearchSessionWorkRepository[IO] {
  override def enqueue(work: PendingSearchSessionWork, now: Instant): RepositoryF[IO, Unit] =
    com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(())))

  override def findForActor(
      actorId: UserId,
      searchId: UUID
  ): RepositoryF[IO, Option[SearchSessionLookup]] =
    com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(None)))

  override def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryF[IO, Option[ClaimedSearchSessionWork]] =
    com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(None)))

  override def complete(claim: ClaimedSearchSessionWork, now: Instant): RepositoryF[IO, Unit] =
    com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(())))

  override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryF[IO, Unit] =
    com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(())))

  override def fail(
      claim: ClaimedSearchSessionWork,
      failure: SearchSessionWorkFailure,
      now: Instant
  ): RepositoryF[IO, Unit] = com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(())))
}
