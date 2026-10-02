package com.example.graphQL.cats.service.port

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId

import java.time.Instant
import java.util.UUID

private[cats] object TestSearchSessionWorkRepository extends SearchSessionWorkRepository {
  override def enqueue(work: PendingSearchSessionWork, now: Instant): RepositoryIO[Unit] =
    com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))

  override def findForActor(
      actorId: UserId,
      searchId: UUID
  ): RepositoryIO[Option[SearchSessionLookup]] =
    com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(None)))

  override def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedSearchSessionWork]] =
    com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(None)))

  override def complete(claim: ClaimedSearchSessionWork, now: Instant): RepositoryIO[Unit] =
    com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))

  override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryIO[Unit] =
    com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))

  override def fail(
      claim: ClaimedSearchSessionWork,
      failure: SearchSessionWorkFailure,
      now: Instant
  ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))
}
