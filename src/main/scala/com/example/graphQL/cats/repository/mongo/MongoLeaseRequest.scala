package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.RepositoryF

import cats.effect.IO

import java.time.Instant

/** Why a lease request cannot be granted before any store is touched. */
private[mongo] enum LeaseRequestError {
  case EmptyWorker, WorkerTooLong, WindowNotAfterNow

  /** A malformed request is the caller's fault, not stored data: every queue reports it the same way. */
  def asRepositoryError: RepositoryError = RepositoryError.InvalidEvent
}

/** The one validation of a claim or renewal: a bounded worker identity and a lease that ends after it starts. */
private[mongo] object LeaseRequest {
  val MaxWorkerIdLength = 128

  /** A renewal names no worker: only the window is checked. */
  def validateWindow(now: Instant, leaseUntil: Instant): Either[LeaseRequestError, Unit] =
    Either.cond(leaseUntil.isAfter(now), (), LeaseRequestError.WindowNotAfterNow)

  def validate(workerId: String, now: Instant, leaseUntil: Instant): Either[LeaseRequestError, Unit] =
    if (workerId.isEmpty) Left(LeaseRequestError.EmptyWorker)
    else if (workerId.length > MaxWorkerIdLength) Left(LeaseRequestError.WorkerTooLong)
    else validateWindow(now, leaseUntil)

  /** `granted` runs only for a valid request. */
  def whenValid[A](workerId: String, now: Instant, leaseUntil: Instant)(
      granted: => RepositoryF[IO, A]
  ): RepositoryF[IO, A] =
    validate(workerId, now, leaseUntil).fold(error => MongoF.raise(error.asRepositoryError), _ => granted)

  def whenWindowValid[A](now: Instant, leaseUntil: Instant)(granted: => RepositoryF[IO, A]): RepositoryF[IO, A] =
    validateWindow(now, leaseUntil).fold(error => MongoF.raise(error.asRepositoryError), _ => granted)
}
