package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.RepositoryF

import cats.effect.IO

import scala.concurrent.duration.*

/** Why a lease request cannot be granted before any store is touched. */
private[mongo] enum LeaseRequestError {
  case EmptyWorker, WorkerTooLong, DurationNotPositive, DurationTooLong

  /** A malformed request is the caller's fault, not stored data: every queue reports it the same way. */
  def asRepositoryError: RepositoryError = RepositoryError.InvalidEvent
}

/** The one validation of a claim or renewal: a bounded worker identity and a bounded positive lease duration. The
  * expiry instant is decided by the database clock (`$$NOW` plus the duration), never by the caller.
  */
private[mongo] object LeaseRequest {
  val MaxWorkerIdLength = 128

  /** The longest lease a caller may ask for; matches the upper bound of the configured publisher lease (one hour). */
  val MaxLeaseDuration: FiniteDuration = 1.hour

  /** A renewal names no worker: only the duration is checked. */
  def validateDuration(duration: FiniteDuration): Either[LeaseRequestError, Unit] =
    if (duration.toMillis <= 0L) Left(LeaseRequestError.DurationNotPositive)
    else if (duration > MaxLeaseDuration) Left(LeaseRequestError.DurationTooLong)
    else Right(())

  def validate(workerId: String, duration: FiniteDuration): Either[LeaseRequestError, Unit] =
    if (workerId.isEmpty) Left(LeaseRequestError.EmptyWorker)
    else if (workerId.length > MaxWorkerIdLength) Left(LeaseRequestError.WorkerTooLong)
    else validateDuration(duration)

  /** `granted` runs only for a valid request. */
  def whenValid[A](workerId: String, duration: FiniteDuration)(
      granted: => RepositoryF[IO, A]
  ): RepositoryF[IO, A] =
    validate(workerId, duration).fold(error => MongoF.raise(error.asRepositoryError), _ => granted)

  def whenDurationValid[A](duration: FiniteDuration)(granted: => RepositoryF[IO, A]): RepositoryF[IO, A] =
    validateDuration(duration).fold(error => MongoF.raise(error.asRepositoryError), _ => granted)
}
