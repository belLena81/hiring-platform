package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.EitherT
import cats.effect.{Async, Clock, Outcome}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.mongodb.MongoException
import fs2.Stream
import mongo4cats.client.ClientSession
import org.reactivestreams.Publisher

import scala.util.control.NonFatal

/** FS2 boundary for Mongo's cold Reactive Streams publishers. */
private[analytics] final class MongoPublisherStream(settings: AnalyticsOperationalSettings) {
  def stream[F[_], A](query: Int => Stream[F, A]): Stream[F, A] = query(settings.mongoPublisherBufferSize)

  def stream[F[_], A](source: Stream[F, A]): Stream[F, A] = source

  def optional[F[_], A](value: F[Option[A]]): F[Option[A]] = value

  def one[F[_], A](value: F[A]): F[A] = value

  def drain[F[_]: Async, A](source: Stream[F, A]): F[Unit] = stream(source).compile.drain

  def stream[F[_]: Async, A](publisher: => Publisher[A]): Stream[F, A] =
    Stream
      .eval(Async[F].delay(publisher))
      .flatMap(value => fs2.interop.reactivestreams.fromPublisher[F, A](value, settings.mongoPublisherBufferSize))

  def optional[F[_]: Async, A](publisher: => Publisher[A]): F[Option[A]] = stream[F, A](publisher).compile.last

  def one[F[_]: Async, A](publisher: => Publisher[A]): F[A] =
    optional[F, A](publisher).flatMap(
      _.liftTo[F](new IllegalStateException("Mongo publisher completed without a value"))
    )

  def drain[F[_]: Async, A](publisher: => Publisher[A]): F[Unit] = stream[F, A](publisher).compile.drain

  /** Reactive Streams sessions expose transaction primitives rather than the sync driver's withTransaction helper.
    * Preserve its retry rules: rerun the body for transient transaction failures and retry commit for an unknown commit
    * result, within Mongo's documented transaction callback retry window.
    */
  def transaction[F[_]: Async: Clock, A](
      session: ClientSession[F]
  )(work: EitherT[F, AnalyticsError, A]): EitherT[F, AnalyticsError, A] =
    transaction(session)(work)(Clock[F])

  def transaction[F[_]: Async, A](session: ClientSession[F])(
      work: EitherT[F, AnalyticsError, A]
  )(clock: Clock[F]): EitherT[F, AnalyticsError, A] =
    EitherT(clock.monotonic.flatMap { startedAt =>
      val deadline = startedAt + settings.mongoTransactionWindow

      def beforeDeadline: F[Boolean] = clock.monotonic.map(_ < deadline)

      def abortIfActive: F[Unit] = if (session.hasActiveTransaction) session.abortTransaction else Async[F].unit

      def retryTransaction(error: MongoException): F[Boolean] =
        if (error.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) beforeDeadline
        else Async[F].pure(false)

      def commit: F[Unit] = session.commitTransaction.handleErrorWith {
        case error: MongoException if error.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL) =>
          beforeDeadline.flatMap(if _ then commit else Async[F].raiseError(error))
        case error: MongoException =>
          retryTransaction(error).flatMap(if _ then Async[F].raiseError(RetryTransaction(error))
          else Async[F].raiseError(error))
        case error => Async[F].raiseError(error)
      }

      def runOnce: F[Either[AnalyticsError, A]] = {
        val transaction = session.startTransaction *> work.value.flatMap {
          case success @ Right(_) => commit.as(success)
          case failure @ Left(_)  => abortIfActive.as(failure)
        }
        transaction.guaranteeCase {
          case Outcome.Succeeded(_) => Async[F].unit
          case _                    => abortIfActive
        }
      }

      def run: F[Either[AnalyticsError, A]] = runOnce.handleErrorWith {
        case retry: RetryTransaction =>
          beforeDeadline.flatMap(if _ then Async[F].defer(run) else Async[F].raiseError(retry.getCause))
        case error: MongoException =>
          retryTransaction(error).flatMap(if _ then Async[F].defer(run) else Async[F].raiseError(error))
        case error: AnalyticsError => Async[F].raiseError(error)
        case NonFatal(error)       => Async[F].raiseError(error)
      }
      run
    })

  private final case class RetryTransaction(cause: MongoException)
      extends RuntimeException("retry Mongo transaction", cause)
}
