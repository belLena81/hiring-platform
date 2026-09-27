package com.example.hiring.analytics.mongo

import cats.effect.{IO, Outcome}
import cats.syntax.all.*
import com.example.hiring.analytics.AnalyticsError
import com.mongodb.MongoException
import com.mongodb.reactivestreams.client.ClientSession
import fs2.Stream
import org.reactivestreams.Publisher

import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** FS2 boundary for Mongo's cold Reactive Streams publishers. */
private[analytics] object MongoPublisherStream {
  private val BufferSize = 256

  def stream[A](publisher: => Publisher[A]): Stream[IO, A] =
    Stream
      .eval(IO.delay(publisher))
      .flatMap(value => fs2.interop.reactivestreams.fromPublisher[IO, A](value, BufferSize))

  def optional[A](publisher: => Publisher[A]): IO[Option[A]] = stream(publisher).compile.last

  def one[A](publisher: => Publisher[A]): IO[A] =
    optional(publisher).flatMap(_.liftTo[IO](new IllegalStateException("Mongo publisher completed without a value")))

  def drain[A](publisher: => Publisher[A]): IO[Unit] = stream(publisher).compile.drain

  /** Reactive Streams sessions expose transaction primitives rather than the sync driver's withTransaction helper.
    * Preserve its retry rules: rerun the body for transient transaction failures and retry commit for an unknown commit
    * result, within Mongo's documented transaction callback retry window.
    */
  def transaction[A](session: ClientSession)(work: IO[Either[AnalyticsError, A]]): IO[Either[AnalyticsError, A]] = {
    val deadline = System.nanoTime() + 120.seconds.toNanos

    def abortIfActive: IO[Unit] = IO.delay(session.hasActiveTransaction).ifM(drain(session.abortTransaction()), IO.unit)

    def retryTransaction(error: MongoException): Boolean =
      error.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL) && System.nanoTime() < deadline

    def commit: IO[Unit] =
      drain(session.commitTransaction()).handleErrorWith {
        case error: MongoException
            if error.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL) &&
              System.nanoTime() < deadline =>
          commit
        case error: MongoException if retryTransaction(error) =>
          IO.raiseError(RetryTransaction(error))
        case error => IO.raiseError(error)
      }

    def runOnce: IO[Either[AnalyticsError, A]] = {
      val transaction = IO.delay(session.startTransaction()) *> work.flatMap {
        case rejected @ Left(_) => drain(session.abortTransaction()).as(rejected)
        case success @ Right(_) => commit.as(success)
      }
      transaction.guaranteeCase {
        case Outcome.Succeeded(_) => IO.unit
        case _                    => abortIfActive
      }
    }

    def run: IO[Either[AnalyticsError, A]] = runOnce.handleErrorWith {
      case _: RetryTransaction if System.nanoTime() < deadline => IO.defer(run)
      case error: MongoException if retryTransaction(error)    => IO.defer(run)
      case error: RetryTransaction                             => IO.raiseError(error.getCause)
      case NonFatal(error)                                     => IO.raiseError(error)
    }
    run
  }

  private final case class RetryTransaction(cause: MongoException)
      extends RuntimeException("retry Mongo transaction", cause)
}
