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
import retry.{ResultHandler, retryingOnErrors}

import scala.util.control.NoStackTrace

/** FS2 boundary for Mongo's cold Reactive Streams publishers. */
private[analytics] final class MongoPublisherStream(settings: AnalyticsOperationalSettings) {
  def stream[F[_], A](query: Int => Stream[F, A]): Stream[F, A] =
    query(settings.mongoPublisherBufferSize)

  def stream[F[_]: Async, A](publisher: => Publisher[A]): Stream[F, A] =
    Stream
      .eval(Async[F].delay(publisher))
      .flatMap(value =>
        fs2.interop.reactivestreams
          .fromPublisher[F, A](value, settings.mongoPublisherBufferSize)
      )

  def optional[F[_]: Async, A](publisher: => Publisher[A]): F[Option[A]] = stream[F, A](publisher).compile.last

  def one[F[_]: Async, A](publisher: => Publisher[A]): F[A] =
    optional[F, A](publisher).flatMap(_.liftTo[F](MongoPublisherStream.CompletedWithoutValue))

  def drain[F[_]: Async, A](publisher: => Publisher[A]): F[Unit] = stream[F, A](publisher).compile.drain

  /** Reactive Streams sessions expose transaction primitives rather than the sync driver's withTransaction helper.
    * Preserve its retry rules: rerun the body for transient transaction failures and retry commit for an unknown commit
    * result, within Mongo's documented transaction callback retry window.
    */
  def transaction[F[_]: Async, A](session: ClientSession[F])(
      work: EitherT[F, AnalyticsError, A]
  )(clock: Clock[F]): EitherT[F, AnalyticsError, A] =
    EitherT(clock.monotonic.flatMap { startedAt =>
      // Both retry levels share one monotonic deadline and retry immediately, as Mongo's withTransaction helper does.
      val deadline = RetryDeadline.until(clock, startedAt + settings.mongoTransactionWindow)

      def hasLabel(label: String)(error: Throwable): Boolean = error match {
        case mongo: MongoException => mongo.hasErrorLabel(label)
        case _                     => false
      }
      def retryOn(isWorthRetrying: Throwable => Boolean) =
        ResultHandler.retryOnSomeErrors[F, Either[AnalyticsError, A]](isWorthRetrying, (_, _) => Async[F].unit)

      def abortIfActive: F[Unit] = if (session.hasActiveTransaction) session.abortTransaction else Async[F].unit

      // An unknown commit result may be committed again; any other failure is left to the transaction-level retry.
      val commit: F[Unit] = retryingOnErrors(Async[F].defer(session.commitTransaction))(
        deadline,
        ResultHandler.retryOnSomeErrors[F, Unit](
          hasLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL),
          (_, _) => Async[F].unit
        )
      )

      // Session primitives are built per attempt: a retry must start a fresh transaction, not replay a built effect.
      val runOnce: F[Either[AnalyticsError, A]] = Async[F].defer {
        val transaction = session.startTransaction *> work.value.flatMap {
          case success @ Right(_) => commit.as(success)
          case failure @ Left(_)  => abortIfActive.as(failure)
        }
        transaction.guaranteeCase {
          case Outcome.Succeeded(_) => Async[F].unit
          case _                    => abortIfActive
        }
      }

      retryingOnErrors(runOnce)(deadline, retryOn(hasLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)))
    })
}

private[analytics] object MongoPublisherStream {

  /** A single-result driver operation completed empty; adapters translate it into their owning [[AnalyticsError]]. */
  case object CompletedWithoutValue
      extends RuntimeException("Mongo publisher completed without a value")
      with NoStackTrace
}
