package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AnalyticsLakehouseIdentity
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock

import cats.Monad
import cats.effect.{Async, Clock, Resource, Temporal}
import cats.effect.kernel.Poll
import cats.effect.std.{Random, UUIDGen}
import cats.syntax.all.*
import com.mongodb.MongoException
import com.mongodb.{WriteConcern, ReadConcern, ReadPreference}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import retry.{PolicyDecision, RetryPolicies, RetryPolicy, RetryStatus}
import retry.{HandlerDecision, retryingOnFailures}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Mongo mutex with no expiry or automatic takeover. A stale row must be cleared manually after its owner stops. */
private[analytics] final class MongoAnalyticsLakehouseLock[F[_]: Async: UUIDGen](
    database: MongoDatabase[F],
    streams: MongoPublisherStream,
    clock: Clock[F],
    private[analytics] val afterInsertOverride: Option[F[Unit]] = None
) extends AnalyticsLakehouseLock[F] {
  private val effect = Async[F]
  private val random = Random.javaUtilConcurrentThreadLocalRandom[F]
  import MongoAnalyticsLakehouseLock.*

  private val collection = database
    .getCollection[AnalyticsMongoRecords.LakehouseLock](CollectionName, AnalyticsMongoRecords.lakehouseLockRegistry)
    .map(
      _.withReadConcern(ReadConcern.MAJORITY)
        .withReadPreference(ReadPreference.primary())
        .withWriteConcern(
          WriteConcern.MAJORITY.withJournal(true).withWTimeout(WriteTimeout.toMillis, TimeUnit.MILLISECONDS)
        )
    )

  override def resource(root: String): Resource[F, Unit] =
    Resource.makeFull[F, String](poll => acquire(root, poll))(owner => release(root, owner)).void

  private def acquire(root: String, poll: Poll[F]): F[String] =
    effect.fromEither(lockId(root)).flatMap { id =>
      UUIDGen[F].randomUUID.map(_.toString).flatMap { owner =>
        clock.monotonic.flatMap { startedAt =>
          val policy = contentionPolicy(clock, startedAt + WaitTimeout, random)
          // The sleep is the only cancellable step: an insert whose outcome is unknown must reach reconciliation.
          def attempt(status: RetryStatus): F[String] = clock.realTimeInstant
            .flatMap(acquiredAt =>
              collection.flatMap(
                _.insertOne(
                  AnalyticsMongoRecords.LakehouseLock(id, owner, acquiredAt),
                  mongo4cats.models.collection.InsertOneOptions()
                )
              )
            )
            .flatMap(_ => afterInsertOverride.getOrElse(effect.unit))
            .as(owner)
            .handleErrorWith {
              case error: MongoException if error.getCode == 11000 =>
                clock.monotonic.flatMap { reconciliationStarted =>
                  effect
                    .timeoutTo(
                      readOwner(id, ReconciliationTimeout),
                      ReconciliationTimeout,
                      effect.raiseError(AnalyticsError.LakehouseLockOwnershipUncertain)
                    )
                    .attempt
                    .flatMap {
                      case Right(Some(stored)) if stored == owner => effect.pure(owner)
                      case Left(_)                                => reconcile(id, owner, Some(reconciliationStarted))
                      case _                                      =>
                        policy.decideNextRetry((), status).flatMap {
                          case PolicyDecision.DelayAndRetry(delay) =>
                            poll(Temporal[F].sleep(delay)) *> attempt(status.addRetry(delay))
                          case PolicyDecision.GiveUp => effect.raiseError(AnalyticsError.LakehouseLockTimeout)
                        }
                    }
                }
              case error: AnalyticsError => effect.raiseError(error)
              case NonFatal(_)           => reconcile(id, owner)
            }
          attempt(RetryStatus.NoRetriesYet)
        }
      }
    }

  /** Remains masked until Resource owns a confirmed row. Absence never proves an ambiguous insert failed. */
  private def readOwner(id: String, remaining: FiniteDuration): F[Option[String]] =
    collection
      .flatMap(value =>
        streams.optional(
          value.underlying
            .find(new Document("_id", id))
            .maxTime(remaining.toMillis.max(1L), TimeUnit.MILLISECONDS)
            .first
        )
      )
      .map(_.map(_.ownerToken))

  private def reconcile(id: String, owner: String, startedAt: Option[FiniteDuration] = None): F[String] =
    MongoAnalyticsLakehouseLock.reconcileOwnership[F](owner, remaining => readOwner(id, remaining), startedAt)(using
      effect,
      clock
    )

  private def release(root: String, owner: String): F[Unit] = effect.fromEither(lockId(root)).flatMap { id =>
    collection
      .flatMap(
        _.deleteOne(
          new Document("_id", id).append("ownerToken", owner),
          mongo4cats.models.collection.DeleteOptions()
        )
      )
      .flatMap(result =>
        effect
          .raiseWhen(result.getDeletedCount != 1L)(AnalyticsError.LakehouseLockOwnershipLost)
          .void
      )
      .translating(AnalyticsError.LakehouseFailure(_))
  }
}

private[analytics] object MongoAnalyticsLakehouseLock {
  val CollectionName = "analytics_lakehouse_mutexes"
  private val WaitTimeout = 2.minutes
  private val InitialRetryDelay = 100.millis
  private val MaximumRetryDelay = 5.seconds
  private val WriteTimeout = 15.seconds
  private val ReconciliationTimeout = 30.seconds

  /** Un-jittered contention delays: 100ms doubled per retry and capped at 5s (100, 200, ..., 3200, 5000, 5000, ...). */
  private[analytics] def contentionCeilings[F[_]: Monad]: RetryPolicy[F, Any] =
    RetryPolicies.capDelay(MaximumRetryDelay, RetryPolicies.exponentialBackoff[F](InitialRetryDelay))

  /** Full jitter: each wait is uniform over [0, ceiling] milliseconds, with a minimum ceiling of one millisecond. */
  private[analytics] def contentionBackoff[F[_]: Monad](random: Random[F]): RetryPolicy[F, Any] =
    contentionCeilings[F].flatMapDelay(ceiling =>
      random.betweenLong(0L, math.max(1L, ceiling.toMillis) + 1L).map(_.millis)
    )

  /** Contended acquisition waits with jittered backoff until the monotonic deadline passes. */
  private[analytics] def contentionPolicy[F[_]: Monad](
      clock: Clock[F],
      deadline: FiniteDuration,
      random: Random[F]
  ): RetryPolicy[F, Any] =
    RetryDeadline.until(clock, deadline).join(contentionBackoff(random))

  private enum Reconciliation {
    case Owned, Foreign, Unconfirmed, Expired
  }

  /** Waits for a majority read to confirm whether an uncertain insert belongs to `owner`. Delays are 100ms doubled and
    * capped at 5s, each clamped to the time left in the reconciliation window.
    */
  private[analytics] def reconcileOwnership[F[_]](
      owner: String,
      readOwner: FiniteDuration => F[Option[String]],
      startedAt: Option[FiniteDuration] = None
  )(using F: Async[F], clock: Clock[F]): F[String] =
    startedAt.fold(clock.monotonic)(F.pure).flatMap { started =>
      def remaining: F[FiniteDuration] = clock.monotonic.map(current => ReconciliationTimeout - (current - started))

      val step: F[Reconciliation] = remaining.flatMap { left =>
        if (left <= Duration.Zero) F.pure(Reconciliation.Expired)
        else
          F.timeoutTo(readOwner(left), left, F.raiseError(AnalyticsError.LakehouseLockOwnershipUncertain)).attempt.map {
            case Right(Some(stored)) if stored == owner => Reconciliation.Owned
            case Right(Some(_))                         => Reconciliation.Foreign
            case _                                      => Reconciliation.Unconfirmed
          }
      }
      val backoff = contentionCeilings[F]
      val policy: RetryPolicy[F, Reconciliation] = RetryPolicy[F, Reconciliation] { (_, status) =>
        (remaining, backoff.decideNextRetry((), status)).mapN {
          case (left, PolicyDecision.DelayAndRetry(delay)) if left > Duration.Zero =>
            PolicyDecision.DelayAndRetry(delay.min(left))
          case _ => PolicyDecision.GiveUp
        }
      }
      val handler: retry.ValueHandler[F, Reconciliation] = (outcome, _) =>
        F.pure(if (outcome == Reconciliation.Unconfirmed) HandlerDecision.Continue else HandlerDecision.Stop)
      retryingOnFailures(step)(policy, handler).flatMap {
        case Right(Reconciliation.Owned)   => F.pure(owner)
        case Right(Reconciliation.Foreign) => F.raiseError(AnalyticsError.LakehouseLockTimeout)
        case _                             => F.raiseError(AnalyticsError.LakehouseLockOwnershipUncertain)
      }
    }

  /** The hash lets operators locate the mutex without storing a potentially sensitive URI in Mongo. */
  def lockId(root: String): Either[AnalyticsError, String] =
    AnalyticsLakehouseIdentity
      .from(root)
      .leftMap(_ =>
        AnalyticsError.InvalidConfiguration(
          "analytics lakehouse root must be an absolute URI without credentials or query parameters"
        )
      )
}
