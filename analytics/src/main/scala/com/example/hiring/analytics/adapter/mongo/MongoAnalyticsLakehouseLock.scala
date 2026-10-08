package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AnalyticsLakehouseIdentity
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock

import cats.effect.{Async, Resource, Temporal}
import cats.effect.kernel.Poll
import cats.syntax.all.*
import com.mongodb.MongoException
import com.mongodb.{WriteConcern, ReadConcern, ReadPreference}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Mongo mutex with no expiry or automatic takeover. A stale row must be cleared manually after its owner stops. */
private[analytics] final class MongoAnalyticsLakehouseLock[F[_]: Async](
    database: MongoDatabase[F],
    streams: MongoPublisherStream,
    private[analytics] val nowOverride: Option[F[java.time.Instant]] = None,
    private[analytics] val monotonicOverride: Option[F[FiniteDuration]] = None,
    private[analytics] val afterInsertOverride: Option[F[Unit]] = None
) extends AnalyticsLakehouseLock[F] {
  private val effect = Async[F]
  private val now = nowOverride.getOrElse(effect.realTimeInstant)
  private val monotonic = monotonicOverride.getOrElse(effect.monotonic)
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
      effect.delay(UUID.randomUUID().toString).flatMap { owner =>
        monotonic.flatMap { startedAt =>
          val deadline = startedAt + WaitTimeout
          def attempt(retryDelay: FiniteDuration): F[String] = now
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
                effect.monotonic.flatMap { reconciliationStarted =>
                  effect
                    .timeoutTo(
                      readOwner(id, ReconciliationTimeout),
                      ReconciliationTimeout,
                      effect.raiseError(uncertainOwnership)
                    )
                    .attempt
                    .flatMap {
                      case Right(Some(stored)) if stored == owner => effect.pure(owner)
                      case Left(_)                                => reconcile(id, owner, Some(reconciliationStarted))
                      case _                                      =>
                        monotonic.flatMap { current =>
                          if (current >= deadline) effect.raiseError(AnalyticsError.LakehouseLockTimeout)
                          else
                            poll(MongoAnalyticsLakehouseLock.jitter[F](retryDelay).flatMap(Temporal[F].sleep)) *>
                              attempt((retryDelay * 2).min(MaximumRetryDelay))
                        }
                    }
                }
              case error: AnalyticsError => effect.raiseError(error)
              case NonFatal(_)           => reconcile(id, owner)
            }
          attempt(InitialRetryDelay)
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
    MongoAnalyticsLakehouseLock.reconcileOwnership(owner, remaining => readOwner(id, remaining), startedAt)

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
          .raiseWhen(result.getDeletedCount != 1L)(
            AnalyticsError.LakehouseFailure(new IllegalStateException("lakehouse mutex owner changed before release"))
          )
          .void
      )
      .handleErrorWith {
        case error: AnalyticsError => effect.raiseError(error)
        case NonFatal(error)       => effect.raiseError(AnalyticsError.LakehouseFailure(error))
        case error                 => effect.raiseError(error)
      }
  }
}

private[analytics] object MongoAnalyticsLakehouseLock {
  val CollectionName = "analytics_lakehouse_mutexes"
  private val WaitTimeout = 2.minutes
  private val InitialRetryDelay = 100.millis
  private val MaximumRetryDelay = 5.seconds
  private val WriteTimeout = 15.seconds
  private val ReconciliationTimeout = 30.seconds

  private def uncertainOwnership: AnalyticsError = AnalyticsError.LakehouseFailure(
    new IllegalStateException("lakehouse mutex acquisition is uncertain; manual ownership recovery required")
  )

  private[analytics] def reconcileOwnership[F[_]: Async](
      owner: String,
      readOwner: FiniteDuration => F[Option[String]],
      startedAt: Option[FiniteDuration] = None
  ): F[String] = {
    val F = Async[F]
    val uncertain = uncertainOwnership
    startedAt.fold(F.monotonic)(F.pure).flatMap { started =>
      def loop(delay: FiniteDuration): F[String] = F.monotonic.flatMap { current =>
        val remaining = ReconciliationTimeout - (current - started)
        if (remaining <= Duration.Zero) F.raiseError(uncertain)
        else
          F.timeoutTo(readOwner(remaining), remaining, F.raiseError(uncertain)).attempt.flatMap {
            case Right(Some(stored)) if stored == owner => F.pure(owner)
            case Right(Some(_))                         => F.raiseError(AnalyticsError.LakehouseLockTimeout)
            case _                                      =>
              F.monotonic.flatMap { observed =>
                val left = ReconciliationTimeout - (observed - started)
                if (left <= Duration.Zero) F.raiseError(uncertain)
                else F.sleep(delay.min(left)) *> loop((delay * 2).min(MaximumRetryDelay))
              }
          }
      }
      loop(InitialRetryDelay)
    }
  }

  private def jitter[F[_]: Async](maximum: FiniteDuration): F[FiniteDuration] = Async[F].delay {
    val bound = math.max(1L, maximum.toMillis)
    ThreadLocalRandom.current().nextLong(bound + 1L).millis
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
