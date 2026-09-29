package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AnalyticsDigest
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock

import cats.effect.{Async, Resource, Temporal}
import cats.syntax.all.*
import com.mongodb.MongoException
import com.mongodb.WriteConcern
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.net.URI
import java.nio.charset.StandardCharsets
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
    private[analytics] val monotonicOverride: Option[F[FiniteDuration]] = None
) extends AnalyticsLakehouseLock[F] {
  private val effect = Async[F]
  private val now = nowOverride.getOrElse(effect.realTimeInstant)
  private val monotonic = monotonicOverride.getOrElse(effect.monotonic)
  import MongoAnalyticsLakehouseLock.*

  private val collection = database
    .getCollection[AnalyticsMongoRecords.LakehouseLock](CollectionName, AnalyticsMongoRecords.lakehouseLockRegistry)
    .map(
      _.withWriteConcern(
        WriteConcern.MAJORITY.withJournal(true).withWTimeout(WriteTimeout.toMillis, TimeUnit.MILLISECONDS)
      )
    )

  override def resource(root: String): Resource[F, Unit] =
    Resource.make(acquire(root))(owner => release(root, owner)).void

  private def acquire(root: String): F[String] =
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
            .as(owner)
            .handleErrorWith {
              case error: MongoException if error.getCode == 11000 =>
                monotonic.flatMap { current =>
                  if (current >= deadline) effect.raiseError(AnalyticsError.LakehouseLockTimeout)
                  else
                    MongoAnalyticsLakehouseLock.jitter[F](retryDelay).flatMap(Temporal[F].sleep) *> attempt(
                      (retryDelay * 2).min(MaximumRetryDelay)
                    )
                }
              case error: AnalyticsError => effect.raiseError(error)
              case NonFatal(error)       => effect.raiseError(AnalyticsError.LakehouseFailure(error))
            }
          attempt(InitialRetryDelay)
        }
      }
    }

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

  private def jitter[F[_]: Async](maximum: FiniteDuration): F[FiniteDuration] = Async[F].delay {
    val bound = math.max(1L, maximum.toMillis)
    ThreadLocalRandom.current().nextLong(bound + 1L).millis
  }

  /** The hash lets operators locate the mutex without storing a potentially sensitive URI in Mongo. */
  def lockId(root: String): Either[AnalyticsError, String] =
    Either
      .catchNonFatal {
        val uri = new URI(root).normalize()
        require(
          uri.getScheme != null && uri.getRawUserInfo == null && uri.getRawQuery == null && uri.getRawFragment == null
        )
        require(!uri.isOpaque)
        val rawPath = Option(uri.getRawPath).getOrElse("")
        val authority = Option(uri.getRawAuthority).fold("")(a => s"//${a.toLowerCase(java.util.Locale.ROOT)}")
        val withRootSlash = if (authority.nonEmpty && rawPath.isEmpty) "/" else rawPath
        val canonicalPath =
          if (withRootSlash.length > 1) withRootSlash.reverse.dropWhile(_ == '/').reverse else withRootSlash
        val normalized = s"${uri.getScheme.toLowerCase(java.util.Locale.ROOT)}:${authority}${canonicalPath}"
        AnalyticsDigest.sha256Hex(normalized.getBytes(StandardCharsets.UTF_8))
      }
      .leftMap(_ =>
        AnalyticsError.InvalidConfiguration(
          "analytics lakehouse root must be an absolute URI without credentials or query parameters"
        )
      )
}
