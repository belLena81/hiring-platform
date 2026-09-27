package com.example.hiring.analytics.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.AnalyticsError
import com.example.hiring.analytics.batch.AnalyticsLakehouseLock
import com.mongodb.MongoException
import com.mongodb.WriteConcern
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document

import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Mongo mutex with no expiry or automatic takeover. A stale row must be cleared manually after its owner stops. */
private[analytics] final class MongoAnalyticsLakehouseLock(database: MongoDatabase) extends AnalyticsLakehouseLock {
  import MongoAnalyticsLakehouseLock.*

  private val collection = database
    .getCollection(CollectionName, classOf[Document])
    .withWriteConcern(
      WriteConcern.MAJORITY.withJournal(true).withWTimeout(WriteTimeout.toMillis, TimeUnit.MILLISECONDS)
    )

  override def resource(root: String): Resource[IO, Unit] =
    Resource.make(acquire(root))(owner => release(root, owner)).void

  private def acquire(root: String): IO[String] =
    IO.fromEither(lockId(root)).flatMap { id =>
      val owner = UUID.randomUUID().toString
      val deadline = System.nanoTime() + WaitTimeout.toNanos
      def attempt(retryDelay: FiniteDuration): IO[String] = MongoPublisherStream
        .one(
          collection.insertOne(
            new Document("_id", id)
              .append("ownerToken", owner)
              .append("acquiredAt", java.util.Date.from(Instant.now()))
          )
        )
        .as(owner)
        .handleErrorWith {
          case error: MongoException if error.getCode == 11000 =>
            if (System.nanoTime() >= deadline) IO.raiseError(AnalyticsError.LakehouseLockTimeout)
            else jitter(retryDelay).flatMap(IO.sleep) *> attempt((retryDelay * 2).min(MaximumRetryDelay))
          case error: AnalyticsError => IO.raiseError(error)
          case NonFatal(error)       => IO.raiseError(AnalyticsError.LakehouseFailure(error))
        }
      attempt(InitialRetryDelay)
    }

  private def release(root: String, owner: String): IO[Unit] = IO.fromEither(lockId(root)).flatMap { id =>
    MongoPublisherStream
      .one(collection.deleteOne(new Document("_id", id).append("ownerToken", owner)))
      .flatMap(result =>
        IO.raiseWhen(result.getDeletedCount != 1L)(
          AnalyticsError.LakehouseFailure(new IllegalStateException("lakehouse mutex owner changed before release"))
        ).void
      )
      .handleErrorWith { case NonFatal(error) => IO.raiseError(AnalyticsError.LakehouseFailure(error)) }
  }
}

private[analytics] object MongoAnalyticsLakehouseLock {
  val CollectionName = "analytics_lakehouse_mutexes"
  private val WaitTimeout = 2.minutes
  private val InitialRetryDelay = 100.millis
  private val MaximumRetryDelay = 5.seconds
  private val WriteTimeout = 15.seconds

  private def jitter(maximum: FiniteDuration): IO[FiniteDuration] = IO.delay {
    val bound = math.max(1L, maximum.toMillis)
    ThreadLocalRandom.current().nextLong(bound + 1L).millis
  }

  /** The hash lets operators locate the mutex without storing a potentially sensitive URI in Mongo. */
  def lockId(root: String): Either[AnalyticsError, String] =
    Either
      .catchNonFatal {
        val uri = new URI(Option(root).getOrElse("")).normalize()
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
        MessageDigest
          .getInstance("SHA-256")
          .digest(normalized.getBytes(StandardCharsets.UTF_8))
          .map(byte => f"${byte & 0xff}%02x")
          .mkString
      }
      .leftMap(_ =>
        AnalyticsError.InvalidConfiguration(
          "analytics lakehouse root must be an absolute URI without credentials or query parameters"
        )
      )
}
