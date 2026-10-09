package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.*

import cats.effect.Async
import cats.syntax.all.*
import mongo4cats.client.ClientSession
import mongo4cats.collection.MongoCollection
import mongo4cats.errors.MongoJsonParsingException
import org.bson.conversions.Bson
import com.mongodb.client.model.Filters

import java.time.Instant
import java.util.{Date, UUID}
import scala.util.control.NonFatal

/** Shared lease predicates and Mongo error translation for typed erasure records. */
private[analytics] abstract class MongoAnalyticsErasureStoreSupport[F[_]: Async](
    protected val streams: MongoPublisherStream
) {
  import MongoAnalyticsErasureStoreSupport.*

  protected def ownedClaim(claim: ErasureClaim, now: Instant): Bson = ownedClaimFilter(claim, now)
  protected def toErasureUpdate(matched: Boolean): ErasureUpdate =
    if (matched) ErasureUpdate.Applied else ErasureUpdate.LeaseLost

  protected def progressKeyFilter(value: Long): Bson =
    if (value == 0L)
      Filters.or(
        Filters.eq(AnalyticsCollections.Fields.ProgressKey, 0L),
        Filters.exists(AnalyticsCollections.Fields.ProgressKey, false)
      )
    else Filters.eq(AnalyticsCollections.Fields.ProgressKey, value)

  protected def matchedUpdate[A](collection: MongoCollection[F, A], filter: Bson, update: Bson): F[Boolean] =
    collection.updateOne(filter, update, new com.mongodb.client.model.UpdateOptions()).map(_.getMatchedCount == 1L)

  protected def matchedUpdate[A](
      session: ClientSession[F],
      collection: MongoCollection[F, A],
      filter: Bson,
      update: Bson
  ): F[Boolean] =
    streams.one(collection.underlying.updateOne(session.underlying, filter, update)).map(_.getMatchedCount == 1L)

  protected def mongo[A](work: => F[A]): F[A] = Async[F].defer(work).adaptError {
    case error: AnalyticsError        => error
    case _: MongoJsonParsingException => AnalyticsError.MalformedMarker
    case NonFatal(cause)              => AnalyticsError.MarkerStorageFailure(cause)
  }
}

private[analytics] object MongoAnalyticsErasureStoreSupport {
  val RequestCollection = AnalyticsCollections.ErasureRequests
  val HeartbeatCollection = AnalyticsCollections.ErasureHeartbeats
  val HeartbeatId = "analytics-erasure"
  val MaximumClaimPageSize = 100
  val ProgressPerPhase = ErasurePhase.ProgressPerPhase

  final case class PublisherFence(deleted: Boolean, leaseToken: Option[String], leaseUntil: Option[Instant])

  private def isCanonicalUuid(value: String): Boolean =
    scala.util.Try(UUID.fromString(value)).toOption.exists(_.toString == value)

  private[analytics] def decodeRetentionBarrier(
      barrier: Option[AnalyticsMongoRecords.RetentionBarrier]
  ): Either[AnalyticsError, Option[KafkaRetentionBarrier]] =
    barrier.traverse { value =>
      KafkaRetentionBarrier.from(
        value.topic,
        value.partitions.map(partition => partition.number -> partition.endOffsetExclusive)
      )
    }

  private[analytics] def decodePublisherFence(
      document: AnalyticsMongoRecords.PublisherFence
  ): Either[AnalyticsError, PublisherFence] =
    Right(PublisherFence(document.deleted.getOrElse(false), document.leaseToken, document.leaseUntil))

  private[analytics] def ownedClaimFilter(claim: ErasureClaim, now: Instant): Bson =
    Filters.and(
      Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
      Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
      Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
      Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(now))
    )

  private[analytics] def decodeClaim(document: AnalyticsMongoRecords.ErasureRequest): Option[ErasureClaim] = {
    val phase = document.phase
      .flatMap(ErasurePhase.fromString)
      .orElse(Option.when(document.phase.isEmpty)(ErasurePhase.Requested))
    for {
      _ <- Option.when(document.fencingVersion.contains(1))(())
      _ <- Option.when(isCanonicalUuid(document._id))(())
      id <- AccountSubjectId.from(document._id).toOption
      token <- document.leaseToken.filter(isCanonicalUuid)
      expiry <- document.leaseUntil
      currentPhase <- phase
      progress = document.progress.getOrElse(0)
      progressKey = document.progressKey.getOrElse(0L)
      attempts = document.attemptCount.getOrElse(0)
      _ <- Option.when(
        progress >= 0 && progress < ErasurePhase.ProgressPerPhase && attempts >= 0 &&
          progressKey == currentPhase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong
      )(())
    } yield ErasureClaim(id, token, expiry, currentPhase, progress, progressKey, attempts)
  }
}
