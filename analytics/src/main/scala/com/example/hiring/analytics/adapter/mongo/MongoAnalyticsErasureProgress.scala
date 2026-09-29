package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.*

import cats.data.{Chain, EitherT}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.codecs.CodecRegistry
import mongo4cats.circe.MongoJsonCodecs
import io.circe.{Decoder, Json}
import org.bson.BsonDocument
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.CodecProvider
import com.mongodb.client.model.{FindOneAndUpdateOptions, Filters, ReturnDocument, Sorts, Updates}
import com.mongodb.client.model.{ReplaceOneModel, ReplaceOptions, WriteModel}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Mongo persistence for lease-owned erasure progress and physical-file evidence. */
final class MongoAnalyticsErasureProgress[F[_]: Async] private (
    client: MongoClient[F],
    requests: MongoCollection[F, Json],
    deltaEvidence: MongoCollection[F, Json],
    streams: MongoPublisherStream
) extends MongoAnalyticsErasureStoreSupport[F](streams)
    with ErasureProgress[F] {
  import MongoAnalyticsErasureStoreSupport.*

  def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): F[ErasureUpdate] = mongo {
    matchedUpdate(
      requests,
      ownedClaim(claim, now),
      Updates.set(AnalyticsCollections.Fields.DeltaPurgedAt, Date.from(at))
    ).map(toErasureUpdate)
  }

  def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): F[ErasureUpdate] = mongo {
    matchedUpdate(
      requests,
      ownedClaim(claim, now),
      Updates.set(AnalyticsCollections.Fields.DeltaGeneration, generation)
    ).map(toErasureUpdate)
  }

  def persistAffectedRows(claim: ErasureClaim, affectedRows: Long, now: Instant): F[ErasureUpdate] =
    if (affectedRows < 0L)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("affected row count cannot be negative"))
    else
      mongo {
        matchedUpdate(
          requests,
          ownedClaim(claim, now),
          Updates.max(AnalyticsCollections.Fields.DeltaAffectedRows, affectedRows)
        ).map(toErasureUpdate)
      }

  /** Stores exact pre-purge Delta file identities before the rewrite can invalidate them. */

  def persistDeltaFiles(claim: ErasureClaim, files: Vector[String], now: Instant): F[ErasureUpdate] =
    if (files.distinct.size != files.size || files.exists(path => path == null || path.trim.isEmpty))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("analytics erasure file evidence is malformed"))
    else {
      val operations: java.util.List[WriteModel[Json]] = files.map { path =>
        val id = java.util.UUID
          .nameUUIDFromBytes(
            (claim.requestId.value + "\u0000" + path).getBytes(java.nio.charset.StandardCharsets.UTF_8)
          )
          .toString
        new ReplaceOneModel[Json](
          Filters.eq(AnalyticsCollections.Fields.Id, id),
          Json.obj(
            AnalyticsCollections.Fields.Id -> Json.fromString(id),
            AnalyticsCollections.Fields.RequestId -> Json.fromString(claim.requestId.value),
            AnalyticsCollections.Fields.FilePath -> Json.fromString(path)
          ),
          new ReplaceOptions().upsert(true)
        ): WriteModel[Json]
      }.asJava
      val prepared = operations

      mongo {
        MongoSession.resource(client, streams).use { session =>
          streams
            .transaction(session) {
              EitherT.liftF[F, AnalyticsError, ErasureUpdate](
                matchedUpdate(
                  session,
                  requests,
                  ownedClaim(claim, now),
                  Updates.inc(AnalyticsCollections.Fields.DeltaEvidenceRevision, 1L)
                ).flatMap { ownership =>
                  if (!ownership || prepared.isEmpty) Async[F].pure(toErasureUpdate(ownership))
                  else
                    streams
                      .drain(deltaEvidence.underlying.bulkWrite(session.underlying, prepared))
                      .as(ErasureUpdate.Applied)
                }
              )
            }
            .rethrowT
        }
      }
    }

  def readDeltaFiles(requestId: AccountSubjectId): F[Vector[String]] =
    mongo {
      streams
        .stream(capacity =>
          deltaEvidence.find(Filters.eq(AnalyticsCollections.Fields.RequestId, requestId.value)).boundedStream(capacity)
        )
        .compile
        .toVector
        .flatMap { documents =>
          Async[F].fromEither(
            documents.traverse(document => readField[String](document, AnalyticsCollections.Fields.FilePath))
          )
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(error)       => AnalyticsError.MarkerStorageFailure(error)
        }
    }

  def readAffectedRows(requestId: AccountSubjectId): F[Long] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first)
      .flatMap(document =>
        Async[F].fromEither(
          document
            .traverse(value =>
              value.hcursor
                .get[Json](AnalyticsCollections.Fields.DeltaAffectedRows)
                .toOption
                .fold[Either[AnalyticsError, Option[Long]]](Right(None))(_ =>
                  readLong64(value, AnalyticsCollections.Fields.DeltaAffectedRows).map(Some(_))
                )
                .map(_.getOrElse(0L))
            )
            .map(_.getOrElse(0L))
        )
      )
  }

  def readDeltaGeneration(requestId: AccountSubjectId): F[Option[Long]] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first)
      .flatMap(document =>
        Async[F].fromEither(
          document
            .traverse(value =>
              value.hcursor
                .get[Json](AnalyticsCollections.Fields.DeltaGeneration)
                .toOption
                .fold[Either[AnalyticsError, Option[Long]]](Right(None))(_ =>
                  readLong64(value, AnalyticsCollections.Fields.DeltaGeneration).map(Some(_))
                )
            )
            .map(_.flatten)
        )
      )
  }

  def readDeltaPurgedAt(requestId: AccountSubjectId): F[Option[Instant]] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first)
      .flatMap(document =>
        Async[F].fromEither(
          document
            .traverse(value =>
              readOptional[Date](value, AnalyticsCollections.Fields.DeltaPurgedAt)
                .map(_.map(_.toInstant))
            )
            .map(_.flatten)
        )
      )
  }

  /** Lets other requests progress while preserving the durable checkpoint for a final publication barrier. */

  def releaseForOtherRequests(claim: ErasureClaim, now: Instant): F[ErasureUpdate] = mongo {
    matchedUpdate(
      requests,
      ownedClaim(claim, now),
      Updates.combine(
        Updates.unset(AnalyticsCollections.Fields.LeaseToken),
        Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
      )
    ).map(toErasureUpdate)
  }

  /** Records a sanitized failure only while this worker still owns the live lease. */

  def recordFailure(
      claim: ErasureClaim,
      category: ErasureFailureCategory,
      attempt: Int,
      retryAt: Option[Instant],
      now: Instant
  ): F[ErasureUpdate] =
    if (attempt != claim.attemptCount + 1 || retryAt.exists(at => !at.isAfter(now)))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("invalid erasure retry state"))
    else
      mongo {
        val updates = Vector(
          Updates.set(AnalyticsCollections.Fields.AttemptCount, attempt),
          Updates.set(AnalyticsCollections.Fields.FailureCategory, category.persistedName),
          Updates.set(AnalyticsCollections.Fields.RepairRequired, retryAt.isEmpty),
          Updates.unset(AnalyticsCollections.Fields.LeaseToken),
          Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
        ) ++ retryAt.toVector.map(at => Updates.set(AnalyticsCollections.Fields.ResumeAfter, Date.from(at)))
        matchedUpdate(requests, ownedClaim(claim, now), Updates.combine(updates.asJava)).map(toErasureUpdate)
      }

  def defer(claim: ErasureClaim, resumeAt: Instant, now: Instant): F[ErasureUpdate] =
    if (!resumeAt.isAfter(now))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("deferred retry must be in the future"))
    else
      mongo {
        matchedUpdate(
          requests,
          ownedClaim(claim, now),
          Updates.combine(
            Updates.set(AnalyticsCollections.Fields.ResumeAfter, Date.from(resumeAt)),
            Updates.unset(AnalyticsCollections.Fields.LeaseToken),
            Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
          )
        ).map(toErasureUpdate)
      }

  def renew(claim: ErasureClaim, now: Instant, leaseUntil: Instant): F[ErasureUpdate] =
    if (!leaseUntil.isAfter(now))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("lease expiry must follow current time"))
    else
      mongo {
        matchedUpdate(
          requests,
          ownedClaim(claim, now),
          Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil))
        ).map(toErasureUpdate)
      }

  /** Persists ordered, named progress. A compare-and-set on progressKey prevents an old worker from moving progress
    * backwards or writing after another worker reclaimed it.
    */

  def advance(claim: ErasureClaim, phase: ErasurePhase, progress: Int, now: Instant): F[ErasureUpdate] =
    if (progress < 0 || progress >= ErasurePhase.ProgressPerPhase)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("analytics erasure progress is out of bounds"))
    else {
      val nextKey = phase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong
      if (nextKey < claim.progressKey)
        Async[F].raiseError(AnalyticsError.InvalidConfiguration("analytics erasure progress cannot move backwards"))
      else if (phase.ordinal > claim.phase.ordinal + 1)
        Async[F].raiseError(AnalyticsError.InvalidConfiguration("analytics erasure phases cannot be skipped"))
      else if (nextKey == claim.progressKey) mongo {
        val filter = Filters.and(ownedClaim(claim, now), progressKeyFilter(nextKey))
        matchedUpdate(requests, filter, Updates.set(AnalyticsCollections.Fields.ProgressKey, nextKey))
          .map(toErasureUpdate)
      }
      else
        mongo {
          // Compare against the exact value observed by this claim. A delayed retry from the same
          // lease must not overwrite a later durable checkpoint.
          val filter = Filters.and(ownedClaim(claim, now), progressKeyFilter(claim.progressKey))
          val update = Updates.combine(
            Updates.set(AnalyticsCollections.Fields.Phase, phase.persistedName),
            Updates.set(AnalyticsCollections.Fields.Progress, progress),
            Updates.set(AnalyticsCollections.Fields.ProgressKey, nextKey)
          )
          matchedUpdate(requests, filter, update).map(toErasureUpdate)
        }
    }

  /** Marks the shared worker health lease live after startup or a successful work cycle. */
}

object MongoAnalyticsErasureProgress {
  def resource[F[_]: Async](
      client: MongoClient[F],
      database: MongoDatabase[F],
      streams: MongoPublisherStream,
      collectionName: String = MongoAnalyticsErasureStoreSupport.RequestCollection
  ): Resource[F, MongoAnalyticsErasureProgress[F]] =
    for {
      requests <- Resource.eval(
        database.getCollection[Json](collectionName, MongoAnalyticsErasureStoreSupport.jsonRegistry)
      )
      deltaEvidence <- Resource.eval(
        database
          .getCollection[Json](AnalyticsCollections.ErasureDeltaFiles, MongoAnalyticsErasureStoreSupport.jsonRegistry)
      )
    } yield new MongoAnalyticsErasureProgress(client, requests, deltaEvidence, streams)
}
