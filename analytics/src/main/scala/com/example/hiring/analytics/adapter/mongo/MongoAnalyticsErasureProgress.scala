package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.*

import cats.data.EitherT
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import com.mongodb.client.model.{Filters, Projections, Updates}
import com.mongodb.client.model.{ReplaceOneModel, ReplaceOptions, WriteModel}

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Mongo persistence for lease-owned erasure progress and physical-file evidence. */
final class MongoAnalyticsErasureProgress[F[_]: Async] private (
    client: MongoClient[F],
    requests: MongoCollection[F, AnalyticsMongoRecords.ErasureRequest],
    deltaEvidence: MongoCollection[F, AnalyticsMongoRecords.DeltaFileEvidence],
    streams: MongoPublisherStream
) extends MongoAnalyticsErasureStoreSupport[F](streams)
    with ErasureProgress[F] {

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
      val operations: java.util.List[WriteModel[AnalyticsMongoRecords.DeltaFileEvidence]] = files.map { path =>
        val id = java.util.UUID
          .nameUUIDFromBytes(
            (claim.requestId.value + "\u0000" + path).getBytes(java.nio.charset.StandardCharsets.UTF_8)
          )
          .toString
        new ReplaceOneModel[AnalyticsMongoRecords.DeltaFileEvidence](
          Filters.eq(AnalyticsCollections.Fields.Id, id),
          AnalyticsMongoRecords.DeltaFileEvidence(id, claim.requestId.value, path),
          new ReplaceOptions().upsert(true)
        ): WriteModel[AnalyticsMongoRecords.DeltaFileEvidence]
      }.asJava
      val prepared = operations

      mongo {
        MongoSession.resource(client).use { session =>
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
          deltaEvidence
            .find(Filters.eq(AnalyticsCollections.Fields.RequestId, requestId.value))
            .projection(
              Projections.include(
                AnalyticsCollections.Fields.Id,
                AnalyticsCollections.Fields.RequestId,
                AnalyticsCollections.Fields.FilePath
              )
            )
            .boundedStream(capacity)
        )
        .compile
        .toVector
        .map(_.map(_.filePath))
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(error)       => AnalyticsError.MarkerStorageFailure(error)
        }
    }

  def readAffectedRows(requestId: AccountSubjectId): F[Long] = mongo {
    requests
      .find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value))
      .projection(
        Projections.include(AnalyticsCollections.Fields.Id, AnalyticsCollections.Fields.DeltaAffectedRows)
      )
      .first
      .flatMap(document => Async[F].pure(document.fold(0L)(_.deltaAffectedRows.getOrElse(0L))))
  }

  def readDeltaGeneration(requestId: AccountSubjectId): F[Option[Long]] = mongo {
    requests
      .find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value))
      .projection(Projections.include(AnalyticsCollections.Fields.Id, AnalyticsCollections.Fields.DeltaGeneration))
      .first
      .flatMap(document => Async[F].pure(document.flatMap(_.deltaGeneration)))
  }

  def readDeltaPurgedAt(requestId: AccountSubjectId): F[Option[Instant]] = mongo {
    requests
      .find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value))
      .projection(Projections.include(AnalyticsCollections.Fields.Id, AnalyticsCollections.Fields.DeltaPurgedAt))
      .first
      .flatMap(document => Async[F].pure(document.flatMap(_.deltaPurgedAt)))
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
        database.getCollection[AnalyticsMongoRecords.ErasureRequest](
          collectionName,
          AnalyticsMongoRecords.erasureRequestRegistry
        )
      )
      deltaEvidence <- Resource.eval(
        database.getCollection[AnalyticsMongoRecords.DeltaFileEvidence](
          AnalyticsCollections.ErasureDeltaFiles,
          AnalyticsMongoRecords.deltaFileEvidenceRegistry
        )
      )
    } yield new MongoAnalyticsErasureProgress(client, requests, deltaEvidence, streams)
}
