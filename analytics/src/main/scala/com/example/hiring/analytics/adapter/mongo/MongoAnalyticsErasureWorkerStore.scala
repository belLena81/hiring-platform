package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.ErasureBarrier
import com.example.hiring.analytics.service.erasure.ErasureClaim
import com.example.hiring.analytics.service.erasure.ErasureFailureCategory
import com.example.hiring.analytics.service.erasure.ErasurePhase
import com.example.hiring.analytics.service.erasure.ErasureProgress
import com.example.hiring.analytics.service.erasure.ErasureQueue
import com.example.hiring.analytics.service.erasure.ErasureRequestState
import com.example.hiring.analytics.service.erasure.ErasureUpdate
import com.example.hiring.analytics.service.erasure.KafkaRetentionBarrier

import cats.effect.Async
import cats.data.EitherT
import cats.data.Chain
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.codecs.CodecRegistry
import mongo4cats.circe.MongoJsonCodecs
import io.circe.{Decoder, Encoder, Json}
import org.bson.BsonDocument
import org.bson.codecs.{BsonDocumentCodec, Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry as JavaCodecRegistry}
import com.mongodb.client.model.{FindOneAndUpdateOptions, Filters, ReturnDocument, Sorts, Updates}
import com.mongodb.client.model.{ReplaceOneModel, ReplaceOptions, WriteModel}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Durable lease, progress, and liveness primitives for the analytics erasure worker. */
final class MongoAnalyticsErasureWorkerStore[F[_]: Async](
    client: MongoClient[F],
    database: MongoDatabase[F],
    streams: MongoPublisherStream,
    collectionName: String = MongoAnalyticsErasureWorkerStore.RequestCollection
) extends ErasureQueue[F],
      ErasureProgress[F],
      ErasureBarrier[F] {
  import MongoAnalyticsErasureWorkerStore.*

  import MongoAnalyticsErasureWorkerStore.jsonCodecs.*

  private final case class Collections(
      requests: MongoCollection[F, Json],
      heartbeats: MongoCollection[F, Json],
      fences: MongoCollection[F, Json],
      outbox: MongoCollection[F, Json],
      deltaEvidence: MongoCollection[F, Json],
      ledger: MongoCollection[F, Json]
  )

  private def collections: F[Collections] =
    for {
      requests <- database.getCollection[Json](collectionName, MongoAnalyticsErasureWorkerStore.jsonRegistry)
      heartbeats <- database.getCollection[Json](
        MongoAnalyticsErasureWorkerStore.HeartbeatCollection,
        MongoAnalyticsErasureWorkerStore.jsonRegistry
      )
      fences <- database
        .getCollection[Json](AnalyticsCollections.OutboxSubjectFences, MongoAnalyticsErasureWorkerStore.jsonRegistry)
      outbox <- database
        .getCollection[Json](AnalyticsCollections.EventOutbox, MongoAnalyticsErasureWorkerStore.jsonRegistry)
      deltaEvidence <- database
        .getCollection[Json](AnalyticsCollections.ErasureDeltaFiles, MongoAnalyticsErasureWorkerStore.jsonRegistry)
      ledger <- database
        .getCollection[Json](AnalyticsCollections.HiringMigrationLedger, MongoAnalyticsErasureWorkerStore.jsonRegistry)
    } yield Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger)

  final case class RepairRequest(requestId: AccountSubjectId, phase: String, attemptCount: Int, failureCategory: String)

  private def decodeRepairRequest(document: Json): Either[AnalyticsError, RepairRequest] = {
    val malformed = AnalyticsError.MalformedMarker
    for {
      id <- readField[String](document, AnalyticsCollections.Fields.Id)
      subjectId <- AccountSubjectId.from(id).leftMap(_ => malformed)
      phase <- readOptional[String](document, AnalyticsCollections.Fields.Phase)
        .map(_.getOrElse(ErasurePhase.Requested.persistedName))
      _ <- Either.cond(ErasurePhase.fromString(phase).nonEmpty, (), malformed)
      attempt <- readInt32(document, AnalyticsCollections.Fields.AttemptCount)
      category <- readField[String](document, AnalyticsCollections.Fields.FailureCategory)
      _ <- Either.cond(ErasureFailureCategory.values.exists(_.persistedName == category), (), malformed)
    } yield RepairRequest(subjectId, phase, attempt, category)
  }

  private def readField[A: Decoder](document: Json, name: String): Either[AnalyticsError, A] =
    document.hcursor.get[A](name).leftMap(_ => AnalyticsError.MalformedMarker)

  private def readOptional[A: Decoder](document: Json, name: String): Either[AnalyticsError, Option[A]] =
    document.hcursor.get[Option[A]](name).leftMap(_ => AnalyticsError.MalformedMarker)

  private def readInt32(document: Json, name: String): Either[AnalyticsError, Int] =
    int32(document, name)

  private def readLong64(document: Json, name: String): Either[AnalyticsError, Long] =
    int64(document, name)

  private given Decoder[Instant] = jsonCodecs.instantDecoder
  private given Decoder[Date] = jsonCodecs.instantDecoder.map(Date.from)

  def inspectRepairRequests(limit: Int): F[Vector[RepairRequest]] =
    if (limit <= 0 || limit > MaximumClaimPageSize)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("repair inspection limit is out of bounds"))
    else
      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
        streams
          .stream { capacity =>
            requests
              .find(Filters.eq(AnalyticsCollections.Fields.RepairRequired, true))
              .sort(Sorts.ascending(AnalyticsCollections.Fields.RequestedAt))
              .limit(limit)
              .boundedStream(capacity)
          }
          .compile
          .toVector
          .flatMap { documents =>
            Async[F].fromEither(documents.traverse(decodeRepairRequest))
          }
          .adaptError {
            case error: AnalyticsError => error
            case NonFatal(error)       => AnalyticsError.MarkerStorageFailure(error)
          }
      }

  /** Explicitly requeues one observed repair request without altering its durable phase or progress. */
  def requeueRepair(requestId: AccountSubjectId, expectedAttempt: Int, now: Instant): F[ErasureUpdate] =
    if (expectedAttempt < 1)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("invalid repair request identity"))
    else
      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
        val observedState = Filters.and(
          Filters.eq(AnalyticsCollections.Fields.Id, requestId.value),
          Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
          Filters.eq(AnalyticsCollections.Fields.RepairRequired, true),
          Filters.eq(AnalyticsCollections.Fields.AttemptCount, expectedAttempt),
          Filters.or(
            Filters.exists(AnalyticsCollections.Fields.LeaseUntil, false),
            Filters.lte(AnalyticsCollections.Fields.LeaseUntil, Date.from(now))
          ),
          Filters.exists(AnalyticsCollections.Fields.LeaseToken, false)
        )
        matchedUpdate(
          requests,
          observedState,
          Updates.combine(
            Updates.set(AnalyticsCollections.Fields.State, ErasureRequestState.Pending.persistedName),
            Updates.set(AnalyticsCollections.Fields.ResumeAfter, Date.from(now)),
            Updates.set(AnalyticsCollections.Fields.RepairRequired, false),
            Updates.unset(AnalyticsCollections.Fields.FailureCategory)
          )
        ).map {
          case true  => ErasureUpdate.Applied
          case false => ErasureUpdate.LeaseLost
        }
      }

  /** Atomically claims at most `limit` oldest eligible requests. */
  def claim(now: Instant, leaseUntil: Instant, limit: Int): F[Vector[ErasureClaim]] =
    if (limit <= 0 || limit > MaximumClaimPageSize || !leaseUntil.isAfter(now))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("invalid analytics erasure claim bounds"))
    else
      cats.Monad[F].tailRecM(Chain.empty[ErasureClaim]) { claims =>
        if (claims.length >= limit) Async[F].pure(Right(claims.toList.toVector))
        else
          claimNext(now, leaseUntil).map {
            case None        => Right(claims.toList.toVector)
            case Some(claim) => Left(claims.append(claim))
          }
      }

  private def claimNext(now: Instant, leaseUntil: Instant): F[Option[ErasureClaim]] =
    mongo { db =>
      val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
      val token = UUID.randomUUID().toString
      val available = Filters.and(
        Filters.or(
          Filters.lte(AnalyticsCollections.Fields.LeaseUntil, Date.from(now)),
          Filters.exists(AnalyticsCollections.Fields.LeaseUntil, false)
        ),
        Filters.or(
          Filters.lte(AnalyticsCollections.Fields.ResumeAfter, Date.from(now)),
          Filters.exists(AnalyticsCollections.Fields.ResumeAfter, false)
        )
      )
      val nonFinalizer = Filters.and(
        Filters.ne(AnalyticsCollections.Fields.RepairRequired, true),
        Filters.ne(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName),
        Filters.or(
          Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Pending.persistedName),
          Filters.and(
            Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
            available
          )
        )
      )
      val finalizer = Filters.and(
        Filters.ne(AnalyticsCollections.Fields.RepairRequired, true),
        Filters.eq(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName),
        Filters.or(
          Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Pending.persistedName),
          Filters.and(
            Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
            available
          )
        )
      )
      val update = Updates.combine(
        Updates.set(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
        Updates.set(AnalyticsCollections.Fields.LeaseToken, token),
        Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil)),
        Updates.unset(AnalyticsCollections.Fields.ResumeAfter)
      )
      val options = new FindOneAndUpdateOptions()
        .sort(Sorts.ascending(AnalyticsCollections.Fields.RequestedAt, AnalyticsCollections.Fields.Id))
        .returnDocument(ReturnDocument.AFTER)
      db.requests
        .findOneAndUpdate(nonFinalizer, update, options)
        .flatMap {
          case some @ Some(_) => Async[F].pure(some)
          case None           => db.requests.findOneAndUpdate(finalizer, update, options)
        }
    }.flatMap(document =>
      Async[F].fromEither(document.traverse(decodeClaim(_).toRight(AnalyticsError.MalformedMarker)))
    )

  /** Deleted users must have a durable deleted fence before their publisher leases can drain. */
  def publisherDrainReady(
      subjectId: AccountSubjectId,
      now: Instant,
      deliveryTimeout: scala.concurrent.duration.FiniteDuration
  ): F[Boolean] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
    streams
      .optional(fences.find(Filters.eq(AnalyticsCollections.Fields.Id, subjectId.value)).first)
      .map {
        case None        => false
        case Some(fence) =>
          decodePublisherFence(fence).toOption.exists { decoded =>
            decoded.deleted && decoded.leaseToken.forall(_ =>
              decoded.leaseUntil.exists(until => !now.isBefore(until.plusMillis(deliveryTimeout.toMillis)))
            )
          }
      }
  }

  /** IDs are copied into the durable request in the account deletion transaction. */
  def transactionalIds(requestId: AccountSubjectId): F[Vector[String]] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first)
      .map(_.toRight(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing")))
      .map(_.flatMap { request =>
        request.hcursor
          .get[Json](AnalyticsCollections.Fields.FencingVersion)
          .toOption
          .fold[Either[AnalyticsError, Option[Int]]](Right(None))(_ =>
            readInt32(request, AnalyticsCollections.Fields.FencingVersion).map(Some(_))
          )
          .flatMap { fencingVersion =>
            if (!fencingVersion.contains(1))
              Left(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing"))
            else
              readField[Vector[String]](request, AnalyticsCollections.Fields.TransactionalIds)
                .flatMap(values => Either.cond(values.forall(_.trim.nonEmpty), values, AnalyticsError.MalformedMarker))
                .map(_.distinct.sorted)
          }
      })
      .flatMap(Async[F].fromEither)
  }

  /** Purging is safe only after the matching publisher fence is deleted and its send lease is drained. */
  def purgeOutbox(
      subjectId: AccountSubjectId,
      now: Instant,
      deliveryTimeout: scala.concurrent.duration.FiniteDuration
  ): F[Boolean] =
    publisherDrainReady(subjectId, now, deliveryTimeout).flatMap {
      case false => Async[F].pure(false)
      case true  =>
        mongo { db =>
          val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
          migrationValidation(ledger).flatMap { _ =>
            val filter = Filters.in(
              AnalyticsCollections.Fields.SubjectIds,
              java.util.Collections.singletonList(subjectId.value)
            )
            outbox.deleteMany(filter, new com.mongodb.client.model.DeleteOptions()).void.as(filter)
          }
        }.flatMap { filter =>
          mongo { db =>
            val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
            streams
              .stream(capacity => outbox.find(filter).limit(1).boundedStream(capacity))
              .take(1)
              .compile
              .count
              .flatMap {
                case count if count > 0L => Async[F].pure(false)
                case _                   => outboxValidation(outbox).as(true)
              }
          }
        }
    }

  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): F[ErasureUpdate] = mongo {
    db =>
      val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
      val partitionDocuments = barrier.partitions
        .map(partition =>
          new Document(AnalyticsCollections.Fields.PartitionNumber, partition.number)
            .append(AnalyticsCollections.Fields.EndOffsetExclusive, partition.endOffsetExclusive)
        )
        .asJava
      val update = Updates.set(
        AnalyticsCollections.Fields.KafkaRetentionBarrier,
        new Document(AnalyticsCollections.Fields.Topic, barrier.topic)
          .append(AnalyticsCollections.Fields.Partitions, partitionDocuments)
      )
      matchedUpdate(requests, ownedClaim(claim, now), update).map(toErasureUpdate)
  }

  def readBarrier(requestId: AccountSubjectId): F[Option[KafkaRetentionBarrier]] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first)
      .map(
        _.traverse(document =>
          readOptional[Json](document, AnalyticsCollections.Fields.KafkaRetentionBarrier)
            .flatMap(_.traverse(decodeRetentionBarrier))
        ).map(_.flatten)
      )
  }.flatMap(Async[F].fromEither)

  def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): F[ErasureUpdate] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
    matchedUpdate(
      requests,
      ownedClaim(claim, now),
      Updates.set(AnalyticsCollections.Fields.DeltaPurgedAt, Date.from(at))
    ).map(toErasureUpdate)
  }

  def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): F[ErasureUpdate] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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
      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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

      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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
    mongo { db =>
      val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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

  def readAffectedRows(requestId: AccountSubjectId): F[Long] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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

  def readDeltaGeneration(requestId: AccountSubjectId): F[Option[Long]] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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

  def readDeltaPurgedAt(requestId: AccountSubjectId): F[Option[Instant]] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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
  def releaseForOtherRequests(claim: ErasureClaim, now: Instant): F[ErasureUpdate] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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
      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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
      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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

  def hasNonReadyOtherRequests(requestId: AccountSubjectId): F[Boolean] = {
    val filter = Filters.and(
      Filters.ne(AnalyticsCollections.Fields.Id, requestId.value),
      Filters.in(
        AnalyticsCollections.Fields.State,
        ErasureRequestState.Pending.persistedName,
        ErasureRequestState.Processing.persistedName
      ),
      Filters.ne(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
    )
    mongo { db =>
      val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
      streams
        .stream(capacity => requests.find(filter).limit(1).boundedStream(capacity))
        .take(1)
        .compile
        .count
        .map(_ > 0L)
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.MarkerStorageFailure(cause)
        }
    }
  }

  /** A lease can only be extended while its token is current and its prior lease is still live. */
  def renew(claim: ErasureClaim, now: Instant, leaseUntil: Instant): F[ErasureUpdate] =
    if (!leaseUntil.isAfter(now))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("lease expiry must follow current time"))
    else
      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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
      else if (nextKey == claim.progressKey) mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
        val filter = Filters.and(ownedClaim(claim, now), progressKeyFilter(nextKey))
        matchedUpdate(requests, filter, Updates.set(AnalyticsCollections.Fields.ProgressKey, nextKey))
          .map(toErasureUpdate)
      }
      else
        mongo { db =>
          val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
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
  def heartbeat(now: Instant, leaseUntil: Instant): F[Unit] =
    if (!leaseUntil.isAfter(now))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("heartbeat expiry must follow current time"))
    else
      mongo { db =>
        val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
        heartbeats
          .updateOne(
            Filters.eq(AnalyticsCollections.Fields.Id, HeartbeatId),
            Updates.combine(
              Updates.set(AnalyticsCollections.Fields.State, "Ready"),
              Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil)),
              Updates.set(AnalyticsCollections.Fields.UpdatedAt, Date.from(now))
            ),
            new com.mongodb.client.model.UpdateOptions().upsert(true)
          )
          .void
      }

  def preflight: F[Unit] = mongo { db =>
    val Collections(requests, heartbeats, fences, outbox, deltaEvidence, ledger) = db
    // Delta file evidence is empty until the first erasure reaches physical reclamation;
    // Mongo creates the collection on its first evidence write.
    val required = Set(
      AnalyticsCollections.ErasureRequests,
      AnalyticsCollections.ErasureCompletions,
      AnalyticsCollections.ErasureHeartbeats,
      AnalyticsCollections.ReportSnapshots,
      AnalyticsCollections.ReportControl,
      AnalyticsCollections.ReportRuns,
      AnalyticsCollections.EventOutbox,
      AnalyticsCollections.OutboxSubjectFences,
      AnalyticsCollections.Users
    )
    database.listCollectionNames.map(_.toVector).flatMap { collectedNames =>
      val missing = required.diff(collectedNames.toSet)
      val collectionValidation = Either.cond(
        missing.isEmpty,
        (),
        AnalyticsError.InvalidConfiguration(
          "analytics erasure worker collections are missing: " + missing.toVector.sorted.mkString(",")
        )
      )
      Async[F].fromEither(collectionValidation) *> migrationValidation(ledger) *> outboxValidation(outbox)
    }
  }

  private def migrationValidation(ledger: MongoCollection[F, Json]): F[Unit] = {
    streams
      .optional(
        ledger
          .find(Filters.eq(AnalyticsCollections.Fields.Id, AnalyticsCollections.MigrationIds.OutboxSubjectReferences))
          .first
      )
      .flatMap(migration =>
        Either
          .cond(
            migration.exists { document =>
              readField[String](document, AnalyticsCollections.Fields.State).toOption.contains("Complete")
            },
            (),
            AnalyticsError.InvalidConfiguration("outbox subject-reference migration is incomplete")
          )
          .liftTo[F]
      )
  }

  private def outboxValidation(outbox: MongoCollection[F, Json]): F[Unit] =
    streams
      .optional(outbox.find(unverifiedOutboxFilter).limit(1).first)
      .flatMap(row =>
        Either
          .cond(
            row.isEmpty,
            (),
            AnalyticsError.InvalidConfiguration("outbox contains rows without verified subject references")
          )
          .liftTo[F]
      )

  private def unverifiedOutboxFilter: org.bson.conversions.Bson = {
    val validSubject = new Document(
      "$and",
      List(
        new Document("$eq", List(new Document("$type", "$$subject"), "string").asJava),
        new Document(
          "$regexMatch",
          new Document("input", "$$subject")
            .append("regex", "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        )
      ).asJava
    )
    val validArray = new Document(
      "$and",
      List(
        new Document("$gt", List(new Document("$size", "$subjectIds"), 0).asJava),
        new Document(
          "$allElementsTrue",
          List(
            new Document(
              "$map",
              new Document("input", "$subjectIds")
                .append("as", "subject")
                .append("in", validSubject)
            )
          ).asJava
        )
      ).asJava
    )
    val invalidArray = new Document(
      "$expr",
      new Document(
        "$cond",
        List(
          new Document("$isArray", "$subjectIds"),
          new Document("$not", List(validArray).asJava),
          true
        ).asJava
      )
    )
    Filters.or(
      Filters.exists(AnalyticsCollections.Fields.SubjectRefsVersion, false),
      Filters.ne(AnalyticsCollections.Fields.SubjectRefsVersion, 1),
      Filters.exists(AnalyticsCollections.Fields.SubjectIds, false),
      invalidArray
    )
  }

  private def ownedClaim(claim: ErasureClaim, now: Instant) =
    MongoAnalyticsErasureWorkerStore.ownedClaimFilter(claim, now)

  private def toErasureUpdate(matched: Boolean): ErasureUpdate =
    if (matched) ErasureUpdate.Applied else ErasureUpdate.LeaseLost

  private def progressKeyFilter(value: Long) =
    if (value == 0L)
      Filters.or(
        Filters.eq(AnalyticsCollections.Fields.ProgressKey, 0L),
        Filters.exists(AnalyticsCollections.Fields.ProgressKey, false)
      )
    else Filters.eq(AnalyticsCollections.Fields.ProgressKey, value)

  private def matchedUpdate(collection: MongoCollection[F, Json], filter: Bson, update: Bson): F[Boolean] =
    collection.updateOne(filter, update, new com.mongodb.client.model.UpdateOptions()).map(_.getMatchedCount == 1L)

  private def matchedUpdate(
      session: ClientSession[F],
      collection: MongoCollection[F, Json],
      filter: Bson,
      update: Bson
  ): F[Boolean] =
    streams.one(collection.underlying.updateOne(session.underlying, filter, update)).map(_.getMatchedCount == 1L)

  private def mongo[A](work: Collections => F[A]): F[A] =
    collections.flatMap(db => Async[F].defer(work(db))).adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.MarkerStorageFailure(cause)
    }
}

private[analytics] object MongoAnalyticsErasureWorkerStore {
  val RequestCollection = AnalyticsCollections.ErasureRequests
  val HeartbeatCollection = AnalyticsCollections.ErasureHeartbeats
  val HeartbeatId = "analytics-erasure"
  val MaximumClaimPageSize = 100
  val ProgressPerPhase = ErasurePhase.ProgressPerPhase

  private object jsonCodecs extends MongoJsonCodecs
  import jsonCodecs.*
  private val bsonDocumentCodec = new BsonDocumentCodec()
  private val jsonDecoder = jsonCodecs.deriveJsonBsonValueDecoder[Json]
  private val jsonEncoder = jsonCodecs.deriveJsonBsonValueEncoder[Json]

  private[analytics] val jsonCodec: Codec[Json] = new Codec[Json] {
    override def getEncoderClass: Class[Json] = classOf[Json]
    override def encode(writer: org.bson.BsonWriter, value: Json, context: EncoderContext): Unit = {
      val document = jsonEncoder.encode(value).asDocument.getOrElse(mongo4cats.bson.Document.empty).toBsonDocument
      bsonDocumentCodec.encode(writer, document, context)
    }
    override def decode(reader: org.bson.BsonReader, context: DecoderContext): Json = {
      val document = bsonDocumentCodec.decode(reader, context)
      jsonDecoder
        .decode(mongo4cats.bson.BsonValue.document(mongo4cats.bson.Document.fromJava(document)))
        .getOrElse(Json.obj())
    }
  }

  private val jsonProvider = new mongo4cats.codecs.MongoCodecProvider[Json] {
    override def get: CodecProvider = new CodecProvider {
      override def get[T](clazz: Class[T], registry: JavaCodecRegistry): Codec[T] =
        if (clazz == classOf[Json]) jsonCodec.asInstanceOf[Codec[T]] else null
    }
  }

  private val jsonRegistry = CodecRegistry.mergeWithDefault(CodecRegistry.from(jsonProvider.get))

  private[analytics] def jsonFromBson(document: Document): Json = {
    val raw = document.toBsonDocument(classOf[Document], com.mongodb.MongoClientSettings.getDefaultCodecRegistry)
    jsonDecoder
      .decode(mongo4cats.bson.BsonValue.document(mongo4cats.bson.Document.fromJava(raw)))
      .getOrElse(Json.obj())
  }

  private def malformed = AnalyticsError.MalformedMarker
  private def field[A: Decoder](document: Json, key: String): Either[AnalyticsError, A] =
    document.hcursor.get[A](key).leftMap(_ => malformed)
  private def optional[A: Decoder](document: Json, key: String): Either[AnalyticsError, Option[A]] =
    document.hcursor.get[Option[A]](key).leftMap(_ => malformed)
  private def int32(document: Json, key: String): Either[AnalyticsError, Int] =
    document.hcursor.get[Int](key).leftMap(_ => malformed)
  private def int64(document: Json, key: String): Either[AnalyticsError, Long] =
    document.hcursor.get[Long](key).leftMap(_ => malformed)

  private[analytics] def decodeRetentionBarrier(document: Json): Either[AnalyticsError, KafkaRetentionBarrier] =
    for {
      topic <- field[String](document, AnalyticsCollections.Fields.Topic)
      rows <- field[Vector[Json]](document, AnalyticsCollections.Fields.Partitions)
      partitions <- rows.traverse(row =>
        for {
          number <- int32(row, AnalyticsCollections.Fields.PartitionNumber)
          offset <- int64(row, AnalyticsCollections.Fields.EndOffsetExclusive)
        } yield KafkaRetentionBarrier.Partition(number, offset)
      )
      valid <- KafkaRetentionBarrier.validate(KafkaRetentionBarrier(topic, partitions))
    } yield valid

  private final case class PublisherFence(deleted: Boolean, leaseToken: Option[String], leaseUntil: Option[Instant])

  private def decodePublisherFence(document: Json): Either[AnalyticsError, PublisherFence] =
    for {
      deleted <- optional[Boolean](document, AnalyticsCollections.Fields.Deleted)
      leaseToken <- optional[String](document, AnalyticsCollections.Fields.LeaseToken)
      leaseUntil <- optional[Instant](document, AnalyticsCollections.Fields.LeaseUntil)
    } yield PublisherFence(deleted.getOrElse(false), leaseToken, leaseUntil)

  private[analytics] def ownedClaimFilter(claim: ErasureClaim, now: Instant) =
    Filters.and(
      Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
      Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
      Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
      Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(now))
    )

  private[analytics] def decodeClaim(document: Json): Option[ErasureClaim] = {
    val decoded = for {
      fencingVersion <- int32(document, AnalyticsCollections.Fields.FencingVersion)
      _ <- Either.cond(fencingVersion == 1, (), malformed)
      rawId <- field[String](document, AnalyticsCollections.Fields.Id)
      _ <- Either.cond(scala.util.Try(UUID.fromString(rawId)).toOption.exists(_.toString == rawId), (), malformed)
      id <- AccountSubjectId.from(rawId).leftMap(_ => malformed)
      leaseToken <- field[String](document, AnalyticsCollections.Fields.LeaseToken)
      _ <- Either.cond(
        scala.util.Try(UUID.fromString(leaseToken)).toOption.exists(_.toString == leaseToken),
        (),
        malformed
      )
      leaseExpiry <- field[Instant](document, AnalyticsCollections.Fields.LeaseUntil)
      storedPhase <- optional[String](document, AnalyticsCollections.Fields.Phase)
      currentPhase <- storedPhase.fold[Either[AnalyticsError, ErasurePhase]](Right(ErasurePhase.Requested))(phase =>
        ErasurePhase.fromString(phase).toRight(malformed)
      )
      progress <- document.hcursor
        .get[Json](AnalyticsCollections.Fields.Progress)
        .toOption
        .fold[Either[AnalyticsError, Int]](Right(0))(_ => int32(document, AnalyticsCollections.Fields.Progress))
      progressKey <- document.hcursor
        .get[Json](AnalyticsCollections.Fields.ProgressKey)
        .toOption
        .fold[Either[AnalyticsError, Long]](Right(0L))(_ => int64(document, AnalyticsCollections.Fields.ProgressKey))
      attempts <- document.hcursor
        .get[Json](AnalyticsCollections.Fields.AttemptCount)
        .toOption
        .fold[Either[AnalyticsError, Int]](Right(0))(_ => int32(document, AnalyticsCollections.Fields.AttemptCount))
      _ <- Either.cond(
        progress >= 0 && progress < ErasurePhase.ProgressPerPhase && attempts >= 0 &&
          progressKey == currentPhase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong,
        (),
        malformed
      )
    } yield ErasureClaim(id, leaseToken, leaseExpiry, currentPhase, progress, progressKey, attempts)
    decoded.toOption
  }

}
