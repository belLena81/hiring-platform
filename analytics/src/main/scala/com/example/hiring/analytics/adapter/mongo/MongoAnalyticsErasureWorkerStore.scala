package com.example.hiring.analytics.adapter.mongo
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*
import com.example.hiring.analytics.*

import cats.effect.Async
import cats.data.Chain
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoCollection, MongoDatabase}
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
    client: MongoClient,
    database: MongoDatabase,
    streams: MongoPublisherStream,
    collectionName: String = MongoAnalyticsErasureWorkerStore.RequestCollection
) extends ErasureQueue[F],
      ErasureProgress[F],
      ErasureBarrier[F] {
  import MongoAnalyticsErasureWorkerStore.*

  private val requests: MongoCollection[Document] = database.getCollection(collectionName, classOf[Document])
  private val heartbeats: MongoCollection[Document] =
    database.getCollection(MongoAnalyticsErasureWorkerStore.HeartbeatCollection, classOf[Document])
  private val fences = database.getCollection(AnalyticsCollections.OutboxSubjectFences, classOf[Document])
  private val outbox = database.getCollection(AnalyticsCollections.EventOutbox, classOf[Document])
  private val deltaEvidence = database.getCollection(AnalyticsCollections.ErasureDeltaFiles, classOf[Document])

  final case class RepairRequest(requestId: AccountSubjectId, phase: String, attemptCount: Int, failureCategory: String)

  private val repairRequestDecoder = BsonDecoder.instance[RepairRequest] { document =>
    import BsonValueDecoder.given
    for {
      id <- BsonDecoder.required[String](document, AnalyticsCollections.Fields.Id, AnalyticsError.MalformedMarker)
      subjectId <- AccountSubjectId.from(id).leftMap(_ => AnalyticsError.MalformedMarker)
      phase <- BsonDecoder
        .optional[String](document, AnalyticsCollections.Fields.Phase, AnalyticsError.MalformedMarker)
        .map(_.getOrElse(ErasurePhase.Requested.persistedName))
      _ <- Either.cond(ErasurePhase.fromString(phase).nonEmpty, (), AnalyticsError.MalformedMarker)
      attempt <- BsonDecoder
        .required[Int](document, AnalyticsCollections.Fields.AttemptCount, AnalyticsError.MalformedMarker)
      category <- BsonDecoder
        .required[String](document, AnalyticsCollections.Fields.FailureCategory, AnalyticsError.MalformedMarker)
      _ <- Either.cond(
        ErasureFailureCategory.values.exists(_.persistedName == category),
        (),
        AnalyticsError.MalformedMarker
      )
    } yield RepairRequest(subjectId, phase, attempt, category)
  }

  def inspectRepairRequests(limit: Int): F[Vector[RepairRequest]] =
    if (limit <= 0 || limit > MaximumClaimPageSize)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("repair inspection limit is out of bounds"))
    else
      streams
        .stream(
          requests
            .find(Filters.eq(AnalyticsCollections.Fields.RepairRequired, true))
            .sort(Sorts.ascending(AnalyticsCollections.Fields.RequestedAt))
            .limit(limit)
        )
        .compile
        .toVector
        .flatMap { documents =>
          Async[F].fromEither(documents.traverse(repairRequestDecoder.decode))
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(error)       => AnalyticsError.MarkerStorageFailure(error)
        }

  /** Explicitly requeues one observed repair request without altering its durable phase or progress. */
  def requeueRepair(requestId: AccountSubjectId, expectedAttempt: Int, now: Instant): F[ErasureUpdate] =
    if (expectedAttempt < 1)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("invalid repair request identity"))
    else
      mongo {
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
    mongo {
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
      streams
        .optional(requests.findOneAndUpdate(nonFinalizer, update, options))
        .flatMap {
          case some @ Some(_) => Async[F].pure(some)
          case None           => streams.optional(requests.findOneAndUpdate(finalizer, update, options))
        }
    }.flatMap(document =>
      Async[F].fromEither(document.traverse(decodeClaim(_).toRight(AnalyticsError.MalformedMarker)))
    )

  /** Deleted users must have a durable deleted fence before their publisher leases can drain. */
  def publisherDrainReady(
      subjectId: AccountSubjectId,
      now: Instant,
      deliveryTimeout: scala.concurrent.duration.FiniteDuration
  ): F[Boolean] = mongo {
    streams
      .optional(fences.find(Filters.eq(AnalyticsCollections.Fields.Id, subjectId.value)).first())
      .map {
        case None        => false
        case Some(fence) =>
          decodePublisherFence(fence).exists { decoded =>
            decoded.deleted && decoded.leaseToken.forall(_ =>
              decoded.leaseUntil.exists(until => !now.isBefore(until.plusMillis(deliveryTimeout.toMillis)))
            )
          }
      }
  }

  /** IDs are copied into the durable request in the account deletion transaction. */
  def transactionalIds(requestId: AccountSubjectId): F[Vector[String]] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first())
      .map(_.toRight(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing")))
      .map(_.flatMap { request =>
        import BsonValueDecoder.given
        BsonDecoder
          .optional[Int](request, AnalyticsCollections.Fields.FencingVersion, AnalyticsError.MalformedMarker)
          .flatMap { fencingVersion =>
            if (!fencingVersion.contains(1))
              Left(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing"))
            else
              BsonDecoder
                .required[Vector[Any]](
                  request,
                  AnalyticsCollections.Fields.TransactionalIds,
                  AnalyticsError.MalformedMarker
                )
                .flatMap(_.traverse {
                  case value: String if value.trim.nonEmpty => Right(value)
                  case _                                    => Left(AnalyticsError.MalformedMarker)
                })
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
        mongo {
          migrationValidation.flatMap { _ =>
            val filter = Filters.in(
              AnalyticsCollections.Fields.SubjectIds,
              java.util.Collections.singletonList(subjectId.value)
            )
            streams.drain(outbox.deleteMany(filter)).as(filter)
          }
        }.flatMap { filter =>
          streams.stream(outbox.find(filter).limit(1)).take(1).compile.count.flatMap {
            case count if count > 0L => Async[F].pure(false)
            case _                   => outboxValidation.as(true)
          }
        }
    }

  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): F[ErasureUpdate] = mongo {
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

  def readBarrier(requestId: AccountSubjectId): F[Option[KafkaRetentionBarrier]] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first())
      .map(
        _.traverse(document =>
          BsonDecoder
            .optional[Document](
              document,
              AnalyticsCollections.Fields.KafkaRetentionBarrier,
              AnalyticsError.MalformedMarker
            )
            .flatMap(_.traverse(retentionBarrierDecoder.decode))
        ).map(_.flatten)
      )
  }.flatMap(Async[F].fromEither)

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
      val operations: java.util.List[WriteModel[Document]] = files.map { path =>
        val id = java.util.UUID
          .nameUUIDFromBytes(
            (claim.requestId.value + "\u0000" + path).getBytes(java.nio.charset.StandardCharsets.UTF_8)
          )
          .toString
        new ReplaceOneModel[Document](
          Filters.eq(AnalyticsCollections.Fields.Id, id),
          new Document(AnalyticsCollections.Fields.Id, id)
            .append(AnalyticsCollections.Fields.RequestId, claim.requestId.value)
            .append(AnalyticsCollections.Fields.FilePath, path),
          new ReplaceOptions().upsert(true)
        ): WriteModel[Document]
      }.asJava
      val prepared = operations

      mongo {
        MongoSession.resource(client, streams).use { session =>
          streams
            .transaction(session) {
              matchedUpdate(
                session,
                requests,
                ownedClaim(claim, now),
                Updates.inc(AnalyticsCollections.Fields.DeltaEvidenceRevision, 1L)
              ).flatMap { ownership =>
                if (!ownership || prepared.isEmpty) Async[F].pure(toErasureUpdate(ownership))
                else streams.drain(deltaEvidence.bulkWrite(session, prepared)).as(ErasureUpdate.Applied)
              }
            }
        }
      }
    }

  def readDeltaFiles(requestId: AccountSubjectId): F[Vector[String]] =
    streams
      .stream(
        deltaEvidence.find(Filters.eq(AnalyticsCollections.Fields.RequestId, requestId.value))
      )
      .compile
      .toVector
      .flatMap { documents =>
        Async[F].fromEither(
          documents.traverse(document =>
            BsonDecoder.required[String](document, AnalyticsCollections.Fields.FilePath, AnalyticsError.MalformedMarker)
          )
        )
      }
      .adaptError {
        case error: AnalyticsError => error
        case NonFatal(error)       => AnalyticsError.MarkerStorageFailure(error)
      }

  def readAffectedRows(requestId: AccountSubjectId): F[Long] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first())
      .flatMap(document =>
        Async[F].fromEither(
          document
            .traverse(value =>
              BsonDecoder
                .optional[Long](value, AnalyticsCollections.Fields.DeltaAffectedRows, AnalyticsError.MalformedMarker)
                .map(_.getOrElse(0L))
            )
            .map(_.getOrElse(0L))
        )
      )
  }

  def readDeltaGeneration(requestId: AccountSubjectId): F[Option[Long]] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first())
      .flatMap(document =>
        Async[F].fromEither(
          document
            .traverse(value =>
              BsonDecoder
                .optional[Long](value, AnalyticsCollections.Fields.DeltaGeneration, AnalyticsError.MalformedMarker)
            )
            .map(_.flatten)
        )
      )
  }

  def readDeltaPurgedAt(requestId: AccountSubjectId): F[Option[Instant]] = mongo {
    streams
      .optional(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value)).first())
      .flatMap(document =>
        Async[F].fromEither(
          document
            .traverse(value =>
              BsonDecoder
                .optional[Date](value, AnalyticsCollections.Fields.DeltaPurgedAt, AnalyticsError.MalformedMarker)
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
    streams
      .stream(requests.find(filter).limit(1))
      .take(1)
      .compile
      .count
      .map(_ > 0L)
      .adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)       => AnalyticsError.MarkerStorageFailure(cause)
      }
  }

  /** A lease can only be extended while its token is current and its prior lease is still live. */
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
  def heartbeat(now: Instant, leaseUntil: Instant): F[Unit] =
    if (!leaseUntil.isAfter(now))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("heartbeat expiry must follow current time"))
    else
      mongo {
        streams.drain(
          heartbeats.updateOne(
            Filters.eq(AnalyticsCollections.Fields.Id, HeartbeatId),
            Updates.combine(
              Updates.set(AnalyticsCollections.Fields.State, "Ready"),
              Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil)),
              Updates.set(AnalyticsCollections.Fields.UpdatedAt, Date.from(now))
            ),
            new com.mongodb.client.model.UpdateOptions().upsert(true)
          )
        )
      }

  def preflight: F[Unit] = mongo {
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
    streams.stream(database.listCollectionNames()).compile.toVector.flatMap { collectedNames =>
      val missing = required.diff(collectedNames.toSet)
      val collectionValidation = Either.cond(
        missing.isEmpty,
        (),
        AnalyticsError.InvalidConfiguration(
          "analytics erasure worker collections are missing: " + missing.toVector.sorted.mkString(",")
        )
      )
      Async[F].fromEither(collectionValidation) *> migrationValidation *> outboxValidation
    }
  }

  private def migrationValidation: F[Unit] = {
    val ledger = database.getCollection(AnalyticsCollections.HiringMigrationLedger, classOf[Document])
    streams
      .optional(
        ledger
          .find(Filters.eq(AnalyticsCollections.Fields.Id, AnalyticsCollections.MigrationIds.OutboxSubjectReferences))
          .first()
      )
      .flatMap(migration =>
        Either
          .cond(
            migration.exists { document =>
              import BsonValueDecoder.given
              BsonDecoder
                .required[String](
                  document,
                  AnalyticsCollections.Fields.State,
                  AnalyticsError.InvalidConfiguration("outbox subject-reference migration is incomplete")
                )
                .toOption
                .contains("Complete")
            },
            (),
            AnalyticsError.InvalidConfiguration("outbox subject-reference migration is incomplete")
          )
          .liftTo[F]
      )
  }

  private def outboxValidation: F[Unit] =
    streams
      .optional(outbox.find(unverifiedOutboxFilter).limit(1).first())
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

  private def matchedUpdate(collection: MongoCollection[Document], filter: Bson, update: Bson): F[Boolean] =
    streams.one(collection.updateOne(filter, update)).map(_.getMatchedCount == 1L)

  private def matchedUpdate(
      session: ClientSession,
      collection: MongoCollection[Document],
      filter: Bson,
      update: Bson
  ): F[Boolean] = streams.one(collection.updateOne(session, filter, update)).map(_.getMatchedCount == 1L)

  private def mongo[A](work: => F[A]): F[A] =
    Async[F].defer(work).adaptError {
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

  private val retentionBarrierDecoder = BsonDecoder.instance[KafkaRetentionBarrier] { document =>
    import BsonValueDecoder.given
    for {
      topic <- BsonDecoder.required[String](document, AnalyticsCollections.Fields.Topic, AnalyticsError.MalformedMarker)
      rows <- BsonDecoder.required[Vector[Any]](
        document,
        AnalyticsCollections.Fields.Partitions,
        AnalyticsError.MalformedMarker
      )
      partitions <- rows.traverse {
        case row: Document =>
          for {
            number <- BsonDecoder
              .required[Int](row, AnalyticsCollections.Fields.PartitionNumber, AnalyticsError.MalformedMarker)
            offset <- BsonDecoder
              .required[Long](row, AnalyticsCollections.Fields.EndOffsetExclusive, AnalyticsError.MalformedMarker)
          } yield KafkaRetentionBarrier.Partition(number, offset)
        case _ => Left(AnalyticsError.MalformedMarker)
      }
      valid <- KafkaRetentionBarrier.validate(KafkaRetentionBarrier(topic, partitions))
    } yield valid
  }

  private final case class PublisherFence(deleted: Boolean, leaseToken: Option[String], leaseUntil: Option[Instant])

  private val publisherFenceDecoder = BsonDecoder.instance[PublisherFence] { document =>
    import BsonValueDecoder.given
    for {
      deleted <- BsonDecoder
        .optional[Boolean](document, AnalyticsCollections.Fields.Deleted, AnalyticsError.MalformedMarker)
      leaseToken <- BsonDecoder
        .optional[String](document, AnalyticsCollections.Fields.LeaseToken, AnalyticsError.MalformedMarker)
      leaseUntil <- BsonDecoder
        .optional[Date](document, AnalyticsCollections.Fields.LeaseUntil, AnalyticsError.MalformedMarker)
    } yield PublisherFence(deleted.getOrElse(false), leaseToken, leaseUntil.map(_.toInstant))
  }

  private def decodePublisherFence(document: Document): Either[AnalyticsError, PublisherFence] =
    publisherFenceDecoder.decode(document)

  private[analytics] def ownedClaimFilter(claim: ErasureClaim, now: Instant) =
    Filters.and(
      Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId.value),
      Filters.eq(AnalyticsCollections.Fields.State, ErasureRequestState.Processing.persistedName),
      Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
      Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(now))
    )

  private val claimDecoder = BsonDecoder.instance[ErasureClaim] { document =>
    import BsonValueDecoder.given
    val malformed = AnalyticsError.MalformedMarker
    for {
      fencingVersion <- BsonDecoder.required[Int](document, AnalyticsCollections.Fields.FencingVersion, malformed)
      _ <- Either.cond(fencingVersion == 1, (), malformed)
      rawId <- BsonDecoder.required[String](document, AnalyticsCollections.Fields.Id, malformed)
      _ <- Either.cond(scala.util.Try(UUID.fromString(rawId)).toOption.exists(_.toString == rawId), (), malformed)
      id <- AccountSubjectId.from(rawId).leftMap(_ => malformed)
      leaseToken <- BsonDecoder.required[String](document, AnalyticsCollections.Fields.LeaseToken, malformed)
      _ <- Either.cond(
        scala.util.Try(UUID.fromString(leaseToken)).toOption.exists(_.toString == leaseToken),
        (),
        malformed
      )
      leaseExpiry <- BsonDecoder.required[java.util.Date](document, AnalyticsCollections.Fields.LeaseUntil, malformed)
      storedPhase <- BsonDecoder.optional[String](document, AnalyticsCollections.Fields.Phase, malformed)
      currentPhase <- storedPhase
        .fold[Either[AnalyticsError, ErasurePhase]](Right(ErasurePhase.Requested))(phase =>
          ErasurePhase.fromString(phase).toRight(malformed)
        )
      progress <- BsonDecoder
        .optional[Int](document, AnalyticsCollections.Fields.Progress, malformed)
        .map(_.getOrElse(0))
      progressKey <- BsonDecoder
        .optional[Long](document, AnalyticsCollections.Fields.ProgressKey, malformed)
        .map(_.getOrElse(0L))
      attempts <- BsonDecoder
        .optional[Int](document, AnalyticsCollections.Fields.AttemptCount, malformed)
        .map(_.getOrElse(0))
      _ <- Either.cond(
        progress >= 0 && progress < ErasurePhase.ProgressPerPhase && attempts >= 0 &&
          progressKey == currentPhase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong,
        (),
        malformed
      )
    } yield ErasureClaim(id, leaseToken, leaseExpiry.toInstant, currentPhase, progress, progressKey, attempts)
  }

  private[analytics] def decodeClaim(document: Document): Option[ErasureClaim] = claimDecoder.decode(document).toOption
}
