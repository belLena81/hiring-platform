package com.example.hiring.analytics.mongo

import com.example.hiring.analytics.erasure.{
  AnalyticsErasureStore,
  ErasureClaim,
  ErasureFailureCategory,
  ErasurePhase,
  KafkaRetentionBarrier
}

import com.example.hiring.analytics.*

import cats.effect.IO
import cats.data.Chain
import cats.syntax.all.*
import com.mongodb.client.{ClientSession, MongoCollection, MongoDatabase}
import com.mongodb.client.model.{FindOneAndUpdateOptions, Filters, ReturnDocument, Sorts, Updates}
import com.mongodb.client.model.{ReplaceOneModel, ReplaceOptions, WriteModel}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Durable lease, progress, and liveness primitives for the analytics erasure worker. */
final class MongoAnalyticsErasureWorkerStore(
    client: com.mongodb.client.MongoClient,
    database: MongoDatabase,
    collectionName: String = MongoAnalyticsErasureWorkerStore.RequestCollection
) extends AnalyticsErasureStore {
  import MongoAnalyticsErasureWorkerStore.*

  private val requests: MongoCollection[Document] = database.getCollection(collectionName, classOf[Document])
  private val heartbeats: MongoCollection[Document] =
    database.getCollection(MongoAnalyticsErasureWorkerStore.HeartbeatCollection, classOf[Document])
  private val fences = database.getCollection(AnalyticsCollections.OutboxSubjectFences, classOf[Document])
  private val outbox = database.getCollection(AnalyticsCollections.EventOutbox, classOf[Document])
  private val deltaEvidence = database.getCollection(AnalyticsCollections.ErasureDeltaFiles, classOf[Document])

  final case class RepairRequest(requestId: String, phase: String, attemptCount: Int, failureCategory: String)

  def inspectRepairRequests(limit: Int): IO[Vector[RepairRequest]] =
    if (limit <= 0 || limit > MaximumClaimPageSize)
      IO.raiseError(AnalyticsError.InvalidConfiguration("repair inspection limit is out of bounds"))
    else
      MongoCursorStream(
        requests
          .find(Filters.eq(AnalyticsCollections.Fields.RepairRequired, true))
          .sort(Sorts.ascending(AnalyticsCollections.Fields.RequestedAt))
          .limit(limit)
          .iterator()
      ).compile.toVector
        .flatMap { documents =>
          IO.fromEither(documents.traverse { document =>
            for {
              id <- Option(document.getString(AnalyticsCollections.Fields.Id)).toRight(AnalyticsError.MalformedMarker)
              phase <- Option(document.getString(AnalyticsCollections.Fields.Phase))
                .fold[Either[AnalyticsError, String]](Right(ErasurePhase.Requested.persistedName))(Right(_))
              _ <- Either.cond(ErasurePhase.fromString(phase).nonEmpty, (), AnalyticsError.MalformedMarker)
              attempt <- Option(document.getInteger(AnalyticsCollections.Fields.AttemptCount))
                .map(_.intValue())
                .toRight(AnalyticsError.MalformedMarker)
              category <- Option(document.getString(AnalyticsCollections.Fields.FailureCategory))
                .toRight(AnalyticsError.MalformedMarker)
              _ <- Either.cond(
                ErasureFailureCategory.values.exists(_.persistedName == category),
                (),
                AnalyticsError.MalformedMarker
              )
            } yield RepairRequest(id, phase, attempt, category)
          })
        }
        .adaptError { case NonFatal(error) => AnalyticsError.MarkerStorageFailure(error) }

  /** Explicitly requeues one observed repair request without altering its durable phase or progress. */
  def requeueRepair(requestId: String, expectedAttempt: Int, now: Instant): IO[Boolean] =
    if (expectedAttempt < 1 || scala.util.Try(UUID.fromString(requestId)).isFailure)
      IO.raiseError(AnalyticsError.InvalidConfiguration("invalid repair request identity"))
    else
      mongo {
        val observedState = Filters.and(
          Filters.eq(AnalyticsCollections.Fields.Id, requestId),
          Filters.eq(AnalyticsCollections.Fields.State, "Processing"),
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
            Updates.set(AnalyticsCollections.Fields.State, "Pending"),
            Updates.set(AnalyticsCollections.Fields.ResumeAfter, Date.from(now)),
            Updates.set(AnalyticsCollections.Fields.RepairRequired, false),
            Updates.unset(AnalyticsCollections.Fields.FailureCategory)
          )
        )
      }

  /** Atomically claims at most `limit` oldest eligible requests. */
  def claim(now: Instant, leaseUntil: Instant, limit: Int): IO[Vector[ErasureClaim]] =
    if (limit <= 0 || limit > MaximumClaimPageSize || !leaseUntil.isAfter(now))
      IO.raiseError(AnalyticsError.InvalidConfiguration("invalid analytics erasure claim bounds"))
    else
      cats.Monad[IO].tailRecM(Chain.empty[ErasureClaim]) { claims =>
        if (claims.length >= limit) IO.pure(Right(claims.toList.toVector))
        else
          claimNext(now, leaseUntil).map {
            case None        => Right(claims.toList.toVector)
            case Some(claim) => Left(claims.append(claim))
          }
      }

  private def claimNext(now: Instant, leaseUntil: Instant): IO[Option[ErasureClaim]] =
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
          Filters.eq(AnalyticsCollections.Fields.State, "Pending"),
          Filters.and(Filters.eq(AnalyticsCollections.Fields.State, "Processing"), available)
        )
      )
      val finalizer = Filters.and(
        Filters.ne(AnalyticsCollections.Fields.RepairRequired, true),
        Filters.eq(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName),
        Filters.or(
          Filters.eq(AnalyticsCollections.Fields.State, "Pending"),
          Filters.and(Filters.eq(AnalyticsCollections.Fields.State, "Processing"), available)
        )
      )
      val update = Updates.combine(
        Updates.set(AnalyticsCollections.Fields.State, "Processing"),
        Updates.set(AnalyticsCollections.Fields.LeaseToken, token),
        Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil)),
        Updates.unset(AnalyticsCollections.Fields.ResumeAfter)
      )
      val options = new FindOneAndUpdateOptions()
        .sort(Sorts.ascending(AnalyticsCollections.Fields.RequestedAt, AnalyticsCollections.Fields.Id))
        .returnDocument(ReturnDocument.AFTER)
      Option(requests.findOneAndUpdate(nonFinalizer, update, options))
        .orElse(Option(requests.findOneAndUpdate(finalizer, update, options)))
    }.flatMap(document => IO.fromEither(document.traverse(decodeClaim(_).toRight(AnalyticsError.MalformedMarker))))

  /** Deleted users must have a durable deleted fence before their publisher leases can drain. */
  def publisherDrainReady(
      subjectId: String,
      now: Instant,
      deliveryTimeout: scala.concurrent.duration.FiniteDuration
  ): IO[Boolean] = mongo {
    val fence = fences.find(Filters.eq(AnalyticsCollections.Fields.Id, subjectId)).first()
    if (fence == null || !java.lang.Boolean.TRUE.equals(fence.getBoolean(AnalyticsCollections.Fields.Deleted))) false
    else if (fence.getString(AnalyticsCollections.Fields.LeaseToken) == null) true
    else
      Option(fence.getDate(AnalyticsCollections.Fields.LeaseUntil)) match {
        case None        => false
        case Some(until) => !now.isBefore(until.toInstant.plusMillis(deliveryTimeout.toMillis))
      }
  }

  /** IDs are copied into the durable request in the account deletion transaction. */
  def transactionalIds(requestId: String): IO[Vector[String]] = mongo {
    val request = requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId)).first()
    if (
      request == null || Option(request.getInteger(AnalyticsCollections.Fields.FencingVersion))
        .forall(_.intValue() != 1)
    )
      Left(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing"))
    else
      Option(request.getList(AnalyticsCollections.Fields.TransactionalIds, classOf[String]))
        .toRight(AnalyticsError.MalformedMarker)
        .flatMap(
          _.asScala.toVector
            .traverse(value => Option(value).filter(_.trim.nonEmpty).toRight(AnalyticsError.MalformedMarker))
        )
        .map(_.distinct.sorted)
  }.flatMap(IO.fromEither)

  /** Purging is safe only after the matching publisher fence is deleted and its send lease is drained. */
  def purgeOutbox(
      subjectId: String,
      now: Instant,
      deliveryTimeout: scala.concurrent.duration.FiniteDuration
  ): IO[Boolean] =
    publisherDrainReady(subjectId, now, deliveryTimeout).flatMap {
      case false => IO.pure(false)
      case true  =>
        mongo {
          migrationValidation.map { _ =>
            val filter = Filters.in(
              AnalyticsCollections.Fields.SubjectIds,
              java.util.Collections.singletonList(subjectId)
            )
            outbox.deleteMany(filter)
            filter
          }
        }.flatMap(IO.fromEither).flatMap { filter =>
          MongoCursorStream(outbox.find(filter).limit(1).iterator()).take(1).compile.count.flatMap {
            case count if count > 0L => IO.pure(false)
            case _                   => mongo(outboxValidation.as(true)).flatMap(IO.fromEither)
          }
        }
    }

  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): IO[Boolean] = mongo {
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
    matchedUpdate(requests, ownedClaim(claim, now), update)
  }

  def readBarrier(requestId: String): IO[Option[KafkaRetentionBarrier]] = mongo {
    val request = requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId)).first()
    Option(request)
      .flatMap(document => Option(document.get(AnalyticsCollections.Fields.KafkaRetentionBarrier, classOf[Document])))
      .traverse { stored =>
        for {
          topic <- Option(stored.get(AnalyticsCollections.Fields.Topic))
            .collect { case value: String => value }
            .toRight(AnalyticsError.MalformedMarker)
          rows <- Option(stored.get(AnalyticsCollections.Fields.Partitions))
            .collect { case values: java.util.List[?] => values.asScala.toVector }
            .toRight(AnalyticsError.MalformedMarker)
          partitions <- rows.traverse { row =>
            for {
              value <- Option(row)
                .collect { case document: Document => document }
                .toRight(AnalyticsError.MalformedMarker)
              number <- Option(value.get(AnalyticsCollections.Fields.PartitionNumber))
                .collect { case n: java.lang.Integer => n.intValue() }
                .toRight(AnalyticsError.MalformedMarker)
              offset <- Option(value.get(AnalyticsCollections.Fields.EndOffsetExclusive))
                .collect { case n: java.lang.Long => n.longValue() }
                .toRight(AnalyticsError.MalformedMarker)
            } yield KafkaRetentionBarrier.Partition(number, offset)
          }
          valid <- KafkaRetentionBarrier.validate(KafkaRetentionBarrier(topic, partitions))
        } yield valid
      }
  }.flatMap(IO.fromEither)

  def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): IO[Boolean] = mongo {
    matchedUpdate(
      requests,
      ownedClaim(claim, now),
      Updates.set(AnalyticsCollections.Fields.DeltaPurgedAt, Date.from(at))
    )
  }

  def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): IO[Boolean] = mongo {
    matchedUpdate(
      requests,
      ownedClaim(claim, now),
      Updates.set(AnalyticsCollections.Fields.DeltaGeneration, generation)
    )
  }

  def persistAffectedRows(claim: ErasureClaim, affectedRows: Long, now: Instant): IO[Boolean] =
    if (affectedRows < 0L) IO.raiseError(AnalyticsError.InvalidConfiguration("affected row count cannot be negative"))
    else
      mongo {
        matchedUpdate(
          requests,
          ownedClaim(claim, now),
          Updates.max(AnalyticsCollections.Fields.DeltaAffectedRows, affectedRows)
        )
      }

  /** Stores exact pre-purge Delta file identities before the rewrite can invalidate them. */
  def persistDeltaFiles(claim: ErasureClaim, files: Vector[String], now: Instant): IO[Boolean] =
    if (files.distinct.size != files.size || files.exists(path => path == null || path.trim.isEmpty))
      IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure file evidence is malformed"))
    else
      mongo {
        val operations: java.util.List[WriteModel[Document]] = files.map { path =>
          val id = java.util.UUID
            .nameUUIDFromBytes(
              (claim.requestId + "\u0000" + path).getBytes(java.nio.charset.StandardCharsets.UTF_8)
            )
            .toString
          new ReplaceOneModel[Document](
            Filters.eq(AnalyticsCollections.Fields.Id, id),
            new Document(AnalyticsCollections.Fields.Id, id)
              .append(AnalyticsCollections.Fields.RequestId, claim.requestId)
              .append(AnalyticsCollections.Fields.FilePath, path),
            new ReplaceOptions().upsert(true)
          ): WriteModel[Document]
        }.asJava
        operations
      }.flatMap { operations =>
        MongoSession.resource(client).use { session =>
          mongo {
            session.withTransaction(() => {
              val ownership = matchedUpdate(
                session,
                requests,
                ownedClaim(claim, now),
                Updates.inc(AnalyticsCollections.Fields.DeltaEvidenceRevision, 1L)
              )
              if (ownership) {
                if (!operations.isEmpty) deltaEvidence.bulkWrite(session, operations)
              }
              ownership
            })
          }
        }
      }

  def readDeltaFiles(requestId: String): IO[Vector[String]] =
    MongoCursorStream(
      deltaEvidence.find(Filters.eq(AnalyticsCollections.Fields.RequestId, requestId)).iterator()
    ).compile.toVector
      .flatMap { documents =>
        IO.fromEither(
          documents.traverse(document =>
            Option(document.getString(AnalyticsCollections.Fields.FilePath)).toRight(AnalyticsError.MalformedMarker)
          )
        )
      }
      .adaptError {
        case error: AnalyticsError => error
        case NonFatal(error)       => AnalyticsError.MarkerStorageFailure(error)
      }

  def readAffectedRows(requestId: String): IO[Long] = mongo {
    Option(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId)).first())
      .flatMap(document => Option(document.getLong(AnalyticsCollections.Fields.DeltaAffectedRows)))
      .fold(0L)(_.longValue())
  }

  def readDeltaGeneration(requestId: String): IO[Option[Long]] = mongo {
    Option(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId)).first())
      .flatMap(document => Option(document.getLong(AnalyticsCollections.Fields.DeltaGeneration)))
      .map(_.longValue())
  }

  def readDeltaPurgedAt(requestId: String): IO[Option[Instant]] = mongo {
    Option(requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId)).first())
      .flatMap(document => Option(document.getDate(AnalyticsCollections.Fields.DeltaPurgedAt)))
      .map(_.toInstant)
  }

  /** Lets other requests progress while preserving the durable checkpoint for a final publication barrier. */
  def releaseForOtherRequests(claim: ErasureClaim, now: Instant): IO[Boolean] = mongo {
    matchedUpdate(
      requests,
      ownedClaim(claim, now),
      Updates.combine(
        Updates.unset(AnalyticsCollections.Fields.LeaseToken),
        Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
      )
    )
  }

  /** Records a sanitized failure only while this worker still owns the live lease. */
  def recordFailure(
      claim: ErasureClaim,
      category: ErasureFailureCategory,
      attempt: Int,
      retryAt: Option[Instant],
      now: Instant
  ): IO[Boolean] =
    if (attempt != claim.attemptCount + 1 || retryAt.exists(at => !at.isAfter(now)))
      IO.raiseError(AnalyticsError.InvalidConfiguration("invalid erasure retry state"))
    else
      mongo {
        val updates = Vector(
          Updates.set(AnalyticsCollections.Fields.AttemptCount, attempt),
          Updates.set(AnalyticsCollections.Fields.FailureCategory, category.persistedName),
          Updates.set(AnalyticsCollections.Fields.RepairRequired, retryAt.isEmpty),
          Updates.unset(AnalyticsCollections.Fields.LeaseToken),
          Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
        ) ++ retryAt.toVector.map(at => Updates.set(AnalyticsCollections.Fields.ResumeAfter, Date.from(at)))
        matchedUpdate(requests, ownedClaim(claim, now), Updates.combine(updates.asJava))
      }

  def defer(claim: ErasureClaim, resumeAt: Instant, now: Instant): IO[Boolean] =
    if (!resumeAt.isAfter(now))
      IO.raiseError(AnalyticsError.InvalidConfiguration("deferred retry must be in the future"))
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
        )
      }

  def hasNonReadyOtherRequests(requestId: String): IO[Boolean] = {
    val filter = Filters.and(
      Filters.ne(AnalyticsCollections.Fields.Id, requestId),
      Filters.in(AnalyticsCollections.Fields.State, "Pending", "Processing"),
      Filters.ne(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName)
    )
    MongoCursorStream(requests.find(filter).limit(1).iterator())
      .take(1)
      .compile
      .count
      .map(_ > 0L)
      .adaptError { case NonFatal(cause) => AnalyticsError.MarkerStorageFailure(cause) }
  }

  /** A lease can only be extended while its token is current and its prior lease is still live. */
  def renew(claim: ErasureClaim, now: Instant, leaseUntil: Instant): IO[Boolean] =
    if (!leaseUntil.isAfter(now))
      IO.raiseError(AnalyticsError.InvalidConfiguration("lease expiry must follow current time"))
    else
      mongo {
        matchedUpdate(
          requests,
          ownedClaim(claim, now),
          Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil))
        )
      }

  /** Persists ordered, named progress. A compare-and-set on progressKey prevents an old worker from moving progress
    * backwards or writing after another worker reclaimed it.
    */
  def advance(claim: ErasureClaim, phase: ErasurePhase, progress: Int, now: Instant): IO[Boolean] =
    if (progress < 0 || progress >= ErasurePhase.ProgressPerPhase)
      IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure progress is out of bounds"))
    else {
      val nextKey = phase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong
      if (nextKey < claim.progressKey)
        IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure progress cannot move backwards"))
      else if (phase.ordinal > claim.phase.ordinal + 1)
        IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure phases cannot be skipped"))
      else if (nextKey == claim.progressKey) mongo {
        val filter = Filters.and(ownedClaim(claim, now), progressKeyFilter(nextKey))
        matchedUpdate(requests, filter, Updates.set(AnalyticsCollections.Fields.ProgressKey, nextKey))
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
          matchedUpdate(requests, filter, update)
        }
    }

  /** Marks the shared worker health lease live after startup or a successful work cycle. */
  def heartbeat(now: Instant, leaseUntil: Instant): IO[Unit] =
    if (!leaseUntil.isAfter(now))
      IO.raiseError(AnalyticsError.InvalidConfiguration("heartbeat expiry must follow current time"))
    else
      mongo {
        heartbeats.updateOne(
          Filters.eq(AnalyticsCollections.Fields.Id, HeartbeatId),
          Updates.combine(
            Updates.set(AnalyticsCollections.Fields.State, "Ready"),
            Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil)),
            Updates.set(AnalyticsCollections.Fields.UpdatedAt, Date.from(now))
          ),
          new com.mongodb.client.model.UpdateOptions().upsert(true)
        )
        ()
      }

  def preflight: IO[Unit] = mongo {
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
    val names = database.listCollectionNames().into(new java.util.ArrayList[String]()).asScala.toSet
    val missing = required.diff(names)
    val collectionValidation = Either.cond(
      missing.isEmpty,
      (),
      AnalyticsError.InvalidConfiguration(
        "analytics erasure worker collections are missing: " + missing.toVector.sorted.mkString(",")
      )
    )
    collectionValidation *> migrationValidation *> outboxValidation
  }.flatMap(IO.fromEither)

  private def migrationValidation: Either[AnalyticsError, Unit] = {
    val ledger = database.getCollection(AnalyticsCollections.HiringMigrationLedger, classOf[Document])
    val migration = ledger
      .find(Filters.eq(AnalyticsCollections.Fields.Id, AnalyticsCollections.MigrationIds.OutboxSubjectReferences))
      .first()
    Either.cond(
      migration != null && migration.getString(AnalyticsCollections.Fields.State) == "Complete",
      (),
      AnalyticsError.InvalidConfiguration("outbox subject-reference migration is incomplete")
    )
  }

  private def outboxValidation: Either[AnalyticsError, Unit] =
    Either.cond(
      outbox.find(unverifiedOutboxFilter).limit(1).first() == null,
      (),
      AnalyticsError.InvalidConfiguration("outbox contains rows without verified subject references")
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

  private def progressKeyFilter(value: Long) =
    if (value == 0L)
      Filters.or(
        Filters.eq(AnalyticsCollections.Fields.ProgressKey, 0L),
        Filters.exists(AnalyticsCollections.Fields.ProgressKey, false)
      )
    else Filters.eq(AnalyticsCollections.Fields.ProgressKey, value)

  private def matchedUpdate(collection: MongoCollection[Document], filter: Bson, update: Bson): Boolean =
    collection.updateOne(filter, update).getMatchedCount == 1L

  private def matchedUpdate(
      session: ClientSession,
      collection: MongoCollection[Document],
      filter: Bson,
      update: Bson
  ): Boolean = collection.updateOne(session, filter, update).getMatchedCount == 1L

  private def mongo[A](work: => A): IO[A] =
    IO.blocking(work).adaptError {
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

  private[analytics] def ownedClaimFilter(claim: ErasureClaim, now: Instant) =
    Filters.and(
      Filters.eq(AnalyticsCollections.Fields.Id, claim.requestId),
      Filters.eq(AnalyticsCollections.Fields.State, "Processing"),
      Filters.eq(AnalyticsCollections.Fields.LeaseToken, claim.leaseToken),
      Filters.gt(AnalyticsCollections.Fields.LeaseUntil, Date.from(now))
    )

  private[analytics] def decodeClaim(document: Document): Option[ErasureClaim] = {
    val requestId = Option(document.getString(AnalyticsCollections.Fields.Id))
    val token = Option(document.getString(AnalyticsCollections.Fields.LeaseToken))
    val until = Option(document.getDate(AnalyticsCollections.Fields.LeaseUntil)).map(_.toInstant)
    val phase = Option(document.getString(AnalyticsCollections.Fields.Phase))
      .fold(Some(ErasurePhase.Requested))(ErasurePhase.fromString)
    val progress = Option(document.getInteger(AnalyticsCollections.Fields.Progress)).map(_.intValue()).getOrElse(0)
    val progressKey = Option(document.getLong(AnalyticsCollections.Fields.ProgressKey)).map(_.longValue()).getOrElse(0L)
    for {
      _ <- Option(document.getInteger(AnalyticsCollections.Fields.FencingVersion)).filter(_.intValue() == 1)
      id <- requestId
      _ <- scala.util.Try(UUID.fromString(id)).toOption.filter(_.toString == id)
      leaseToken <- token
      _ <- scala.util.Try(UUID.fromString(leaseToken)).toOption.filter(_.toString == leaseToken)
      leaseExpiry <- until
      currentPhase <- phase
      attempts = Option(document.getInteger(AnalyticsCollections.Fields.AttemptCount)).map(_.intValue()).getOrElse(0)
      if progress >= 0 && progress < ErasurePhase.ProgressPerPhase &&
        attempts >= 0 && progressKey == currentPhase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong
    } yield ErasureClaim(id, leaseToken, leaseExpiry, currentPhase, progress, progressKey, attempts)
  }
}
