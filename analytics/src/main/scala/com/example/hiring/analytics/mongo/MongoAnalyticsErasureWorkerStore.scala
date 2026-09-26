package com.example.hiring.analytics.mongo

import com.example.hiring.analytics.erasure.{AnalyticsErasureStore, ErasureClaim, ErasurePhase, KafkaRetentionBarrier}

import com.example.hiring.analytics.*

import cats.effect.IO
import cats.data.Chain
import cats.syntax.all.*
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.client.model.{FindOneAndUpdateOptions, Filters, ReturnDocument, Sorts, Updates}
import com.mongodb.client.model.{ReplaceOneModel, ReplaceOptions, WriteModel}
import org.bson.Document

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
        Filters.ne(AnalyticsCollections.Fields.Phase, ErasurePhase.ReadyToPublish.persistedName),
        Filters.or(
          Filters.eq(AnalyticsCollections.Fields.State, "Pending"),
          Filters.and(Filters.eq(AnalyticsCollections.Fields.State, "Processing"), available)
        )
      )
      val finalizer = Filters.and(
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
        .sort(Sorts.ascending("requestedAt", AnalyticsCollections.Fields.Id))
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
    if (fence == null || !java.lang.Boolean.TRUE.equals(fence.getBoolean("deleted"))) false
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
    if (request == null || Option(request.getInteger("fencingVersion")).forall(_.intValue() != 1))
      Left(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing"))
    else
      Option(request.getList("transactionalIds", classOf[String]))
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
        new Document("number", partition.number).append("endOffsetExclusive", partition.endOffsetExclusive)
      )
      .asJava
    val update = Updates.set(
      AnalyticsCollections.Fields.KafkaRetentionBarrier,
      new Document("topic", barrier.topic).append("partitions", partitionDocuments)
    )
    requests.updateOne(ownedClaim(claim, now), update).getMatchedCount == 1L
  }

  def readBarrier(requestId: String): IO[Option[KafkaRetentionBarrier]] = mongo {
    val request = requests.find(Filters.eq(AnalyticsCollections.Fields.Id, requestId)).first()
    Option(request)
      .flatMap(document => Option(document.get(AnalyticsCollections.Fields.KafkaRetentionBarrier, classOf[Document])))
      .map { stored =>
        val topic = Option(stored.getString("topic")).getOrElse(throw AnalyticsError.MalformedMarker)
        val rows =
          Option(stored.getList("partitions", classOf[Document])).getOrElse(throw AnalyticsError.MalformedMarker)
        val partitions = rows.asScala.toVector.map { value =>
          val number =
            Option(value.getInteger("number")).map(_.intValue()).getOrElse(throw AnalyticsError.MalformedMarker)
          val offset = Option(value.getLong("endOffsetExclusive"))
            .map(_.longValue())
            .getOrElse(throw AnalyticsError.MalformedMarker)
          KafkaRetentionBarrier.Partition(number, offset)
        }
        KafkaRetentionBarrier.validate(KafkaRetentionBarrier(topic, partitions)).fold(throw _, identity)
      }
  }

  def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): IO[Boolean] = mongo {
    requests
      .updateOne(ownedClaim(claim, now), Updates.set(AnalyticsCollections.Fields.DeltaPurgedAt, Date.from(at)))
      .getMatchedCount == 1L
  }

  def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): IO[Boolean] = mongo {
    requests
      .updateOne(ownedClaim(claim, now), Updates.set(AnalyticsCollections.Fields.DeltaGeneration, generation))
      .getMatchedCount == 1L
  }

  def persistAffectedRows(claim: ErasureClaim, affectedRows: Long, now: Instant): IO[Boolean] =
    if (affectedRows < 0L) IO.raiseError(AnalyticsError.InvalidConfiguration("affected row count cannot be negative"))
    else
      mongo {
        requests
          .updateOne(ownedClaim(claim, now), Updates.max(AnalyticsCollections.Fields.DeltaAffectedRows, affectedRows))
          .getMatchedCount == 1L
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
            try {
              session.startTransaction()
              val ownership = requests.updateOne(
                session,
                ownedClaim(claim, now),
                Updates.inc(AnalyticsCollections.Fields.DeltaEvidenceRevision, 1L)
              )
              if (ownership.getMatchedCount != 1L) {
                session.abortTransaction()
                false
              } else {
                if (!operations.isEmpty) deltaEvidence.bulkWrite(session, operations)
                session.commitTransaction()
                true
              }
            } catch {
              case NonFatal(error) =>
                try session.abortTransaction()
                catch { case NonFatal(_) => () }
                throw error
            }
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
    val result = requests.updateOne(
      ownedClaim(claim, now),
      Updates.combine(
        Updates.unset(AnalyticsCollections.Fields.LeaseToken),
        Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
      )
    )
    result.getMatchedCount == 1L
  }

  def defer(claim: ErasureClaim, resumeAt: Instant, now: Instant): IO[Boolean] =
    if (!resumeAt.isAfter(now))
      IO.raiseError(AnalyticsError.InvalidConfiguration("deferred retry must be in the future"))
    else
      mongo {
        val result = requests.updateOne(
          ownedClaim(claim, now),
          Updates.combine(
            Updates.set(AnalyticsCollections.Fields.ResumeAfter, Date.from(resumeAt)),
            Updates.unset(AnalyticsCollections.Fields.LeaseToken),
            Updates.unset(AnalyticsCollections.Fields.LeaseUntil)
          )
        )
        result.getMatchedCount == 1L
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
        val result = requests.updateOne(
          ownedClaim(claim, now),
          Updates.set(AnalyticsCollections.Fields.LeaseUntil, Date.from(leaseUntil))
        )
        result.getMatchedCount == 1L
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
        requests.updateOne(filter, Updates.set(AnalyticsCollections.Fields.ProgressKey, nextKey)).getMatchedCount == 1L
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
          requests.updateOne(filter, update).getMatchedCount == 1L
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
      _ <- Option(document.getInteger("fencingVersion")).filter(_.intValue() == 1)
      id <- requestId
      _ <- scala.util.Try(UUID.fromString(id)).toOption.filter(_.toString == id)
      leaseToken <- token
      _ <- scala.util.Try(UUID.fromString(leaseToken)).toOption.filter(_.toString == leaseToken)
      leaseExpiry <- until
      currentPhase <- phase
      if progress >= 0 && progress < ErasurePhase.ProgressPerPhase &&
        progressKey == currentPhase.ordinal.toLong * ErasurePhase.ProgressPerPhase.toLong + progress.toLong
    } yield ErasureClaim(id, leaseToken, leaseExpiry, currentPhase, progress, progressKey)
  }
}
