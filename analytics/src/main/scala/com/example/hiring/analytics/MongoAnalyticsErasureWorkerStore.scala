package com.example.hiring.analytics

import cats.effect.IO
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
) {
  import MongoAnalyticsErasureWorkerStore.*

  private val requests: MongoCollection[Document] = database.getCollection(collectionName, classOf[Document])
  private val heartbeats: MongoCollection[Document] =
    database.getCollection(MongoAnalyticsErasureWorkerStore.HeartbeatCollection, classOf[Document])
  private val fences = database.getCollection("outbox_subject_fences", classOf[Document])
  private val outbox = database.getCollection("event_outbox", classOf[Document])
  private val deltaEvidence = database.getCollection("analytics_erasure_delta_files", classOf[Document])

  /** Atomically claims at most `limit` oldest eligible requests. */
  def claim(now: Instant, leaseUntil: Instant, limit: Int): IO[Vector[ErasureClaim]] =
    if (limit <= 0 || limit > MaximumClaimPageSize || !leaseUntil.isAfter(now))
      IO.raiseError(AnalyticsError.InvalidConfiguration("invalid analytics erasure claim bounds"))
    else mongo {
      val claims = Vector.newBuilder[ErasureClaim]
      var claimed = 0
      var exhausted = false
      while (claimed < limit && !exhausted) {
        val token = UUID.randomUUID().toString
        val available = Filters.and(
          Filters.or(Filters.lte("leaseUntil", Date.from(now)), Filters.exists("leaseUntil", false)),
          Filters.or(Filters.lte("resumeAfter", Date.from(now)), Filters.exists("resumeAfter", false))
        )
        val nonFinalizer = Filters.and(
          Filters.ne("phase", ErasurePhase.ReadyToPublish.toString),
          Filters.or(Filters.eq("state", "Pending"), Filters.and(Filters.eq("state", "Processing"), available))
        )
        val finalizer = Filters.and(
          Filters.eq("phase", ErasurePhase.ReadyToPublish.toString),
          Filters.or(Filters.eq("state", "Pending"), Filters.and(Filters.eq("state", "Processing"), available))
        )
        val update = Updates.combine(
          Updates.set("state", "Processing"),
          Updates.set("leaseToken", token),
          Updates.set("leaseUntil", Date.from(leaseUntil)),
          Updates.unset("resumeAfter")
        )
        val options = new FindOneAndUpdateOptions().sort(Sorts.ascending("requestedAt", "_id")).returnDocument(ReturnDocument.AFTER)
        Option(requests.findOneAndUpdate(nonFinalizer, update, options))
          .orElse(Option(requests.findOneAndUpdate(finalizer, update, options))) match {
          case None => exhausted = true
          case Some(document) =>
            claims += decodeClaim(document).fold(throw AnalyticsError.MalformedMarker)(identity)
            claimed += 1
        }
      }
      claims.result()
    }

  /** Deleted users must have a durable deleted fence before their publisher leases can drain. */
  def publisherDrainReady(subjectId: String, now: Instant, deliveryTimeout: scala.concurrent.duration.FiniteDuration): IO[Boolean] = mongo {
    val fence = fences.find(Filters.eq("_id", subjectId)).first()
    if (fence == null || !java.lang.Boolean.TRUE.equals(fence.getBoolean("deleted"))) false
    else if (fence.getString("leaseToken") == null) true
    else Option(fence.getDate("leaseUntil")) match {
      case None => false
      case Some(until) => !now.isBefore(until.toInstant.plusMillis(deliveryTimeout.toMillis))
    }
  }

  /** IDs are copied into the durable request in the account deletion transaction. */
  def transactionalIds(requestId: String): IO[Vector[String]] = mongo {
    val request = requests.find(Filters.eq("_id", requestId)).first()
    if (request == null || Option(request.getInteger("fencingVersion")).forall(_.intValue() != 1))
      throw AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing")
    Option(request.getList("transactionalIds", classOf[String])).fold[Vector[String]](
      throw AnalyticsError.MalformedMarker
    )(_.asScala.toVector)
      .map(value => Option(value).filter(_.trim.nonEmpty).getOrElse(throw AnalyticsError.MalformedMarker))
      .distinct.sorted
  }

  /** Purging is safe only after the matching publisher fence is deleted and its send lease is drained. */
  def purgeOutbox(subjectId: String, now: Instant, deliveryTimeout: scala.concurrent.duration.FiniteDuration): IO[Boolean] =
    publisherDrainReady(subjectId, now, deliveryTimeout).flatMap {
      case false => IO.pure(false)
      case true => mongo {
        requireSubjectReferenceMigration()
        val filter = Filters.in("subjectIds", java.util.Collections.singletonList(subjectId))
        outbox.deleteMany(filter)
        val remaining = outbox.find(filter).limit(1).iterator().hasNext
        if (!remaining) requireNoUnverifiedOutboxRows()
        !remaining
      }
    }

  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): IO[Boolean] = mongo {
    val partitionDocuments = barrier.partitions.map(partition =>
      new Document("number", partition.number).append("endOffsetExclusive", partition.endOffsetExclusive)
    ).asJava
    val update = Updates.set("kafkaRetentionBarrier", new Document("topic", barrier.topic).append("partitions", partitionDocuments))
    requests.updateOne(ownedClaim(claim, now), update).getMatchedCount == 1L
  }

  def readBarrier(requestId: String): IO[Option[KafkaRetentionBarrier]] = mongo {
    val request = requests.find(Filters.eq("_id", requestId)).first()
    Option(request).flatMap(document => Option(document.get("kafkaRetentionBarrier", classOf[Document]))).map { stored =>
      val topic = Option(stored.getString("topic")).getOrElse(throw AnalyticsError.MalformedMarker)
      val rows = Option(stored.getList("partitions", classOf[Document])).getOrElse(throw AnalyticsError.MalformedMarker)
      val partitions = rows.asScala.toVector.map { value =>
        val number = Option(value.getInteger("number")).map(_.intValue()).getOrElse(throw AnalyticsError.MalformedMarker)
        val offset = Option(value.getLong("endOffsetExclusive")).map(_.longValue()).getOrElse(throw AnalyticsError.MalformedMarker)
        KafkaRetentionBarrier.Partition(number, offset)
      }
      KafkaRetentionBarrier.validate(KafkaRetentionBarrier(topic, partitions)).fold(throw _, identity)
    }
  }

  def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): IO[Boolean] = mongo {
    requests.updateOne(ownedClaim(claim, now), Updates.set("deltaPurgedAt", Date.from(at))).getMatchedCount == 1L
  }

  def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): IO[Boolean] = mongo {
    requests.updateOne(ownedClaim(claim, now), Updates.set("deltaGeneration", generation)).getMatchedCount == 1L
  }

  def persistAffectedRows(claim: ErasureClaim, affectedRows: Long, now: Instant): IO[Boolean] =
    if (affectedRows < 0L) IO.raiseError(AnalyticsError.InvalidConfiguration("affected row count cannot be negative"))
    else mongo {
      requests.updateOne(ownedClaim(claim, now), Updates.max("deltaAffectedRows", affectedRows)).getMatchedCount == 1L
    }

  /** Stores exact pre-purge Delta file identities before the rewrite can invalidate them. */
  def persistDeltaFiles(claim: ErasureClaim, files: Vector[String], now: Instant): IO[Boolean] =
    if (files.distinct.size != files.size || files.exists(path => path == null || path.trim.isEmpty))
      IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure file evidence is malformed"))
    else mongo {
      val operations: java.util.List[WriteModel[Document]] = files.map { path =>
        val id = java.util.UUID.nameUUIDFromBytes(
          (claim.requestId + "\u0000" + path).getBytes(java.nio.charset.StandardCharsets.UTF_8)
        ).toString
        new ReplaceOneModel[Document](
          Filters.eq("_id", id),
          new Document("_id", id).append("requestId", claim.requestId).append("filePath", path),
          new ReplaceOptions().upsert(true)
        ): WriteModel[Document]
      }.asJava
      val session = client.startSession()
      try {
        session.startTransaction()
        val ownership = requests.updateOne(
          session,
          ownedClaim(claim, now),
          Updates.inc("deltaEvidenceRevision", 1L)
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
      } finally session.close()
    }

  def readDeltaFiles(requestId: String): IO[Vector[String]] = mongo {
    val iterator = deltaEvidence.find(Filters.eq("requestId", requestId)).iterator()
    try iterator.asScala.map(document => Option(document.getString("filePath")).getOrElse(throw AnalyticsError.MalformedMarker)).toVector
    finally iterator.close()
  }

  def readAffectedRows(requestId: String): IO[Long] = mongo {
    Option(requests.find(Filters.eq("_id", requestId)).first())
      .flatMap(document => Option(document.getLong("deltaAffectedRows"))).fold(0L)(_.longValue())
  }

  def readDeltaGeneration(requestId: String): IO[Option[Long]] = mongo {
    Option(requests.find(Filters.eq("_id", requestId)).first())
      .flatMap(document => Option(document.getLong("deltaGeneration"))).map(_.longValue())
  }

  def readDeltaPurgedAt(requestId: String): IO[Option[Instant]] = mongo {
    Option(requests.find(Filters.eq("_id", requestId)).first())
      .flatMap(document => Option(document.getDate("deltaPurgedAt"))).map(_.toInstant)
  }

  /** Lets other requests progress while preserving the durable checkpoint for a final publication barrier. */
  def releaseForOtherRequests(claim: ErasureClaim, now: Instant): IO[Boolean] = mongo {
    val result = requests.updateOne(
      ownedClaim(claim, now),
      Updates.combine(Updates.unset("leaseToken"), Updates.unset("leaseUntil"))
    )
    result.getMatchedCount == 1L
  }

  def defer(claim: ErasureClaim, resumeAt: Instant, now: Instant): IO[Boolean] =
    if (!resumeAt.isAfter(now)) IO.raiseError(AnalyticsError.InvalidConfiguration("deferred retry must be in the future"))
    else mongo {
      val result = requests.updateOne(
        ownedClaim(claim, now),
        Updates.combine(
          Updates.set("resumeAfter", Date.from(resumeAt)),
          Updates.unset("leaseToken"),
          Updates.unset("leaseUntil")
        )
      )
      result.getMatchedCount == 1L
    }

  def hasNonReadyOtherRequests(requestId: String): IO[Boolean] = mongo {
    val filter = Filters.and(
      Filters.ne("_id", requestId),
      Filters.in("state", "Pending", "Processing"),
      Filters.ne("phase", ErasurePhase.ReadyToPublish.toString)
    )
    requests.find(filter).limit(1).iterator().hasNext
  }

  /** A lease can only be extended while its token is current and its prior lease is still live. */
  def renew(claim: ErasureClaim, now: Instant, leaseUntil: Instant): IO[Boolean] =
    if (!leaseUntil.isAfter(now)) IO.raiseError(AnalyticsError.InvalidConfiguration("lease expiry must follow current time"))
    else mongo {
      val result = requests.updateOne(
        ownedClaim(claim, now),
        Updates.set("leaseUntil", Date.from(leaseUntil))
      )
      result.getMatchedCount == 1L
    }

  /**
    * Persists ordered, named progress. A compare-and-set on progressKey prevents an old
    * worker from moving progress backwards or writing after another worker reclaimed it.
    */
  def advance(claim: ErasureClaim, phase: ErasurePhase, progress: Int, now: Instant): IO[Boolean] =
    if (progress < 0 || progress >= ProgressPerPhase)
      IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure progress is out of bounds"))
    else {
      val nextKey = phase.ordinal.toLong * ProgressPerPhase.toLong + progress.toLong
      if (nextKey < claim.progressKey)
        IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure progress cannot move backwards"))
      else if (phase.ordinal > claim.phase.ordinal + 1)
        IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure phases cannot be skipped"))
      else if (nextKey == claim.progressKey) mongo {
        val filter = Filters.and(ownedClaim(claim, now), progressKeyFilter(nextKey))
        requests.updateOne(filter, Updates.set("progressKey", nextKey)).getMatchedCount == 1L
      }
      else mongo {
        // Compare against the exact value observed by this claim. A delayed retry from the same
        // lease must not overwrite a later durable checkpoint.
        val filter = Filters.and(ownedClaim(claim, now), progressKeyFilter(claim.progressKey))
        val update = Updates.combine(
          Updates.set("phase", phase.toString),
          Updates.set("progress", progress),
          Updates.set("progressKey", nextKey)
        )
        requests.updateOne(filter, update).getMatchedCount == 1L
      }
    }

  /** Marks the shared worker health lease live after startup or a successful work cycle. */
  def heartbeat(now: Instant, leaseUntil: Instant): IO[Unit] =
    if (!leaseUntil.isAfter(now)) IO.raiseError(AnalyticsError.InvalidConfiguration("heartbeat expiry must follow current time"))
    else mongo {
      heartbeats.updateOne(
        Filters.eq("_id", HeartbeatId),
        Updates.combine(
          Updates.set("state", "Ready"),
          Updates.set("leaseUntil", Date.from(leaseUntil)),
          Updates.set("updatedAt", Date.from(now))
        ),
        new com.mongodb.client.model.UpdateOptions().upsert(true)
      )
      ()
    }

  def preflight: IO[Unit] = mongo {
    val required = Set(
      "analytics_erasure_requests",
      "analytics_erasure_completions",
      "analytics_erasure_delta_files",
      "analytics_worker_heartbeats",
      "analytics_report_snapshots",
      "analytics_report_control",
      "analytics_report_runs",
      "event_outbox",
      "outbox_subject_fences",
      "users"
    )
    val names = database.listCollectionNames().into(new java.util.ArrayList[String]()).asScala.toSet
    val missing = required.diff(names)
    if (missing.nonEmpty)
      throw AnalyticsError.InvalidConfiguration("analytics erasure worker collections are missing: " + missing.toVector.sorted.mkString(","))
    requireSubjectReferenceMigration()
    requireNoUnverifiedOutboxRows()
  }

  private def requireSubjectReferenceMigration(): Unit = {
    val ledger = database.getCollection("hiring_migration_ledger", classOf[Document])
    val migration = ledger.find(Filters.eq("_id", "003_event_outbox_subject_references")).first()
    if (migration == null || migration.getString("state") != "Complete")
      throw AnalyticsError.InvalidConfiguration("outbox subject-reference migration is incomplete")
  }

  private def requireNoUnverifiedOutboxRows(): Unit = {
    val validSubject = new Document("$and", List(
      new Document("$eq", List(new Document("$type", "$$subject"), "string").asJava),
      new Document("$regexMatch", new Document("input", "$$subject")
        .append("regex", "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
    ).asJava)
    val validArray = new Document("$and", List(
      new Document("$gt", List(new Document("$size", "$subjectIds"), 0).asJava),
      new Document("$allElementsTrue", List(new Document("$map", new Document("input", "$subjectIds")
        .append("as", "subject").append("in", validSubject))).asJava)
    ).asJava)
    val invalidArray = new Document("$expr", new Document("$cond", List(
      new Document("$isArray", "$subjectIds"),
      new Document("$not", List(validArray).asJava),
      true
    ).asJava))
    val incomplete = Filters.or(
      Filters.exists("subjectRefsVersion", false),
      Filters.ne("subjectRefsVersion", 1),
      Filters.exists("subjectIds", false),
      invalidArray
    )
    if (outbox.find(incomplete).limit(1).first() != null)
      throw AnalyticsError.InvalidConfiguration("outbox contains rows without verified subject references")
  }

  private def ownedClaim(claim: ErasureClaim, now: Instant) = MongoAnalyticsErasureWorkerStore.ownedClaimFilter(claim, now)

  private def progressKeyFilter(value: Long) =
    if (value == 0L) Filters.or(Filters.eq("progressKey", 0L), Filters.exists("progressKey", false))
    else Filters.eq("progressKey", value)

  private def mongo[A](work: => A): IO[A] =
    IO.blocking(work).adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.MarkerStorageFailure(cause)
    }
}

private[analytics] object MongoAnalyticsErasureWorkerStore {
  val RequestCollection = "analytics_erasure_requests"
  val HeartbeatCollection = "analytics_worker_heartbeats"
  val HeartbeatId = "analytics-erasure"
  val MaximumClaimPageSize = 100
  val ProgressPerPhase = 1000000

  private[analytics] def ownedClaimFilter(claim: ErasureClaim, now: Instant) =
    Filters.and(
      Filters.eq("_id", claim.requestId),
      Filters.eq("state", "Processing"),
      Filters.eq("leaseToken", claim.leaseToken),
      Filters.gt("leaseUntil", Date.from(now))
    )

  private[analytics] def decodeClaim(document: Document): Option[ErasureClaim] = {
    val requestId = Option(document.getString("_id"))
    val token = Option(document.getString("leaseToken"))
    val until = Option(document.getDate("leaseUntil")).map(_.toInstant)
    val phase = Option(document.getString("phase")).fold(Some(ErasurePhase.Requested))(ErasurePhase.fromString)
    val progress = Option(document.getInteger("progress")).map(_.intValue()).getOrElse(0)
    val progressKey = Option(document.getLong("progressKey")).map(_.longValue()).getOrElse(0L)
    for {
      _ <- Option(document.getInteger("fencingVersion")).filter(_.intValue() == 1)
      id <- requestId
      _ <- scala.util.Try(UUID.fromString(id)).toOption.filter(_.toString == id)
      leaseToken <- token
      _ <- scala.util.Try(UUID.fromString(leaseToken)).toOption.filter(_.toString == leaseToken)
      leaseExpiry <- until
      currentPhase <- phase
      if progress >= 0 && progress < ProgressPerPhase &&
        progressKey == currentPhase.ordinal.toLong * ProgressPerPhase.toLong + progress.toLong
    } yield ErasureClaim(id, leaseToken, leaseExpiry, currentPhase, progress, progressKey)
  }
}

final case class ErasureClaim(
    requestId: String,
    leaseToken: String,
    leaseUntil: Instant,
    phase: ErasurePhase,
    progress: Int,
    progressKey: Long
)

/** Ordered durable stages; the worker owns the meaning and idempotent action of each stage. */
enum ErasurePhase {
  case Requested
  case PublisherDrained
  case OutboxPurged
  case DeltaPurged
  case GoldRebuilt
  case ReadyToPublish
  case ReportPublished

  def precedes(other: ErasurePhase): Boolean = ordinal < other.ordinal
}

object ErasurePhase {
  def fromString(value: String): Option[ErasurePhase] = values.find(_.toString == value)
}
