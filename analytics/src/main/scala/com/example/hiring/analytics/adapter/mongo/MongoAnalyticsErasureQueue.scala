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
import com.mongodb.client.model.{FindOneAndUpdateOptions, Filters, Projections, ReturnDocument, Sorts, Updates}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Mongo persistence for erasure claiming, publisher drain, worker liveness, and repair operations. */
final class MongoAnalyticsErasureQueue[F[_]: Async] private (
    database: MongoDatabase[F],
    requests: MongoCollection[F, AnalyticsMongoRecords.ErasureRequest],
    heartbeats: MongoCollection[F, AnalyticsMongoRecords.WorkerHeartbeat],
    fences: MongoCollection[F, AnalyticsMongoRecords.PublisherFence],
    outbox: MongoCollection[F, AnalyticsMongoRecords.EventOutboxReferences],
    ledger: MongoCollection[F, AnalyticsMongoRecords.MigrationEntry],
    streams: MongoPublisherStream
) extends MongoAnalyticsErasureStoreSupport[F](streams)
    with ErasureQueue[F] {
  import MongoAnalyticsErasureStoreSupport.*

  final case class RepairRequest(requestId: AccountSubjectId, phase: String, attemptCount: Int, failureCategory: String)

  private def decodeRepairRequest(
      document: AnalyticsMongoRecords.ErasureRequest
  ): Either[AnalyticsError, RepairRequest] = {
    val malformed = AnalyticsError.MalformedMarker
    for {
      subjectId <- AccountSubjectId.from(document._id).leftMap(_ => malformed)
      phase = document.phase.getOrElse(ErasurePhase.Requested.persistedName)
      _ <- Either.cond(ErasurePhase.fromString(phase).nonEmpty, (), malformed)
      attempt <- document.attemptCount.toRight(malformed)
      category <- document.failureCategory.toRight(malformed)
      _ <- Either.cond(ErasureFailureCategory.values.exists(_.persistedName == category), (), malformed)
    } yield RepairRequest(subjectId, phase, attempt, category)
  }

  def inspectRepairRequests(limit: Int): F[Vector[RepairRequest]] =
    if (limit <= 0 || limit > MaximumClaimPageSize)
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("repair inspection limit is out of bounds"))
    else
      mongo {
        streams
          .stream { capacity =>
            requests
              .find(Filters.eq(AnalyticsCollections.Fields.RepairRequired, true))
              .projection(
                Projections.include(
                  AnalyticsCollections.Fields.Id,
                  AnalyticsCollections.Fields.Phase,
                  AnalyticsCollections.Fields.AttemptCount,
                  AnalyticsCollections.Fields.FailureCategory
                )
              )
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
        .projection(
          Projections.include(
            AnalyticsCollections.Fields.Id,
            AnalyticsCollections.Fields.FencingVersion,
            AnalyticsCollections.Fields.Phase,
            AnalyticsCollections.Fields.LeaseToken,
            AnalyticsCollections.Fields.LeaseUntil,
            AnalyticsCollections.Fields.Progress,
            AnalyticsCollections.Fields.ProgressKey,
            AnalyticsCollections.Fields.AttemptCount
          )
        )
        .returnDocument(ReturnDocument.AFTER)
      requests
        .findOneAndUpdate(nonFinalizer, update, options)
        .flatMap {
          case some @ Some(_) => Async[F].pure(some)
          case None           => requests.findOneAndUpdate(finalizer, update, options)
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
    fences
      .find(Filters.eq(AnalyticsCollections.Fields.Id, subjectId.value))
      .projection(
        Projections.include(
          AnalyticsCollections.Fields.Id,
          AnalyticsCollections.Fields.Deleted,
          AnalyticsCollections.Fields.LeaseToken,
          AnalyticsCollections.Fields.LeaseUntil
        )
      )
      .first
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

  def transactionalIds(requestId: AccountSubjectId): F[Vector[String]] = mongo {
    requests
      .find(Filters.eq(AnalyticsCollections.Fields.Id, requestId.value))
      .projection(
        Projections.include(
          AnalyticsCollections.Fields.Id,
          AnalyticsCollections.Fields.FencingVersion,
          AnalyticsCollections.Fields.TransactionalIds
        )
      )
      .first
      .map(_.toRight(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing")))
      .map(_.flatMap { request =>
        if (request.fencingVersion.isEmpty || !request.fencingVersion.contains(1))
          Left(AnalyticsError.InvalidConfiguration("erasure request predates transactional publisher fencing"))
        else
          request.transactionalIds
            .toRight(AnalyticsError.MalformedMarker)
            .flatMap(values => Either.cond(values.forall(_.trim.nonEmpty), values, AnalyticsError.MalformedMarker))
            .map(_.distinct.sorted)
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
          migrationValidation(ledger).flatMap { _ =>
            val filter = Filters.in(
              AnalyticsCollections.Fields.SubjectIds,
              java.util.Collections.singletonList(subjectId.value)
            )
            outbox.deleteMany(filter, new com.mongodb.client.model.DeleteOptions()).void.as(filter)
          }
        }.flatMap { filter =>
          mongo {
            streams
              .stream(capacity =>
                outbox
                  .find(filter)
                  .projection(
                    Projections
                      .include(AnalyticsCollections.Fields.SubjectRefsVersion, AnalyticsCollections.Fields.SubjectIds)
                  )
                  .limit(1)
                  .boundedStream(capacity)
              )
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
    mongo {
      streams
        .stream(capacity =>
          requests
            .find(filter)
            .projection(Projections.include(AnalyticsCollections.Fields.Id))
            .limit(1)
            .boundedStream(capacity)
        )
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

  def heartbeat(now: Instant, leaseUntil: Instant): F[Unit] =
    if (!leaseUntil.isAfter(now))
      Async[F].raiseError(AnalyticsError.InvalidConfiguration("heartbeat expiry must follow current time"))
    else
      mongo {
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

  private def migrationValidation(ledger: MongoCollection[F, AnalyticsMongoRecords.MigrationEntry]): F[Unit] = {
    ledger
      .find(Filters.eq(AnalyticsCollections.Fields.Id, AnalyticsCollections.MigrationIds.OutboxSubjectReferences))
      .projection(Projections.include(AnalyticsCollections.Fields.Id, AnalyticsCollections.Fields.State))
      .first
      .flatMap(migration =>
        Either
          .cond(
            migration.exists(_.state.contains("Complete")),
            (),
            AnalyticsError.InvalidConfiguration("outbox subject-reference migration is incomplete")
          )
          .liftTo[F]
      )
  }

  private def outboxValidation(outbox: MongoCollection[F, AnalyticsMongoRecords.EventOutboxReferences]): F[Unit] =
    outbox
      .find(unverifiedOutboxFilter)
      .projection(Projections.include(AnalyticsCollections.Fields.Id))
      .limit(1)
      .first
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
}

object MongoAnalyticsErasureQueue {
  def resource[F[_]: Async](
      database: MongoDatabase[F],
      streams: MongoPublisherStream,
      collectionName: String = MongoAnalyticsErasureStoreSupport.RequestCollection
  ): Resource[F, MongoAnalyticsErasureQueue[F]] =
    for {
      requests <- Resource.eval(
        database.getCollection[AnalyticsMongoRecords.ErasureRequest](
          collectionName,
          AnalyticsMongoRecords.erasureRequestRegistry
        )
      )
      heartbeats <- Resource.eval(
        database.getCollection[AnalyticsMongoRecords.WorkerHeartbeat](
          MongoAnalyticsErasureStoreSupport.HeartbeatCollection,
          AnalyticsMongoRecords.workerHeartbeatRegistry
        )
      )
      fences <- Resource.eval(
        database.getCollection[AnalyticsMongoRecords.PublisherFence](
          AnalyticsCollections.OutboxSubjectFences,
          AnalyticsMongoRecords.publisherFenceRegistry
        )
      )
      outbox <- Resource.eval(
        database.getCollection[AnalyticsMongoRecords.EventOutboxReferences](
          AnalyticsCollections.EventOutbox,
          AnalyticsMongoRecords.eventOutboxReferencesRegistry
        )
      )
      ledger <- Resource.eval(
        database.getCollection[AnalyticsMongoRecords.MigrationEntry](
          AnalyticsCollections.HiringMigrationLedger,
          AnalyticsMongoRecords.migrationEntryRegistry
        )
      )
    } yield new MongoAnalyticsErasureQueue(database, requests, heartbeats, fences, outbox, ledger, streams)
}
