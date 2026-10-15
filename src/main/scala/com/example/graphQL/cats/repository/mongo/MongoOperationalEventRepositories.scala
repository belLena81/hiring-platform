package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.resilience.CircuitBreaker

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, OperationalEventJson, SearchSession}
import com.example.graphQL.cats.service.port.{
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  MutationWriteContext,
  OperationalEventOutboxRepository,
  RepositoryF,
  SearchSessionRepository
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions, Projections}
import org.bson.Document
import org.bson.types.Binary
import java.time.Instant
import com.example.graphQL.cats.repository.mongo.MongoDocumentFields.Repository.{instant, string}
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoSearchSessionRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics[IO]
) extends SearchSessionRepository[IO] {
  private val sessions = Mongo4catsCollections.documents(database, MongoCollections.SearchSessions)
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  override def save(session: SearchSession, event: OperationalEventEnvelope): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.save")(
        transactionRunner
          .run { active =>
            val document = MongoHiringCodecs.searchSession(session)
            val filter = MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, session.id.toString),
              MongoFilter.eq(MongoFields.ActorId, session.actorId.value.toString)
            )
            val update = MongoUpdate.combine(
              document.entrySet().asScala.toList.map(field => MongoUpdate.setOnInsert(field.getKey, field.getValue))*
            )
            val options = new UpdateOptions().upsert(true)
            val result = RepositoryF.lift(MongoSessionOperations.updateOne(sessions, active, filter, update, options))
            result.flatMap(value =>
              if (Option(value.getUpsertedId).nonEmpty)
                MongoOperationalEventInsertion.insert(outbox, active, List(event), session.occurredAt, diagnostics)
              else ().pure[[A] =>> RepositoryF[IO, A]]
            )
          }
      )(MongoErrors.duplicateAsConflict)

  override def find(id: UUID): RepositoryF[IO, Option[SearchSession]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.find")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .findOne(sessions, None, MongoFilter.eq(MongoFields.Id, id.toString))
          )
          .subflatMap(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readSearchSession))
          )
      )

  def recordInteraction(event: OperationalEventEnvelope): RepositoryF[IO, Boolean] =
    recordInteractionWithSession(event, None)

  override def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext
  ): RepositoryF[IO, Boolean] =
    RepositoryF.lift(MongoMutationWriteContext.session(context)).flatMap(recordInteractionWithSession(event, _))

  private def recordInteractionWithSession(
      event: OperationalEventEnvelope,
      session: Option[ClientSession[IO]]
  ): RepositoryF[IO, Boolean] =
    val insert = RepositoryF
      .at[IO]
      .fromEither(MongoHiringCodecs.outboxRecord(event, event.occurredAt).leftMap(_ => RepositoryError.InvalidEvent))
      .flatMap(document => RepositoryF.lift(MongoSessionOperations.insertOne(outbox, session, document).as(true)))
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.recordInteraction")(insert)(MongoErrors.duplicateAsConflict)
      .leftFlatMap {
        case RepositoryError.Conflict =>
          MongoRepositorySupport
            .repositoryGuard(diagnostics, "searchSession.recordInteraction.findDuplicate")(
              RepositoryF
                .lift(
                  MongoSessionOperations
                    .findOne(outbox, session, MongoFilter.eq(MongoFields.Id, event.eventId.toString))
                )
                .subflatMap {
                  case Some(existing) if sameEvent(existing, event) => Right(false)
                  case _                                            => Left(RepositoryError.Conflict)
                }
            )
        case error => RepositoryF.at[IO].raise(error)
      }

  private def sameEvent(document: Document, event: OperationalEventEnvelope): Boolean =
    MongoHiringCodecs
      .readOperationalEvent(document)
      .toOption
      .exists(existing =>
        existing.eventId == event.eventId &&
          existing.eventType == event.eventType &&
          existing.aggregateType == event.aggregateType &&
          existing.aggregateId == event.aggregateId &&
          existing.actorId == event.actorId &&
          existing.payload == event.payload
      )
}

final class MongoOperationalEventOutboxRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics[IO],
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends OperationalEventOutboxRepository[IO] {
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private val subjectFences = Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences)
  private val users = Mongo4catsCollections.documents(database, MongoCollections.Users)

  /** Stored outbox lifecycle values; string forms are the persisted contract. */
  private enum OutboxState {
    case Retryable, InFlight, Published, Failed
  }
  private val inFlight = MongoFilter.eq(MongoFields.State, OutboxState.InFlight.toString)

  private def updateMany(
      collection: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate
  ) = MongoSessionOperations.updateMany(collection, session, filter, update)

  override def claim(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryF[IO, List[ClaimedOperationalEvent]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "outbox.claim")(
      claimPage(workerId, transactionalId, now, leaseUntil, limit)
    )

  private def claimPage(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryF[IO, List[ClaimedOperationalEvent]] = {
    val cursors = Mongo4catsCollections.documents(database, MongoProducerRegistrations.ClaimCursorsCollection)
    def cursorFilter(row: Document): Either[RepositoryError, MongoFilter] = for {
      at <- instant(row, "availableAt").map(_.toDate)
      occurred <- instant(row, "occurredAt").map(_.toDate)
      id <- string(row, "eventId")
    } yield MongoFilter.or(
      MongoFilter.gt(MongoFields.AvailableAt, at),
      MongoFilter.and(MongoFilter.eq(MongoFields.AvailableAt, at), MongoFilter.gt(MongoFields.OccurredAt, occurred)),
      MongoFilter.and(
        MongoFilter.eq(MongoFields.AvailableAt, at),
        MongoFilter.eq(MongoFields.OccurredAt, occurred),
        MongoFilter.gt(MongoFields.Id, id)
      )
    )
    def page(after: Option[MongoFilter]): RepositoryF[IO, List[Document]] = RepositoryF
      .lift(
        outbox.flatMap(
          _.find(after.fold(due(now))(value => MongoFilter.and(due(now), value)).bson)
            .sort(Sorts.ascending(MongoFields.AvailableAt, MongoFields.OccurredAt, MongoFields.Id))
            .projection(Projections.include(MongoFields.Id, MongoFields.AvailableAt, MongoFields.OccurredAt))
            .limit(MongoProducerRegistrations.BatchSize)
            .all
        )
      )
      .subflatMap(_.toList.traverse { row =>
        (string(row, MongoFields.Id), instant(row, MongoFields.AvailableAt), instant(row, MongoFields.OccurredAt))
          .mapN((_, _, _) => row)
      })
    def checkpoint(row: Document): RepositoryF[IO, Unit] = RepositoryF
      .lift(
        MongoSessionOperations.updateOne(
          cursors,
          None,
          MongoFilter.eq("_id", workerId),
          MongoUpdate.combine(
            MongoUpdate.set("eventId", row.getString("_id")),
            MongoUpdate.set("availableAt", row.getDate("availableAt")),
            MongoUpdate.set("occurredAt", row.getDate("occurredAt"))
          ),
          new UpdateOptions().upsert(true)
        )
      )
      .void
    def scan(
        rows: List[Document],
        values: List[ClaimedOperationalEvent],
        last: Option[Document]
    ): RepositoryF[IO, List[ClaimedOperationalEvent]] =
      if (rows.isEmpty || values.size >= limit) last.traverse_(checkpoint).as(values)
      else
        rows match {
          case row :: tail =>
            claimOne(workerId, transactionalId, now, leaseUntil, row.getString("_id"))
              .leftFlatMap {
                case RepositoryError.Conflict => RepositoryF.at[IO].pure(None)
                case error                    => RepositoryF.at[IO].raise(error)
              }
              .flatMap(value => scan(tail, values ++ value.toList, Some(row)))
          case Nil => last.traverse_(checkpoint).as(values)
        }
    for {
      current <- RepositoryF.lift(MongoSessionOperations.findOne(cursors, None, MongoFilter.eq("_id", workerId)))
      after <- RepositoryF.at[IO].fromEither(current.traverse(cursorFilter))
      found <- page(after)
      rows <- if (found.isEmpty && after.nonEmpty) page(None) else RepositoryF.at[IO].pure(found)
      result <- scan(rows, Nil, None)
    } yield result
  }

  private def due(now: Instant): MongoFilter = MongoLeaseQueue.claimable(
    MongoFilter.and(
      MongoFilter.eq(MongoFields.State, OutboxState.Retryable.toString),
      MongoFilter.lte(MongoFields.AvailableAt, now.toDate)
    ),
    inFlight,
    now
  )

  override def markPublished(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): RepositoryF[IO, Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, OutboxState.Published.toString),
        MongoUpdate.set(MongoFields.PublishedAt, now.toDate),
        MongoUpdate.set(MongoFields.RetentionExpiresAt, retentionExpiresAt.toDate),
        MongoLeaseQueue.releaseLease,
        MongoUpdate.set(MongoFields.UpdatedAt, now.toDate)
      )
    ).flatMap(_ => releaseSubjectLeases(leaseToken))

  override def releaseForRetry(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): RepositoryF[IO, Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, OutboxState.Retryable.toString),
        MongoUpdate.set(MongoFields.AvailableAt, availableAt.toDate),
        MongoLeaseQueue.releaseLease,
        MongoUpdate.set(MongoFields.UpdatedAt, now.toDate)
      )
    ).flatMap(_ => releaseSubjectLeases(leaseToken))

  override def markFailed(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): RepositoryF[IO, Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, OutboxState.Failed.toString),
        MongoUpdate.set(MongoFields.LastError, reason.take(512)),
        MongoLeaseQueue.releaseLease,
        MongoUpdate.set(MongoFields.UpdatedAt, now.toDate)
      )
    ).flatMap(_ => releaseSubjectLeases(leaseToken))

  override def renewLease(
      eventId: UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseUntil: Instant
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.renewLease")(
        transactionRunner
          .run { session =>
            val held = MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, eventId.toString),
              inFlight,
              MongoFilter.eq(MongoFields.LeaseToken, leaseToken)
            )
            MongoLeaseQueue.renewHeld(outbox, session, held, leaseUntil).flatMap {
              case true =>
                RepositoryF
                  .lift(
                    updateMany(
                      subjectFences,
                      session,
                      MongoFilter.and(
                        MongoFilter.in(MongoFields.Id, subjectIds),
                        MongoFilter.eq(MongoFields.LeaseToken, leaseToken),
                        MongoFilter.ne(MongoFields.Deleted, true)
                      ),
                      MongoUpdate.set(MongoFields.LeaseUntil, leaseUntil.toDate)
                    )
                  )
                  .subflatMap(result =>
                    Either.cond(result.getMatchedCount == subjectIds.size.toLong, (), RepositoryError.Conflict)
                  )
              case false => RepositoryF.at[IO].raise(RepositoryError.Conflict)
            }
          }
      )

  private def claimOne(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      eventId: String
  ): RepositoryF[IO, Option[ClaimedOperationalEvent]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.claimOne")(transactionRunner.run { session =>
        claimOneInSession(session, workerId, now, leaseUntil, eventId).flatMap {
          case None        => RepositoryF.at[IO].pure(None)
          case Some(claim) =>
            subjectIsDeleted(session, claim).flatMap {
              case true  => suppressDeletedClaim(session, claim, now).as(None)
              case false => acquireSubjectLeases(session, claim, transactionalId, now, leaseUntil).as(Some(claim))
            }
        }
      })

  private def claimOneInSession(
      session: Option[ClientSession[IO]],
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      eventId: String
  ): RepositoryF[IO, Option[ClaimedOperationalEvent]] =
    RepositoryF.lift(uuidGen.randomUUID).flatMap { id =>
      val token = id.toString
      MongoRepositorySupport
        .transactionGuard(diagnostics, "outbox.claimOne", session)(
          MongoLeaseQueue
            .claimNext(
              outbox,
              session,
              MongoFilter.and(due(now), MongoFilter.eq(MongoFields.Id, eventId)),
              MongoUpdate.combine(
                MongoLeaseQueue.leaseStamp(OutboxState.InFlight.toString, workerId, token, leaseUntil, now),
                MongoUpdate.inc(MongoFields.Attempts, 1)
              ),
              Sorts.ascending(MongoFields.AvailableAt, MongoFields.OccurredAt, MongoFields.Id)
            ) { document =>
              // Keep attributable control evidence even when the immutable event cannot be interpreted.
              RepositoryF
                .at[IO]
                .fromEither(
                  (
                    requiredSubjectIds(document),
                    requiredSubjectRefsVersion(document),
                    MongoDocumentFields.requiredInt(document, MongoFields.Attempts, min = 0)
                  )
                    .mapN((_, _, _) => ())
                    .leftMap(_ => RepositoryError.InvalidStoredData)
                )
                .flatMap { _ =>
                  readClaimed(document) match {
                    case Right(claim) => RepositoryF.at[IO].pure(Some(claim))
                    case Left(_)      => failInvalidClaim(session, eventId, token, now).as(None)
                  }
                }
            }
            .map(_.flatten)
        )
    }

  private def failInvalidClaim(
      session: Option[ClientSession[IO]],
      eventId: String,
      token: String,
      now: Instant
  ): RepositoryF[IO, Unit] =
    RepositoryF
      .lift(
        MongoSessionOperations.updateOne(
          outbox,
          session,
          MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, eventId),
            inFlight,
            MongoFilter.eq(MongoFields.LeaseToken, token)
          ),
          MongoUpdate.combine(
            MongoUpdate.set(MongoFields.State, OutboxState.Failed.toString),
            MongoUpdate.set(MongoFields.LastError, "INVALID_EVENT_CONTRACT"),
            MongoUpdate.set(MongoFields.UpdatedAt, now.toDate),
            MongoLeaseQueue.releaseLease
          )
        )
      )
      .subflatMap(MongoRepositorySupport.matchedOne(_))

  private def acquireSubjectLeases(
      session: Option[ClientSession[IO]],
      claim: ClaimedOperationalEvent,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryF[IO, Unit] = {
    val leaseAvailable = MongoFilter.or(
      MongoFilter.exists(MongoFields.LeaseUntil, false),
      MongoFilter.lte(MongoFields.LeaseUntil, now.toDate)
    )
    val options = new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
    def acquireOne(subjectId: String): RepositoryF[IO, Unit] = {
      val filter = MongoFilter.and(
        MongoFilter.eq(MongoFields.Id, subjectId),
        MongoFilter.ne(MongoFields.Deleted, true),
        leaseAvailable
      )
      val update = MongoUpdate.combine(
        MongoUpdate.setOnInsert(MongoFields.Id, subjectId),
        MongoUpdate.set(MongoFields.LeaseEventId, claim.event.eventId.toString),
        MongoUpdate.set(MongoFields.LeaseToken, claim.leaseToken),
        MongoUpdate.set(MongoFields.LeaseUntil, leaseUntil.toDate)
      )
      MongoRepositorySupport
        .transactionGuard(diagnostics, "outbox.acquireSubjectLease", session)(
          RepositoryF
            .lift(MongoSessionOperations.findOneAndUpdate(subjectFences, session, filter, update, options))
            .subflatMap {
              case Some(_) => Right(())
              case None    => Left(RepositoryError.Conflict)
            }
        ) {
          case MongoDuplicateKey(_) =>
            Left(RepositoryError.Conflict)
          case error: MongoCommandException if error.getErrorCode == MongoDuplicateKey.Code =>
            Left(RepositoryError.Conflict)
          case _ => Left(RepositoryError.Unavailable)
        }
    }
    MongoOutboxSubjectLeases.acquire(claim.subjectIds)(subject =>
      acquireOne(subject) *>
        MongoProducerRegistrations.register(database, session, subject, transactionalId, "Operational", now)
    )
  }

  private def subjectIsDeleted(
      session: Option[ClientSession[IO]],
      claim: ClaimedOperationalEvent
  ): RepositoryF[IO, Boolean] = {
    val filter = MongoFilter.and(
      MongoFilter.in(MongoFields.Id, claim.subjectIds),
      MongoFilter.eq(MongoFields.AccountStatus, "Deleted")
    )
    MongoRepositorySupport
      .transactionGuard(diagnostics, "outbox.subjectIsDeleted", session)(
        RepositoryF
          .lift(
            MongoSessionOperations
              .findOne(users, session, filter)
          )
          .map(_.nonEmpty)
      )
  }

  private def suppressDeletedClaim(
      session: Option[ClientSession[IO]],
      claim: ClaimedOperationalEvent,
      now: Instant
  ): RepositoryF[IO, Unit] = {
    val filter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, claim.event.eventId.toString),
      inFlight,
      MongoFilter.eq(MongoFields.LeaseToken, claim.leaseToken)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.State, OutboxState.Failed.toString),
      MongoUpdate.set(MongoFields.LastError, "SUBJECT_DELETED"),
      MongoUpdate.set(MongoFields.UpdatedAt, now.toDate),
      MongoLeaseQueue.releaseLease
    )
    MongoRepositorySupport.transactionGuard(diagnostics, "outbox.suppressDeletedClaim", session)(
      RepositoryF
        .lift(MongoSessionOperations.updateOne(outbox, session, filter, update))
        .subflatMap(MongoRepositorySupport.matchedOne(_))
    )
  }

  private def releaseSubjectLeases(leaseToken: String): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.releaseSubjectLeases")(
        RepositoryF
          .lift(
            updateMany(
              subjectFences,
              None,
              MongoFilter.eq(MongoFields.LeaseToken, leaseToken),
              MongoUpdate.combine(
                MongoUpdate.unset(MongoFields.LeaseEventId),
                MongoUpdate.unset(MongoFields.LeaseToken),
                MongoUpdate.unset(MongoFields.LeaseUntil)
              )
            )
          )
          .subflatMap(result => Either.cond(result.getMatchedCount > 0L, (), RepositoryError.Conflict))
      )

  private def updateClaimed(
      eventId: UUID,
      leaseToken: String,
      update: MongoUpdate
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.updateClaimed")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .updateOne(
                outbox,
                None,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, eventId.toString),
                  inFlight,
                  MongoFilter.eq(MongoFields.LeaseToken, leaseToken)
                ),
                update
              )
          )
          .subflatMap(MongoRepositorySupport.matchedOne(_))
      )

  private def readClaimed(
      document: Document
  ): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], ClaimedOperationalEvent] =
    (
      MongoHiringCodecs.readOperationalEvent(document),
      envelopeBytes(document).toValidatedNel,
      MongoDocumentFields.requiredString(document, MongoFields.PartitionKey).toValidatedNel,
      MongoDocumentFields.requiredString(document, MongoFields.LeaseToken).toValidatedNel,
      MongoDocumentFields.requiredInt(document, MongoFields.Attempts, min = 0).toValidatedNel,
      requiredSubjectIds(document).toValidatedNel,
      requiredSubjectRefsVersion(document).toValidatedNel
    ).mapN((event, bytes, key, token, attempts, subjects, _) =>
      ClaimedOperationalEvent(event, bytes, key, token, attempts, subjects)
    ).toEither
      .flatMap { claim =>
        val invalid = NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument)
        OperationalEventJson.decode(claim.envelopeBytes).leftMap(_ => invalid).flatMap { wire =>
          // BSON dates have millisecond precision; the immutable wire bytes retain the original Instant.
          val storedTime = wire.occurredAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
          val consistent = wire.copy(occurredAt = storedTime) == claim.event &&
            claim.partitionKey == wire.partitionKey &&
            MongoHiringCodecs.outboxSubjectIds(wire).contains(claim.subjectIds.sorted)
          Either.cond(consistent, claim.copy(event = wire), invalid)
        }
      }

  private def envelopeBytes(document: Document): Either[MongoHiringCodecs.StoredDocumentError, Array[Byte]] =
    Option(document.get(MongoFields.EnvelopeBytes)) match {
      case Some(value: Array[Byte]) => Right(value)
      case Some(value: Binary)      => Right(value.getData)
      case _ => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.EnvelopeBytes))
    }

  private def requiredSubjectIds(
      document: Document
  ): Either[MongoHiringCodecs.StoredDocumentError, List[String]] =
    MongoDocumentFields.requiredStringList(document, MongoFields.SubjectIds).flatMap { subjects =>
      Either.cond(
        subjects.nonEmpty && subjects.distinct.size == subjects.size &&
          subjects.forall(MongoDocumentFields.parseUuid(MongoFields.SubjectIds, _).isRight),
        subjects,
        MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.SubjectIds)
      )
    }

  private def requiredSubjectRefsVersion(
      document: Document
  ): Either[MongoHiringCodecs.StoredDocumentError, Int] =
    MongoDocumentFields
      .requiredInt(document, MongoFields.SubjectRefsVersion)
      .filterOrElse(_ == 1, MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.SubjectRefsVersion))
}

object MongoOperationalEventOutboxRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics[IO],
      uuidGen: UUIDGen[IO] = UUIDGen[IO],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): MongoOperationalEventOutboxRepository =
    new MongoOperationalEventOutboxRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics, breaker = breaker),
      diagnostics,
      uuidGen
    )
}

final class MongoConsumerReceiptRepository(database: MongoDatabase[IO], diagnostics: Diagnostics[IO])
    extends ConsumerReceiptRepository[IO] {
  private val receipts = Mongo4catsCollections.documents(database, MongoCollections.ConsumerReceipts)

  override def exists(consumerGroup: String, eventId: UUID): RepositoryF[IO, Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.exists")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .findOne(receipts, None, MongoFilter.eq(MongoFields.Id, s"$consumerGroup:${eventId.toString}"))
          )
          .map(_.nonEmpty)
      )

  override def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): RepositoryF[IO, Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.record")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .insertOne(
                receipts,
                None,
                new Document(MongoFields.Id, s"$consumerGroup:${event.eventId.toString}")
                  .append(MongoFields.ConsumerGroup, consumerGroup)
                  .append(MongoFields.EventId, event.eventId.toString)
                  .append(MongoFields.AggregateType, event.aggregateType.toString)
                  .append(MongoFields.AggregateId, event.aggregateId)
                  .append(MongoFields.CreatedAt, now.toDate)
                  .append(MongoFields.ExpiresAt, expiresAt.toDate)
              )
          )
          .as(true)
      ) {
        case MongoDuplicateKey(_) => Right(false)
        case _                    => Left(RepositoryError.Unavailable)
      }

}

final class MongoEventQuarantineRepository(database: MongoDatabase[IO], diagnostics: Diagnostics[IO])
    extends EventQuarantineRepository[IO] {
  private val quarantine = Mongo4catsCollections.documents(database, MongoCollections.EventQuarantine)

  override def save(record: EventQuarantineRecord): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "eventQuarantine.save")(
        RepositoryF
          .lift(
            MongoSessionOperations
              .insertOne(
                quarantine,
                None,
                new Document()
                  .append(MongoFields.Topic, record.topic)
                  .append(MongoFields.Partition, java.lang.Integer.valueOf(record.partition))
                  .append(MongoFields.Offset, java.lang.Long.valueOf(record.offset))
                  .append(MongoFields.Category, record.category.toString)
                  .append(MongoFields.Reason, record.reason.take(512))
                  .append(MongoFields.RawBytes, record.rawBytes)
                  .append(MongoFields.OccurredAt, record.occurredAt.toDate)
                  .append(MongoFields.ExpiresAt, record.expiresAt.toDate)
              )
          )
          .void
      ) {
        case MongoDuplicateKey(_) => Right(())
        case _                    => Left(RepositoryError.Unavailable)
      }

}

object MongoSearchSessionRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics[IO],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): MongoSearchSessionRepository =
    new MongoSearchSessionRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics, breaker = breaker),
      diagnostics
    )
}

private[mongo] object MongoOutboxSubjectLeases {
  def acquire(subjectIds: List[String])(acquireOne: String => RepositoryF[IO, Unit]): RepositoryF[IO, Unit] =
    if (subjectIds.isEmpty) RepositoryF.at[IO].raise(RepositoryError.InvalidStoredData)
    else subjectIds.sorted.traverse_(acquireOne)
}
