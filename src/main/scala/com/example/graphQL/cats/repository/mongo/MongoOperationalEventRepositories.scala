package com.example.graphQL.cats.repository.mongo

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.service.port.{
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  MutationWriteContext,
  OperationalEventOutboxRepository,
  RepositoryIO,
  SearchSessionRepository
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.{MongoCommandException, MongoWriteException}
import com.mongodb.client.model.{FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions, Projections}
import org.bson.Document
import org.bson.types.Binary
import java.time.Instant
import java.util.Date
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoSearchSessionRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics
) extends SearchSessionRepository
    with MongoOperationalEventInsertion
    with MongoConflictWriteMapping {
  private val sessions = Mongo4catsCollections.documents(database, MongoCollections.SearchSessions)
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  override def save(session: SearchSession, event: OperationalEventEnvelope): RepositoryIO[Unit] =
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
            val result = RepositoryIO.lift(MongoSessionOperations.updateOne(sessions, active, filter, update, options))
            result.flatMap {
              case Some(value) if Option(value.getUpsertedId).nonEmpty =>
                insertOperationalEvents(outbox, active, List(event), session.occurredAt, diagnostics)
              case Some(_) => RepositoryIO.fromEither(Right(()))
              case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
            }
          }
      )(mapWrite)

  override def find(id: UUID): RepositoryIO[Option[SearchSession]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.find")(
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(sessions, None, MongoFilter.eq(MongoFields.Id, id.toString))
          )
          .subflatMap(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readSearchSession))
          )
      )(_ => Left(RepositoryError.Unavailable))

  def recordInteraction(event: OperationalEventEnvelope): RepositoryIO[Boolean] =
    recordInteractionWithSession(event, None)

  override def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext
  ): RepositoryIO[Boolean] =
    RepositoryIO.lift(MongoMutationWriteContext.session(context)).flatMap(recordInteractionWithSession(event, _))

  private def recordInteractionWithSession(
      event: OperationalEventEnvelope,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Boolean] =
    val insert = RepositoryIO
      .fromEither(MongoHiringCodecs.outboxRecord(event, event.occurredAt).leftMap(_ => RepositoryError.Unavailable))
      .flatMap(document => RepositoryIO.lift(MongoSessionOperations.insertOne(outbox, session, document)))
      .subflatMap(_.fold[Either[RepositoryError, Boolean]](Left(RepositoryError.MissingWriteResult))(_ => Right(true)))
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.recordInteraction")(insert)(mapWrite)
      .leftFlatMap {
        case RepositoryError.Conflict =>
          MongoRepositorySupport
            .repositoryGuard(diagnostics, "searchSession.recordInteraction.findDuplicate")(
              RepositoryIO
                .lift(
                  MongoSessionOperations
                    .findOne(outbox, session, MongoFilter.eq(MongoFields.Id, event.eventId.toString))
                )
                .subflatMap {
                  case Some(existing) if sameEvent(existing, event) => Right(false)
                  case _                                            => Left(RepositoryError.Conflict)
                }
            )(_ => Left(RepositoryError.Unavailable))
        case error => RepositoryIO.fromEither(Left(error))
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
    diagnostics: Diagnostics,
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends OperationalEventOutboxRepository {
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private val subjectFences = Mongo4catsCollections.documents(database, MongoCollections.OutboxSubjectFences)
  private val users = Mongo4catsCollections.documents(database, MongoCollections.Users)

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
  ): RepositoryIO[List[ClaimedOperationalEvent]] =
    MongoRepositorySupport.repositoryGuard(diagnostics, "outbox.claim")(
      claimPage(workerId, transactionalId, now, leaseUntil, limit)
    )(_ => Left(RepositoryError.Unavailable))

  private def claimPage(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryIO[List[ClaimedOperationalEvent]] = {
    val cursors = Mongo4catsCollections.documents(database, MongoProducerRegistrations.ClaimCursorsCollection)
    def cursorFilter(row: Document): Either[RepositoryError, MongoFilter] = for {
      at <- Option(row.get("availableAt"))
        .collect { case date: Date => date }
        .toRight(RepositoryError.InvalidStoredData)
      occurred <- Option(row.get("occurredAt"))
        .collect { case date: Date => date }
        .toRight(RepositoryError.InvalidStoredData)
      id <- Option(row.get("eventId"))
        .collect { case value: String => value }
        .toRight(RepositoryError.InvalidStoredData)
    } yield MongoFilter.or(
      MongoFilter.gt(MongoFields.AvailableAt, at),
      MongoFilter.and(MongoFilter.eq(MongoFields.AvailableAt, at), MongoFilter.gt(MongoFields.OccurredAt, occurred)),
      MongoFilter.and(
        MongoFilter.eq(MongoFields.AvailableAt, at),
        MongoFilter.eq(MongoFields.OccurredAt, occurred),
        MongoFilter.gt(MongoFields.Id, id)
      )
    )
    def page(after: Option[MongoFilter]): RepositoryIO[List[Document]] = RepositoryIO
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
        val valid = Option(row.get("_id")).exists(_.isInstanceOf[String]) &&
          Option(row.get("availableAt")).exists(_.isInstanceOf[Date]) && Option(row.get("occurredAt"))
            .exists(_.isInstanceOf[Date])
        Either.cond(valid, row, RepositoryError.InvalidStoredData)
      })
    def checkpoint(row: Document): RepositoryIO[Unit] = RepositoryIO
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
      .subflatMap {
        case Some(result) if result.wasAcknowledged() => Right(())
        case _                                        => Left(RepositoryError.MissingWriteResult)
      }
    def scan(
        rows: List[Document],
        values: List[ClaimedOperationalEvent],
        last: Option[Document]
    ): RepositoryIO[List[ClaimedOperationalEvent]] =
      if (rows.isEmpty || values.size >= limit) last.traverse_(checkpoint).as(values)
      else
        rows match {
          case row :: tail =>
            claimOne(workerId, transactionalId, now, leaseUntil, row.getString("_id"))
              .leftFlatMap {
                case RepositoryError.Conflict => RepositoryIO.fromEither(Right(None))
                case error                    => RepositoryIO.fromEither(Left(error))
              }
              .flatMap(value => scan(tail, values ++ value.toList, Some(row)))
          case Nil => last.traverse_(checkpoint).as(values)
        }
    for {
      current <- RepositoryIO.lift(MongoSessionOperations.findOne(cursors, None, MongoFilter.eq("_id", workerId)))
      after <- RepositoryIO.fromEither(current.traverse(cursorFilter))
      found <- page(after)
      rows <- if (found.isEmpty && after.nonEmpty) page(None) else RepositoryIO.fromEither(Right(found))
      result <- scan(rows, Nil, None)
    } yield result
  }

  private def due(now: Instant): MongoFilter = MongoFilter.or(
    MongoFilter
      .and(MongoFilter.eq(MongoFields.State, "Retryable"), MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))),
    MongoFilter.and(
      MongoFilter.eq(MongoFields.State, "InFlight"),
      MongoFilter.lte(MongoFields.LeaseUntil, Date.from(now))
    )
  )

  override def markPublished(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): RepositoryIO[Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, "Published"),
        MongoUpdate.set(MongoFields.PublishedAt, Date.from(now)),
        MongoUpdate.set(MongoFields.RetentionExpiresAt, Date.from(retentionExpiresAt)),
        MongoUpdate.unset(MongoFields.LeaseOwner),
        MongoUpdate.unset(MongoFields.LeaseToken),
        MongoUpdate.unset(MongoFields.LeaseUntil),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
      )
    ).flatMap(_ => releaseSubjectLeases(leaseToken))

  override def releaseForRetry(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): RepositoryIO[Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, "Retryable"),
        MongoUpdate.set(MongoFields.AvailableAt, Date.from(availableAt)),
        MongoUpdate.unset(MongoFields.LeaseOwner),
        MongoUpdate.unset(MongoFields.LeaseToken),
        MongoUpdate.unset(MongoFields.LeaseUntil),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
      )
    ).flatMap(_ => releaseSubjectLeases(leaseToken))

  override def markFailed(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): RepositoryIO[Unit] =
    updateClaimed(
      eventId,
      leaseToken,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, "Failed"),
        MongoUpdate.set(MongoFields.LastError, reason.take(512)),
        MongoUpdate.unset(MongoFields.LeaseOwner),
        MongoUpdate.unset(MongoFields.LeaseToken),
        MongoUpdate.unset(MongoFields.LeaseUntil),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
      )
    ).flatMap(_ => releaseSubjectLeases(leaseToken))

  override def renewLease(
      eventId: UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseUntil: Instant
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.renewLease")(
        transactionRunner
          .run { session =>
            val filter = MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, eventId.toString),
              MongoFilter.eq(MongoFields.State, "InFlight"),
              MongoFilter.eq(MongoFields.LeaseToken, leaseToken)
            )
            val updateOutbox = RepositoryIO.lift(
              MongoSessionOperations.updateOne(
                outbox,
                session,
                filter,
                MongoUpdate.set(MongoFields.LeaseUntil, Date.from(leaseUntil))
              )
            )
            updateOutbox.flatMap {
              case Some(result) if result.getMatchedCount == 1L =>
                RepositoryIO
                  .lift(
                    updateMany(
                      subjectFences,
                      session,
                      MongoFilter.and(
                        MongoFilter.in(MongoFields.Id, subjectIds),
                        MongoFilter.eq(MongoFields.LeaseToken, leaseToken),
                        MongoFilter.ne(MongoFields.Deleted, true)
                      ),
                      MongoUpdate.set(MongoFields.LeaseUntil, Date.from(leaseUntil))
                    )
                  )
                  .subflatMap {
                    case Some(result) if result.getMatchedCount == subjectIds.size.toLong => Right(())
                    case Some(_) => Left(RepositoryError.Conflict)
                    case None    => Left(RepositoryError.MissingWriteResult)
                  }
              case Some(_) => RepositoryIO.fromEither(Left(RepositoryError.Conflict))
              case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
            }
          }
      )(_ => Left(RepositoryError.Unavailable))

  private def claimOne(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      eventId: String
  ): RepositoryIO[Option[ClaimedOperationalEvent]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.claimOne")(transactionRunner.run { session =>
        claimOneInSession(session, workerId, now, leaseUntil, eventId).flatMap {
          case None        => RepositoryIO.fromEither(Right(None))
          case Some(claim) =>
            subjectIsDeleted(session, claim).flatMap {
              case true  => suppressDeletedClaim(session, claim, now).as(None)
              case false => acquireSubjectLeases(session, claim, transactionalId, now, leaseUntil).as(Some(claim))
            }
        }
      })(_ => Left(RepositoryError.Unavailable))

  private def claimOneInSession(
      session: Option[ClientSession[IO]],
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      eventId: String
  ): RepositoryIO[Option[ClaimedOperationalEvent]] = {
    RepositoryIO.lift(uuidGen.randomUUID).flatMap { id =>
      val token = id.toString
      val filter = MongoFilter.and(due(now), MongoFilter.eq(MongoFields.Id, eventId))
      val update = MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, "InFlight"),
        MongoUpdate.set(MongoFields.LeaseOwner, workerId),
        MongoUpdate.set(MongoFields.LeaseToken, token),
        MongoUpdate.set(MongoFields.LeaseUntil, Date.from(leaseUntil)),
        MongoUpdate.inc(MongoFields.Attempts, 1),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
      )
      val options = new FindOneAndUpdateOptions()
        .sort(Sorts.ascending(MongoFields.AvailableAt, MongoFields.OccurredAt, MongoFields.Id))
        .returnDocument(ReturnDocument.AFTER)
      MongoRepositorySupport
        .transactionGuard(diagnostics, "outbox.claimOne", session)(
          RepositoryIO
            .lift(
              outbox
                .flatMap(collection =>
                  session.fold(
                    collection.findOneAndUpdate(filter.bson, update.bson, options)
                  )(active => collection.findOneAndUpdate(active, filter.sessionFilter, update.sessionUpdate, options))
                )
            )
            .subflatMap(_.traverse(readClaimed).leftMap(_ => RepositoryError.InvalidStoredData))
        )(_ => Left(RepositoryError.Unavailable))
    }
  }

  private def acquireSubjectLeases(
      session: Option[ClientSession[IO]],
      claim: ClaimedOperationalEvent,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Unit] = {
    val leaseAvailable = MongoFilter.or(
      MongoFilter.exists(MongoFields.LeaseUntil, false),
      MongoFilter.lte(MongoFields.LeaseUntil, Date.from(now))
    )
    val options = new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
    def acquireOne(subjectId: String): RepositoryIO[Unit] = {
      val filter = MongoFilter.and(
        MongoFilter.eq(MongoFields.Id, subjectId),
        MongoFilter.ne(MongoFields.Deleted, true),
        leaseAvailable
      )
      val update = MongoUpdate.combine(
        MongoUpdate.setOnInsert(MongoFields.Id, subjectId),
        MongoUpdate.set(MongoFields.LeaseEventId, claim.event.eventId.toString),
        MongoUpdate.set(MongoFields.LeaseToken, claim.leaseToken),
        MongoUpdate.set(MongoFields.LeaseUntil, Date.from(leaseUntil))
      )
      MongoRepositorySupport
        .transactionGuard(diagnostics, "outbox.acquireSubjectLease", session)(
          RepositoryIO
            .lift(
              subjectFences
                .flatMap(collection =>
                  session.fold(
                    collection.findOneAndUpdate(filter.bson, update.bson, options)
                  )(active => collection.findOneAndUpdate(active, filter.sessionFilter, update.sessionUpdate, options))
                )
            )
            .subflatMap {
              case Some(_) => Right(())
              case None    => Left(RepositoryError.Conflict)
            }
        ) {
          case error: MongoWriteException if error.getError.getCode == 11000 =>
            Left(RepositoryError.Conflict)
          case error: MongoCommandException if error.getErrorCode == 11000 =>
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
  ): RepositoryIO[Boolean] = {
    val filter = MongoFilter.and(
      MongoFilter.in(MongoFields.Id, claim.subjectIds),
      MongoFilter.eq(MongoFields.AccountStatus, "Deleted")
    )
    MongoRepositorySupport
      .transactionGuard(diagnostics, "outbox.subjectIsDeleted", session)(
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(users, session, filter)
          )
          .map(_.nonEmpty)
      )(_ => Left(RepositoryError.Unavailable))
  }

  private def suppressDeletedClaim(
      session: Option[ClientSession[IO]],
      claim: ClaimedOperationalEvent,
      now: Instant
  ): RepositoryIO[Unit] = {
    val filter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, claim.event.eventId.toString),
      MongoFilter.eq(MongoFields.State, "InFlight"),
      MongoFilter.eq(MongoFields.LeaseToken, claim.leaseToken)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.State, "Failed"),
      MongoUpdate.set(MongoFields.LastError, "SUBJECT_DELETED"),
      MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now)),
      MongoUpdate.unset(MongoFields.LeaseOwner),
      MongoUpdate.unset(MongoFields.LeaseToken),
      MongoUpdate.unset(MongoFields.LeaseUntil)
    )
    MongoRepositorySupport.transactionGuard(diagnostics, "outbox.suppressDeletedClaim", session)(
      RepositoryIO.lift(MongoSessionOperations.updateOne(outbox, session, filter, update)).subflatMap {
        case Some(result) if result.getMatchedCount == 1L => Right(())
        case _                                            => Left(RepositoryError.Conflict)
      }
    )(_ => Left(RepositoryError.Unavailable))
  }

  private def releaseSubjectLeases(leaseToken: String): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.releaseSubjectLeases")(
        RepositoryIO
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
          .subflatMap {
            case Some(result) if result.getMatchedCount > 0L => Right(())
            case Some(_)                                     => Left(RepositoryError.Conflict)
            case None                                        => Left(RepositoryError.MissingWriteResult)
          }
      )(_ => Left(RepositoryError.Unavailable))

  private def updateClaimed(
      eventId: UUID,
      leaseToken: String,
      update: MongoUpdate
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.updateClaimed")(
        RepositoryIO
          .lift(
            MongoSessionOperations
              .updateOne(
                outbox,
                None,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, eventId.toString),
                  MongoFilter.eq(MongoFields.State, "InFlight"),
                  MongoFilter.eq(MongoFields.LeaseToken, leaseToken)
                ),
                update
              )
          )
          .subflatMap {
            case Some(result) if result.getMatchedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      )(_ => Left(RepositoryError.Unavailable))

  private def readClaimed(
      document: Document
  ): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], ClaimedOperationalEvent] =
    (
      MongoHiringCodecs.readOperationalEvent(document),
      envelopeBytes(document).toValidatedNel,
      requiredString(document, MongoFields.PartitionKey).toValidatedNel,
      requiredString(document, MongoFields.LeaseToken).toValidatedNel,
      requiredInt(document, MongoFields.Attempts).toValidatedNel,
      requiredSubjectIds(document).toValidatedNel,
      requiredSubjectRefsVersion(document).toValidatedNel
    ).mapN((event, bytes, key, token, attempts, subjects, _) =>
      ClaimedOperationalEvent(event, bytes, key, token, attempts, subjects)
    ).toEither

  private def envelopeBytes(document: Document): Either[MongoHiringCodecs.StoredDocumentError, Array[Byte]] =
    Option(document.get(MongoFields.EnvelopeBytes)) match {
      case Some(value: Array[Byte]) => Right(value)
      case Some(value: Binary)      => Right(value.getData)
      case _ => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.EnvelopeBytes))
    }

  private def requiredString(document: Document, field: String): Either[MongoHiringCodecs.StoredDocumentError, String] =
    Option(document.get(field)) match {
      case Some(value: String) => Right(value)
      case None                => Left(MongoHiringCodecs.StoredDocumentError.MissingField(field))
      case _                   => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(field))
    }

  private def requiredInt(document: Document, field: String): Either[MongoHiringCodecs.StoredDocumentError, Int] =
    Option(document.get(field)) match {
      case Some(value: java.lang.Integer) => Right(value.intValue)
      case Some(value: java.lang.Long)    => Right(value.intValue)
      case None                           => Left(MongoHiringCodecs.StoredDocumentError.MissingField(field))
      case _                              => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(field))
    }

  private def requiredSubjectIds(
      document: Document
  ): Either[MongoHiringCodecs.StoredDocumentError, List[String]] =
    Option(document.get(MongoFields.SubjectIds)) match {
      case Some(values: java.util.List[?]) =>
        val subjects = values.asScala.toList.collect { case value: String => value }
        if (subjects.nonEmpty && subjects.size == values.size() && subjects.distinct.size == subjects.size)
          Right(subjects)
        else Left(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.SubjectIds))
      case None => Left(MongoHiringCodecs.StoredDocumentError.MissingField(MongoFields.SubjectIds))
      case _    => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.SubjectIds))
    }

  private def requiredSubjectRefsVersion(
      document: Document
  ): Either[MongoHiringCodecs.StoredDocumentError, Int] =
    Option(document.get(MongoFields.SubjectRefsVersion)) match {
      case Some(value: java.lang.Integer) if value.intValue() == 1 => Right(1)
      case Some(value: java.lang.Long) if value.longValue() == 1L  => Right(1)
      case None => Left(MongoHiringCodecs.StoredDocumentError.MissingField(MongoFields.SubjectRefsVersion))
      case _    => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.SubjectRefsVersion))
    }
}

object MongoOperationalEventOutboxRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics,
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): MongoOperationalEventOutboxRepository =
    new MongoOperationalEventOutboxRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics,
      uuidGen
    )
}

final class MongoConsumerReceiptRepository(database: MongoDatabase[IO], diagnostics: Diagnostics)
    extends ConsumerReceiptRepository {
  private val receipts = Mongo4catsCollections.documents(database, MongoCollections.ConsumerReceipts)

  override def exists(consumerGroup: String, eventId: UUID): RepositoryIO[Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.exists")(
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(receipts, None, MongoFilter.eq(MongoFields.Id, s"$consumerGroup:${eventId.toString}"))
          )
          .map(_.nonEmpty)
      )(_ => Left(RepositoryError.Unavailable))

  override def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): RepositoryIO[Boolean] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.record")(
        RepositoryIO
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
                  .append(MongoFields.CreatedAt, Date.from(now))
                  .append(MongoFields.ExpiresAt, Date.from(expiresAt))
              )
          )
          .subflatMap(
            _.fold[Either[RepositoryError, Boolean]](Left(RepositoryError.MissingWriteResult))(_ => Right(true))
          )
      ) {
        case write: MongoWriteException if write.getError.getCode == 11000 => Right(false)
        case _                                                             => Left(RepositoryError.Unavailable)
      }

}

final class MongoEventQuarantineRepository(database: MongoDatabase[IO], diagnostics: Diagnostics)
    extends EventQuarantineRepository {
  private val quarantine = Mongo4catsCollections.documents(database, MongoCollections.EventQuarantine)

  override def save(record: EventQuarantineRecord): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "eventQuarantine.save")(
        RepositoryIO
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
                  .append(MongoFields.OccurredAt, Date.from(record.occurredAt))
                  .append(MongoFields.ExpiresAt, Date.from(record.expiresAt))
              )
          )
          .subflatMap(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ => Right(())))
      ) {
        case write: MongoWriteException if write.getError.getCode == 11000 => Right(())
        case _                                                             => Left(RepositoryError.Unavailable)
      }

}

object MongoSearchSessionRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics
  ): MongoSearchSessionRepository =
    new MongoSearchSessionRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}

private[mongo] object MongoOutboxSubjectLeases {
  def acquire(subjectIds: List[String])(acquireOne: String => RepositoryIO[Unit]): RepositoryIO[Unit] =
    if (subjectIds.isEmpty) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else subjectIds.sorted.traverse_(acquireOne)
}
