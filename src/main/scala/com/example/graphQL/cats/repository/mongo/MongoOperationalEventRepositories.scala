package com.example.graphQL.cats.repository.mongo

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.repository.protocol.{
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  MutationWriteContext,
  OperationalEventOutboxRepository,
  SearchSessionRepository
}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.{MongoCommandException, MongoWriteException}
import com.mongodb.client.model.{Filters, FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.{MongoClient, MongoDatabase}
import org.bson.Document
import org.bson.types.Binary
import java.time.Instant
import java.util.Date
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoSearchSessionRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    diagnostics: Diagnostics = Diagnostics.noop
) extends SearchSessionRepository
    with MongoOperationalEventInsertion
    with MongoConflictWriteMapping {
  private val sessions = database.getCollection("search_sessions")
  private val outbox = database.getCollection("event_outbox")

  override def save(session: SearchSession, event: OperationalEventEnvelope): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.save")(
        transactionRunner
          .run { active =>
            val document = MongoHiringCodecs.searchSession(session)
            val filter = Filters.and(
              Filters.eq("_id", session.id.toString),
              Filters.eq("actorId", session.actorId.value.toString)
            )
            val update = new Document("$setOnInsert", document)
            val options = new UpdateOptions().upsert(true)
            val result = active.fold(
              PublisherBridge.first(sessions.updateOne(filter, update, options))
            )(clientSession => PublisherBridge.first(sessions.updateOne(clientSession, filter, update, options)))
            result.flatMap {
              case Some(value) if Option(value.getUpsertedId).nonEmpty =>
                insertOperationalEvents(outbox, active, List(event), session.occurredAt, diagnostics)
              case Some(_) => IO.pure(Right(()))
              case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
            }
          }
      )(mapWrite)
      .value

  override def find(id: UUID): IO[Either[RepositoryError, Option[SearchSession]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.find")(
        PublisherBridge
          .first(sessions.find(Filters.eq("_id", id.toString)))
          .map(document =>
            MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readSearchSession))
          )
      )(_ => Left(RepositoryError.Unavailable))
      .value

  def recordInteraction(event: OperationalEventEnvelope): IO[Either[RepositoryError, Boolean]] =
    recordInteractionWithSession(event, None)

  override def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext
  ): IO[Either[RepositoryError, Boolean]] =
    MongoMutationWriteContext.session(context).flatMap(recordInteractionWithSession(event, _))

  private def recordInteractionWithSession(
      event: OperationalEventEnvelope,
      session: Option[com.mongodb.reactivestreams.client.ClientSession]
  ): IO[Either[RepositoryError, Boolean]] =
    val insert = IO
      .fromEither(
        MongoHiringCodecs
          .outboxRecord(event, event.occurredAt)
          .leftMap(message => new IllegalArgumentException(message))
      )
      .flatMap(document =>
        session.fold(
          PublisherBridge.first(outbox.insertOne(document))
        )(active => PublisherBridge.first(outbox.insertOne(active, document)))
      )
      .map(_.fold[Either[RepositoryError, Boolean]](Left(RepositoryError.MissingWriteResult))(_ => Right(true)))
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSession.recordInteraction")(insert)(mapWrite)
      .value
      .flatMap {
        case Left(RepositoryError.Conflict) =>
          MongoRepositorySupport
            .repositoryGuard(diagnostics, "searchSession.recordInteraction.findDuplicate")(
              session
                .fold(
                  PublisherBridge.first(outbox.find(Filters.eq("_id", event.eventId.toString)))
                )(active => PublisherBridge.first(outbox.find(active, Filters.eq("_id", event.eventId.toString))))
                .map {
                  case Some(existing) if sameEvent(existing, event) => Right(false)
                  case _                                            => Left(RepositoryError.Conflict)
                }
            )(_ => Left(RepositoryError.Unavailable))
            .value
        case result => IO.pure(result)
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
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    diagnostics: Diagnostics = Diagnostics.noop
) extends OperationalEventOutboxRepository {
  private val outbox = database.getCollection("event_outbox")
  private val subjectFences = database.getCollection("outbox_subject_fences")
  private val users = database.getCollection("users")

  override def claim(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): IO[Either[RepositoryError, List[ClaimedOperationalEvent]]] = {
    def claimUntilBlocked(
        remaining: Int,
        values: List[ClaimedOperationalEvent]
    ): IO[Either[RepositoryError, List[ClaimedOperationalEvent]]] =
      if (remaining <= 0) IO.pure(Right(values))
      else
        claimOne(workerId, transactionalId, now, leaseUntil).flatMap {
          case Left(RepositoryError.Conflict) => IO.pure(Right(values))
          case Left(error)                    => IO.pure(Left(error))
          case Right(None)                    => IO.pure(Right(values))
          case Right(Some(value))             => claimUntilBlocked(remaining - 1, values :+ value)
        }

    if (limit <= 0) IO.pure(Right(Nil)) else claimUntilBlocked(limit, Nil)
  }

  override def markPublished(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): IO[Either[RepositoryError, Unit]] =
    updateClaimed(
      eventId,
      leaseToken,
      Updates.combine(
        Updates.set("state", "Published"),
        Updates.set("publishedAt", Date.from(now)),
        Updates.set("retentionExpiresAt", Date.from(retentionExpiresAt)),
        Updates.unset("leaseOwner"),
        Updates.unset("leaseToken"),
        Updates.unset("leaseUntil"),
        Updates.set("updatedAt", Date.from(now))
      )
    ).flatMap {
      case Right(())   => releaseSubjectLeases(leaseToken)
      case Left(error) => IO.pure(Left(error))
    }

  override def releaseForRetry(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): IO[Either[RepositoryError, Unit]] =
    updateClaimed(
      eventId,
      leaseToken,
      Updates.combine(
        Updates.set("state", "Retryable"),
        Updates.set("availableAt", Date.from(availableAt)),
        Updates.unset("leaseOwner"),
        Updates.unset("leaseToken"),
        Updates.unset("leaseUntil"),
        Updates.set("updatedAt", Date.from(now))
      )
    ).flatMap {
      case Right(())   => releaseSubjectLeases(leaseToken)
      case Left(error) => IO.pure(Left(error))
    }

  override def markFailed(
      eventId: UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): IO[Either[RepositoryError, Unit]] =
    updateClaimed(
      eventId,
      leaseToken,
      Updates.combine(
        Updates.set("state", "Failed"),
        Updates.set("lastError", reason.take(512)),
        Updates.unset("leaseOwner"),
        Updates.unset("leaseToken"),
        Updates.unset("leaseUntil"),
        Updates.set("updatedAt", Date.from(now))
      )
    ).flatMap {
      case Right(())   => releaseSubjectLeases(leaseToken)
      case Left(error) => IO.pure(Left(error))
    }

  override def renewLease(
      eventId: UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseUntil: Instant
  ): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.renewLease")(
        transactionRunner
          .run { session =>
            val filter = Filters.and(
              Filters.eq("_id", eventId.toString),
              Filters.eq("state", "InFlight"),
              Filters.eq("leaseToken", leaseToken)
            )
            val updateOutbox = PublisherBridge
              .first(
                session.fold(
                  outbox.updateOne(
                    filter,
                    Updates.set("leaseUntil", Date.from(leaseUntil))
                  )
                )(active =>
                  outbox.updateOne(
                    active,
                    filter,
                    Updates.set("leaseUntil", Date.from(leaseUntil))
                  )
                )
              )
            updateOutbox.flatMap {
              case Some(result) if result.getMatchedCount == 1L =>
                PublisherBridge
                  .first(
                    session.fold(
                      subjectFences.updateMany(
                        Filters.and(
                          Filters.in("_id", subjectIds.asJava),
                          Filters.eq("leaseToken", leaseToken),
                          Filters.ne("deleted", true)
                        ),
                        Updates.set("leaseUntil", Date.from(leaseUntil))
                      )
                    )(active =>
                      subjectFences.updateMany(
                        active,
                        Filters.and(
                          Filters.in("_id", subjectIds.asJava),
                          Filters.eq("leaseToken", leaseToken),
                          Filters.ne("deleted", true)
                        ),
                        Updates.set("leaseUntil", Date.from(leaseUntil))
                      )
                    )
                  )
                  .map {
                    case Some(result) if result.getMatchedCount == subjectIds.size.toLong => Right(())
                    case Some(_) => Left(RepositoryError.Conflict)
                    case None    => Left(RepositoryError.MissingWriteResult)
                  }
              case Some(_) => IO.pure(Left(RepositoryError.Conflict))
              case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
            }
          }
      )(_ => Left(RepositoryError.Unavailable))
      .value

  private def claimOne(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant
  ): IO[Either[RepositoryError, Option[ClaimedOperationalEvent]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.claimOne")(transactionRunner.run { session =>
        claimOneInSession(session, workerId, now, leaseUntil).flatMap {
          case Left(error)        => IO.pure(Left(error))
          case Right(None)        => IO.pure(Right(None))
          case Right(Some(claim)) =>
            subjectIsDeleted(session, claim).flatMap {
              case Left(error)  => IO.pure(Left(error))
              case Right(true)  => suppressDeletedClaim(session, claim, now).as(Right(None))
              case Right(false) =>
                acquireSubjectLeases(session, claim, transactionalId, now, leaseUntil).map(_.as(Some(claim)))
            }
        }
      })(_ => Left(RepositoryError.Unavailable))
      .value

  private def claimOneInSession(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): IO[Either[RepositoryError, Option[ClaimedOperationalEvent]]] = {
    val token = UUID.randomUUID().toString
    val filter = Filters.or(
      Filters.and(Filters.eq("state", "Retryable"), Filters.lte("availableAt", Date.from(now))),
      Filters.and(Filters.eq("state", "InFlight"), Filters.lte("leaseUntil", Date.from(now)))
    )
    val update = Updates.combine(
      Updates.set("state", "InFlight"),
      Updates.set("leaseOwner", workerId),
      Updates.set("leaseToken", token),
      Updates.set("leaseUntil", Date.from(leaseUntil)),
      Updates.inc("attempts", 1),
      Updates.set("updatedAt", Date.from(now))
    )
    val options = new FindOneAndUpdateOptions()
      .sort(Sorts.ascending("availableAt", "occurredAt", "_id"))
      .returnDocument(ReturnDocument.AFTER)
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.claimOne")(
        PublisherBridge
          .first(
            session.fold(outbox.findOneAndUpdate(filter, update, options))(active =>
              outbox.findOneAndUpdate(active, filter, update, options)
            )
          )
          .map(_.traverse(readClaimed).leftMap(_ => RepositoryError.InvalidStoredData))
      )(_ => Left(RepositoryError.Unavailable))
      .value
  }

  private def acquireSubjectLeases(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      claim: ClaimedOperationalEvent,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant
  ): IO[Either[RepositoryError, Unit]] = {
    val leaseAvailable = Filters.or(
      Filters.exists("leaseUntil", false),
      Filters.lte("leaseUntil", Date.from(now))
    )
    val options = new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
    def acquireOne(subjectId: String): IO[Either[RepositoryError, Unit]] = {
      val filter = Filters.and(
        Filters.eq("_id", subjectId),
        Filters.ne("deleted", true),
        leaseAvailable
      )
      val update = Updates.combine(
        Updates.setOnInsert("_id", subjectId),
        Updates.set("leaseEventId", claim.event.eventId.toString),
        Updates.set("leaseToken", claim.leaseToken),
        Updates.set("leaseUntil", Date.from(leaseUntil)),
        Updates.addToSet("transactionalIds", transactionalId)
      )
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "outbox.acquireSubjectLease")(
          PublisherBridge
            .first(
              session.fold(subjectFences.findOneAndUpdate(filter, update, options))(active =>
                subjectFences.findOneAndUpdate(active, filter, update, options)
              )
            )
            .map {
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
        .value
    }
    val acquired = claim.subjectIds.sorted.foldLeft(
      IO.pure(Right(()): Either[RepositoryError, Unit])
    ) { (current, subjectId) =>
      current.flatMap {
        case Left(error) => IO.pure(Left(error))
        case Right(_)    => acquireOne(subjectId)
      }
    }
    acquired.flatMap {
      case Left(error)                          => IO.pure(Left(error))
      case Right(_) if claim.subjectIds.isEmpty => IO.pure(Left(RepositoryError.InvalidStoredData))
      case Right(_)                             => IO.pure(Right(()))
    }
  }

  private def subjectIsDeleted(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      claim: ClaimedOperationalEvent
  ): IO[Either[RepositoryError, Boolean]] = {
    val filter = Filters.and(
      Filters.in("_id", claim.subjectIds.asJava),
      Filters.eq("accountStatus", "Deleted")
    )
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.subjectIsDeleted")(
        PublisherBridge
          .first(session.fold(users.find(filter))(active => users.find(active, filter)))
          .map(document => Right(document.nonEmpty))
      )(_ => Left(RepositoryError.Unavailable))
      .value
  }

  private def suppressDeletedClaim(
      session: Option[com.mongodb.reactivestreams.client.ClientSession],
      claim: ClaimedOperationalEvent,
      now: Instant
  ): IO[Unit] = {
    val filter = Filters.and(
      Filters.eq("_id", claim.event.eventId.toString),
      Filters.eq("state", "InFlight"),
      Filters.eq("leaseToken", claim.leaseToken)
    )
    val update = Updates.combine(
      Updates.set("state", "Failed"),
      Updates.set("lastError", "SUBJECT_DELETED"),
      Updates.set("updatedAt", Date.from(now)),
      Updates.unset("leaseOwner"),
      Updates.unset("leaseToken"),
      Updates.unset("leaseUntil")
    )
    MongoRepositorySupport.guard(diagnostics, "outbox.suppressDeletedClaim")(
      PublisherBridge
        .first(session.fold(outbox.updateOne(filter, update))(active => outbox.updateOne(active, filter, update)))
        .flatMap {
          case Some(result) if result.getMatchedCount == 1L => IO.unit
          case _ => IO.raiseError(new IllegalStateException("could not suppress deleted-subject outbox event"))
        }
    )(_ => ())
  }

  private def releaseSubjectLeases(leaseToken: String): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.releaseSubjectLeases")(
        PublisherBridge
          .first(
            subjectFences.updateMany(
              Filters.eq("leaseToken", leaseToken),
              Updates.combine(
                Updates.unset("leaseEventId"),
                Updates.unset("leaseToken"),
                Updates.unset("leaseUntil")
              )
            )
          )
          .map {
            case Some(result) if result.getMatchedCount > 0L => Right(())
            case Some(_)                                     => Left(RepositoryError.Conflict)
            case None                                        => Left(RepositoryError.MissingWriteResult)
          }
      )(_ => Left(RepositoryError.Unavailable))
      .value

  private def updateClaimed(
      eventId: UUID,
      leaseToken: String,
      update: org.bson.conversions.Bson
  ): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "outbox.updateClaimed")(
        PublisherBridge
          .first(
            outbox.updateOne(
              Filters.and(
                Filters.eq("_id", eventId.toString),
                Filters.eq("state", "InFlight"),
                Filters.eq("leaseToken", leaseToken)
              ),
              update
            )
          )
          .map {
            case Some(result) if result.getMatchedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      )(_ => Left(RepositoryError.Unavailable))
      .value

  private def readClaimed(
      document: Document
  ): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], ClaimedOperationalEvent] =
    (
      MongoHiringCodecs.readOperationalEvent(document),
      envelopeBytes(document).toValidatedNel,
      requiredString(document, "partitionKey").toValidatedNel,
      requiredString(document, "leaseToken").toValidatedNel,
      requiredInt(document, "attempts").toValidatedNel,
      requiredSubjectIds(document).toValidatedNel,
      requiredSubjectRefsVersion(document).toValidatedNel
    ).mapN((event, bytes, key, token, attempts, subjects, _) =>
      ClaimedOperationalEvent(event, bytes, key, token, attempts, subjects)
    ).toEither

  private def envelopeBytes(document: Document): Either[MongoHiringCodecs.StoredDocumentError, Array[Byte]] =
    Option(document.get("envelopeBytes")) match {
      case Some(value: Array[Byte]) => Right(value)
      case Some(value: Binary)      => Right(value.getData)
      case _                        => Left(MongoHiringCodecs.StoredDocumentError.InvalidField("envelopeBytes"))
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
    Option(document.get("subjectIds")) match {
      case Some(values: java.util.List[?]) =>
        val subjects = values.asScala.toList.collect { case value: String => value }
        if (subjects.nonEmpty && subjects.size == values.size() && subjects.distinct.size == subjects.size)
          Right(subjects)
        else Left(MongoHiringCodecs.StoredDocumentError.InvalidField("subjectIds"))
      case None => Left(MongoHiringCodecs.StoredDocumentError.MissingField("subjectIds"))
      case _    => Left(MongoHiringCodecs.StoredDocumentError.InvalidField("subjectIds"))
    }

  private def requiredSubjectRefsVersion(
      document: Document
  ): Either[MongoHiringCodecs.StoredDocumentError, Int] =
    Option(document.get("subjectRefsVersion")) match {
      case Some(value: java.lang.Integer) if value.intValue() == 1 => Right(1)
      case Some(value: java.lang.Long) if value.longValue() == 1L  => Right(1)
      case None => Left(MongoHiringCodecs.StoredDocumentError.MissingField("subjectRefsVersion"))
      case _    => Left(MongoHiringCodecs.StoredDocumentError.InvalidField("subjectRefsVersion"))
    }
}

object MongoOperationalEventOutboxRepository {
  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      diagnostics: Diagnostics = Diagnostics.noop
  ): MongoOperationalEventOutboxRepository =
    new MongoOperationalEventOutboxRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}

final class MongoConsumerReceiptRepository(database: MongoDatabase, diagnostics: Diagnostics = Diagnostics.noop)
    extends ConsumerReceiptRepository {
  private val receipts = database.getCollection("consumer_receipts")

  override def exists(consumerGroup: String, eventId: UUID): IO[Either[RepositoryError, Boolean]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.exists")(
        PublisherBridge
          .first(receipts.find(Filters.eq("_id", s"$consumerGroup:${eventId.toString}")))
          .map(value => Right(value.nonEmpty))
      )(_ => Left(RepositoryError.Unavailable))
      .value

  override def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): IO[Either[RepositoryError, Boolean]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "consumerReceipt.record")(
        PublisherBridge
          .first(
            receipts.insertOne(
              new Document("_id", s"$consumerGroup:${event.eventId.toString}")
                .append("consumerGroup", consumerGroup)
                .append("eventId", event.eventId.toString)
                .append("aggregateType", event.aggregateType.toString)
                .append("aggregateId", event.aggregateId)
                .append("createdAt", Date.from(now))
                .append("expiresAt", Date.from(expiresAt))
            )
          )
          .map(_.fold[Either[RepositoryError, Boolean]](Left(RepositoryError.MissingWriteResult))(_ => Right(true)))
      ) {
        case write: MongoWriteException if write.getError.getCode == 11000 => Right(false)
        case _                                                             => Left(RepositoryError.Unavailable)
      }
      .value
}

final class MongoEventQuarantineRepository(database: MongoDatabase, diagnostics: Diagnostics = Diagnostics.noop)
    extends EventQuarantineRepository {
  private val quarantine = database.getCollection("event_quarantine")

  override def save(record: EventQuarantineRecord): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "eventQuarantine.save")(
        PublisherBridge
          .first(
            quarantine.insertOne(
              new Document()
                .append("topic", record.topic)
                .append("partition", java.lang.Integer.valueOf(record.partition))
                .append("offset", java.lang.Long.valueOf(record.offset))
                .append("category", record.category.toString)
                .append("reason", record.reason.take(512))
                .append("rawBytes", record.rawBytes)
                .append("occurredAt", Date.from(record.occurredAt))
                .append("expiresAt", Date.from(record.expiresAt))
            )
          )
          .map(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ => Right(())))
      ) {
        case write: MongoWriteException if write.getError.getCode == 11000 => Right(())
        case _                                                             => Left(RepositoryError.Unavailable)
      }
      .value
}

object MongoSearchSessionRepository {
  def standalone(database: MongoDatabase): MongoSearchSessionRepository =
    new MongoSearchSessionRepository(database)

  def transactional(
      database: MongoDatabase,
      client: MongoClient,
      diagnostics: Diagnostics = Diagnostics.noop
  ): MongoSearchSessionRepository =
    new MongoSearchSessionRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}
