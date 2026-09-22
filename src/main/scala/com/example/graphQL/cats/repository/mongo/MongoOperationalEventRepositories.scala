package com.example.graphQL.cats.repository.mongo

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.repository.protocol.{
  ClaimedOperationalEvent, ConsumerReceiptRepository, EventQuarantineRecord, EventQuarantineRepository,
  MutationWriteContext, OperationalEventOutboxRepository, SearchSessionRepository
}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.{MongoClient, MongoDatabase}
import org.bson.Document
import org.bson.types.Binary
import java.time.Instant
import java.util.Date
import java.util.UUID

final class MongoSearchSessionRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction
) extends SearchSessionRepository with MongoOperationalEventInsertion with MongoConflictWriteMapping {
  private val sessions = database.getCollection("search_sessions")
  private val outbox = database.getCollection("event_outbox")

  override def save(session: SearchSession, event: OperationalEventEnvelope): IO[Either[RepositoryError, Unit]] =
    transactionRunner.run { active =>
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
          insertOperationalEvents(outbox, active, List(event), session.occurredAt)
        case Some(_) => IO.pure(Right(()))
        case None => IO.pure(Left(RepositoryError.Unavailable))
      }
    }.handleError(mapWrite)

  override def find(id: UUID): IO[Either[RepositoryError, Option[SearchSession]]] =
    PublisherBridge.first(sessions.find(Filters.eq("_id", id.toString)))
      .map(document => MongoStoredDocumentDecoding.repository(document.traverse(MongoHiringCodecs.readSearchSession)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  def recordInteraction(event: OperationalEventEnvelope): IO[Either[RepositoryError, Boolean]] =
    recordInteractionWithSession(event, None)

  override def recordInteraction(event: OperationalEventEnvelope, context: MutationWriteContext): IO[Either[RepositoryError, Boolean]] =
    recordInteractionWithSession(event, MongoMutationWriteContext.session(context))

  private def recordInteractionWithSession(
      event: OperationalEventEnvelope,
      session: Option[com.mongodb.reactivestreams.client.ClientSession]
  ): IO[Either[RepositoryError, Boolean]] =
    session.fold(
      PublisherBridge.first(outbox.insertOne(MongoHiringCodecs.outboxRecord(event, event.occurredAt)))
    )(active => PublisherBridge.first(outbox.insertOne(active, MongoHiringCodecs.outboxRecord(event, event.occurredAt)))).as(Right(true)).handleErrorWith {
      case write: MongoWriteException if write.getError.getCode == 11000 =>
        session.fold(
          PublisherBridge.first(outbox.find(Filters.eq("_id", event.eventId.toString)))
        )(active => PublisherBridge.first(outbox.find(active, Filters.eq("_id", event.eventId.toString)))).map {
          case Some(existing) if sameEvent(existing, event) => Right(false)
          case Some(_) => Left(RepositoryError.Conflict)
          case None => Left(RepositoryError.Conflict)
        }.handleError(_ => Left(RepositoryError.Unavailable))
      case _ => IO.pure(Left(RepositoryError.Unavailable))
    }

  private def sameEvent(document: Document, event: OperationalEventEnvelope): Boolean =
    MongoHiringCodecs.readOperationalEvent(document).toOption.exists(existing =>
      existing.eventId == event.eventId &&
        existing.eventType == event.eventType &&
        existing.aggregateType == event.aggregateType &&
        existing.aggregateId == event.aggregateId &&
        existing.actorId == event.actorId &&
        existing.payload == event.payload
    )
}

final class MongoOperationalEventOutboxRepository(database: MongoDatabase)
    extends OperationalEventOutboxRepository {
  private val outbox = database.getCollection("event_outbox")

  override def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): IO[Either[RepositoryError, List[ClaimedOperationalEvent]]] =
    List.fill(limit)(()).foldLeft(IO.pure(Right(List.empty[ClaimedOperationalEvent]): Either[RepositoryError, List[ClaimedOperationalEvent]])) {
      case (acc, _) =>
        acc.flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right(values) =>
            claimOne(workerId, now, leaseUntil).map {
              case Left(error) => Left(error)
              case Right(None) => Right(values)
              case Right(Some(value)) => Right(values :+ value)
            }
        }
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
    )

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
    )

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
    )

  private def claimOne(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): IO[Either[RepositoryError, Option[ClaimedOperationalEvent]]] = {
    val token = UUID.randomUUID().toString
    PublisherBridge.first(outbox.findOneAndUpdate(
      Filters.or(
        Filters.and(Filters.eq("state", "Retryable"), Filters.lte("availableAt", Date.from(now))),
        Filters.and(Filters.eq("state", "InFlight"), Filters.lte("leaseUntil", Date.from(now)))
      ),
      Updates.combine(
        Updates.set("state", "InFlight"),
        Updates.set("leaseOwner", workerId),
        Updates.set("leaseToken", token),
        Updates.set("leaseUntil", Date.from(leaseUntil)),
        Updates.inc("attempts", 1),
        Updates.set("updatedAt", Date.from(now))
      ),
      new FindOneAndUpdateOptions()
        .sort(Sorts.ascending("availableAt", "occurredAt", "_id"))
        .returnDocument(ReturnDocument.AFTER)
    )).map(_.traverse(readClaimed).leftMap(_ => RepositoryError.Unavailable))
      .handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def updateClaimed(eventId: UUID, leaseToken: String, update: org.bson.conversions.Bson): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(outbox.updateOne(
      Filters.and(Filters.eq("_id", eventId.toString), Filters.eq("state", "InFlight"), Filters.eq("leaseToken", leaseToken)),
      update
    )).map {
      case Some(result) if result.getMatchedCount == 1L => Right(())
      case Some(_) => Left(RepositoryError.Conflict)
      case None => Left(RepositoryError.Unavailable)
    }.handleError(_ => Left(RepositoryError.Unavailable))

  private def readClaimed(document: Document): Either[NonEmptyList[MongoHiringCodecs.StoredDocumentError], ClaimedOperationalEvent] =
    (
      MongoHiringCodecs.readOperationalEvent(document),
      envelopeBytes(document).toValidatedNel,
      requiredString(document, "partitionKey").toValidatedNel,
      requiredString(document, "leaseToken").toValidatedNel,
      requiredInt(document, "attempts").toValidatedNel
    ).mapN(ClaimedOperationalEvent.apply).toEither

  private def envelopeBytes(document: Document): Either[MongoHiringCodecs.StoredDocumentError, Array[Byte]] =
    Option(document.get("envelopeBytes")) match {
      case Some(value: Array[Byte]) => Right(value)
      case Some(value: Binary) => Right(value.getData)
      case _ => Left(MongoHiringCodecs.StoredDocumentError.InvalidField("envelopeBytes"))
    }

  private def requiredString(document: Document, field: String): Either[MongoHiringCodecs.StoredDocumentError, String] =
    Option(document.get(field)) match {
      case Some(value: String) => Right(value)
      case None => Left(MongoHiringCodecs.StoredDocumentError.MissingField(field))
      case _ => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(field))
    }

  private def requiredInt(document: Document, field: String): Either[MongoHiringCodecs.StoredDocumentError, Int] =
    Option(document.get(field)) match {
      case Some(value: java.lang.Integer) => Right(value.intValue)
      case Some(value: java.lang.Long) => Right(value.intValue)
      case None => Left(MongoHiringCodecs.StoredDocumentError.MissingField(field))
      case _ => Left(MongoHiringCodecs.StoredDocumentError.InvalidField(field))
    }
}

final class MongoConsumerReceiptRepository(database: MongoDatabase) extends ConsumerReceiptRepository {
  private val receipts = database.getCollection("consumer_receipts")

  override def exists(consumerGroup: String, eventId: UUID): IO[Either[RepositoryError, Boolean]] =
    PublisherBridge.first(receipts.find(Filters.eq("_id", s"$consumerGroup:${eventId.toString}")))
      .map(value => Right(value.nonEmpty))
      .handleError(_ => Left(RepositoryError.Unavailable))

  override def record(consumerGroup: String, event: OperationalEventEnvelope, now: Instant, expiresAt: Instant): IO[Either[RepositoryError, Boolean]] =
    PublisherBridge.first(receipts.insertOne(new Document("_id", s"$consumerGroup:${event.eventId.toString}")
      .append("consumerGroup", consumerGroup)
      .append("eventId", event.eventId.toString)
      .append("aggregateType", event.aggregateType.toString)
      .append("aggregateId", event.aggregateId)
      .append("createdAt", Date.from(now))
      .append("expiresAt", Date.from(expiresAt)))).as(Right(true)).handleError {
      case write: MongoWriteException if write.getError.getCode == 11000 => Right(false)
      case _ => Left(RepositoryError.Unavailable)
    }
}

final class MongoEventQuarantineRepository(database: MongoDatabase) extends EventQuarantineRepository {
  private val quarantine = database.getCollection("event_quarantine")

  override def save(record: EventQuarantineRecord): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(quarantine.insertOne(new Document()
      .append("topic", record.topic)
      .append("partition", java.lang.Integer.valueOf(record.partition))
      .append("offset", java.lang.Long.valueOf(record.offset))
      .append("category", record.category.toString)
      .append("reason", record.reason.take(512))
      .append("rawBytes", record.rawBytes)
      .append("occurredAt", Date.from(record.occurredAt))
      .append("expiresAt", Date.from(record.expiresAt)))).as(Right(())).handleError {
      case write: MongoWriteException if write.getError.getCode == 11000 => Right(())
      case _ => Left(RepositoryError.Unavailable)
    }
}

object MongoSearchSessionRepository {
  def standalone(database: MongoDatabase): MongoSearchSessionRepository =
    new MongoSearchSessionRepository(database)

  def transactional(database: MongoDatabase, client: MongoClient): MongoSearchSessionRepository =
    new MongoSearchSessionRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict))
}
