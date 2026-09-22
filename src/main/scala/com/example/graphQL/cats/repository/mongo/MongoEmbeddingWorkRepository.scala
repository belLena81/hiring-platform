package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.mongodb.client.model.{Filters, FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.{ClientSession, MongoDatabase}
import org.bson.Document
import java.time.Instant
import java.util.Date

/** Durable, coalesced embedding work. A newer enqueue increments generation so an older lease cannot delete it. */
final class MongoEmbeddingWorkRepository(database: MongoDatabase) extends EmbeddingWorkRepository {
  private enum StoredWorkError {
    case InvalidDocument
  }

  private val collection = database.getCollection("embedding_work")

  override def enqueue(key: EmbeddingWorkKey, now: Instant): IO[Either[RepositoryError, Unit]] =
    enqueue(None, key, now)

  /**
    * Persists work in the caller's Mongo transaction. This is deliberately a concrete Mongo capability:
    * the generic work port has no transaction/session concept.
    */
  def enqueue(session: ClientSession, key: EmbeddingWorkKey, now: Instant): IO[Either[RepositoryError, Unit]] =
    enqueue(Some(session), key, now)

  private[mongo] def enqueue(
      session: Option[ClientSession],
      key: EmbeddingWorkKey,
      now: Instant
  ): IO[Either[RepositoryError, Unit]] = {
    val readyUpdate = Updates.combine(
      Updates.setOnInsert("kind", key.kind.toString),
      Updates.setOnInsert("entityId", key.entityId),
      Updates.setOnInsert("createdAt", Date.from(now)),
      Updates.inc("generation", java.lang.Long.valueOf(1L)),
      Updates.set("attempts", java.lang.Integer.valueOf(0)),
      Updates.set("state", "Ready"),
      Updates.set("availableAt", Date.from(now)),
      Updates.set("updatedAt", Date.from(now)),
      Updates.unset("leaseOwner"),
      Updates.unset("leaseToken"),
      Updates.unset("leaseUntil"),
      Updates.unset("failure")
    )
    val refreshActiveLease = Updates.combine(
      Updates.inc("generation", java.lang.Long.valueOf(1L)),
      Updates.set("attempts", java.lang.Integer.valueOf(0)),
      Updates.set("updatedAt", Date.from(now))
    )
    updateOne(session,
      Filters.and(Filters.eq("_id", key.value), Filters.ne("state", "Processing")),
      readyUpdate,
      new UpdateOptions().upsert(true)
    ).flatMap {
      case Some(result) if result.getMatchedCount == 1L || result.getUpsertedId != null => IO.pure(Right(()))
      case _ =>
        updateOne(session,
          Filters.and(Filters.eq("_id", key.value), Filters.eq("state", "Processing")),
          refreshActiveLease
        ).map {
          case Some(result) if result.getMatchedCount == 1L => Right(())
          case _ => Left(RepositoryError.Conflict)
        }
    }.handleError(_ => Left(RepositoryError.Unavailable))
  }

  private def updateOne(
      session: Option[ClientSession],
      filter: org.bson.conversions.Bson,
      update: org.bson.conversions.Bson,
      options: UpdateOptions = new UpdateOptions()
  ) =
    session.fold(PublisherBridge.first(collection.updateOne(filter, update, options))) { active =>
      PublisherBridge.first(collection.updateOne(active, filter, update, options))
    }

  override def claim(workerId: String, now: Instant, leaseUntil: Instant): IO[Either[RepositoryError, Option[ClaimedEmbeddingWork]]] = {
    IO.randomUUID.map(_.toString).flatMap { token =>
      val available = Filters.and(Filters.in("state", "Ready", "Retry"), Filters.lte("availableAt", Date.from(now)))
      val expiredLease = Filters.and(Filters.eq("state", "Processing"), Filters.lt("leaseUntil", Date.from(now)))
      val update = Updates.combine(
        Updates.set("state", "Processing"),
        Updates.set("leaseOwner", workerId),
        Updates.set("leaseToken", token),
        Updates.set("leaseUntil", Date.from(leaseUntil)),
        Updates.set("updatedAt", Date.from(now))
      )
      val options = new FindOneAndUpdateOptions()
        .returnDocument(ReturnDocument.AFTER)
        .sort(Sorts.ascending("availableAt", "_id"))
      PublisherBridge.first(collection.findOneAndUpdate(Filters.or(available, expiredLease), update, options))
        .map {
          case Some(document) => readClaim(document) match {
            case Right(claim) => Right(Some(claim))
            case Left(_) => Left(RepositoryError.Unavailable)
          }
          case None => Right(None)
        }
        .handleError(_ => Left(RepositoryError.Unavailable))
    }
  }

  override def complete(claim: ClaimedEmbeddingWork): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(collection.deleteOne(leaseFilter(claim))).map {
      case Some(result) if result.getDeletedCount == 1L => Right(())
      case _ => Left(RepositoryError.Conflict)
    }.handleError(_ => Left(RepositoryError.Unavailable))

  override def retry(claim: ClaimedEmbeddingWork, availableAt: Instant): IO[Either[RepositoryError, Unit]] =
    transition(claim, Updates.combine(
      Updates.set("state", "Retry"),
      Updates.set("availableAt", Date.from(availableAt)),
      Updates.inc("attempts", java.lang.Integer.valueOf(1)),
      Updates.unset("leaseOwner"),
      Updates.unset("leaseToken"),
      Updates.unset("leaseUntil")
    ))

  override def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): IO[Either[RepositoryError, Unit]] =
    transition(claim, Updates.combine(
      Updates.set("state", "Failed"),
      Updates.set("failure", failure.toString),
      Updates.set("finishedAt", Date.from(now)),
      Updates.unset("leaseOwner"),
      Updates.unset("leaseToken"),
      Updates.unset("leaseUntil")
    ))

  private def transition(claim: ClaimedEmbeddingWork, update: org.bson.conversions.Bson): IO[Either[RepositoryError, Unit]] =
    PublisherBridge.first(collection.updateOne(leaseFilter(claim), update)).map {
      case Some(result) if result.getMatchedCount == 1L => Right(())
      case _ => Left(RepositoryError.Conflict)
    }.handleError(_ => Left(RepositoryError.Unavailable))

  private def leaseFilter(claim: ClaimedEmbeddingWork) =
    Filters.and(
      Filters.eq("_id", claim.key.value),
      Filters.eq("generation", java.lang.Long.valueOf(claim.generation)),
      Filters.eq("state", "Processing"),
      Filters.eq("leaseToken", claim.leaseToken)
    )

  private def readClaim(document: Document): Either[StoredWorkError, ClaimedEmbeddingWork] =
    for {
      kind <- requiredString(document, "kind").flatMap(value => EmbeddingWorkKind.values.find(_.toString == value).toRight(StoredWorkError.InvalidDocument))
      entityId <- requiredString(document, "entityId")
      generation <- requiredNumber(document, "generation").map(_.longValue)
      attempts <- requiredNumber(document, "attempts").map(_.intValue)
      leaseToken <- requiredString(document, "leaseToken")
    } yield ClaimedEmbeddingWork(EmbeddingWorkKey(kind, entityId), generation, attempts, leaseToken)

  private def requiredString(document: Document, field: String): Either[StoredWorkError, String] =
    Option(document.get(field)).collect { case value: String => value }.toRight(StoredWorkError.InvalidDocument)

  private def requiredNumber(document: Document, field: String): Either[StoredWorkError, Number] =
    Option(document.get(field)).collect { case value: Number => value }.toRight(StoredWorkError.InvalidDocument)
}
