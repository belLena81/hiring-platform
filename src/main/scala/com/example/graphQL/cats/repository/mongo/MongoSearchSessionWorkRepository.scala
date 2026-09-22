package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.*
import com.mongodb.client.model.{Filters, FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoCollection, MongoDatabase}
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}

/** Mongo implementation of leased search-session materialization work. */
final class MongoSearchSessionWorkRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction
) extends SearchSessionWorkRepository with MongoOperationalEventInsertion with MongoConflictWriteMapping {
  private val work = database.getCollection("search_session_work")
  private val sessions = database.getCollection("search_sessions")
  private val outbox = database.getCollection("event_outbox")

  override def enqueue(value: PendingSearchSessionWork, now: Instant): IO[Either[RepositoryError, Unit]] = {
    val sanitized = MongoSearchSessionWorkCodecs.work(value, now)
    val filter = Filters.and(Filters.eq("_id", value.session.id.toString), Filters.eq("actorId", value.session.actorId.value.toString))
    PublisherBridge.first(work.updateOne(filter, new Document("$setOnInsert", sanitized), new UpdateOptions().upsert(true))).map {
      case Some(_) => Right(())
      case None => Left(RepositoryError.Unavailable)
    }.handleError(mapWrite)
  }

  override def findForActor(actorId: UserId, searchId: UUID): IO[Either[RepositoryError, Option[SearchSessionLookup]]] =
    PublisherBridge.first(sessions.find(Filters.and(Filters.eq("_id", searchId.toString), Filters.eq("actorId", actorId.value.toString)))).flatMap {
      case Some(document) => IO.pure(MongoStoredDocumentDecoding.repository(MongoHiringCodecs.readSearchSession(document)).map(session => Some(SearchSessionLookup.Materialized(session))))
      case None => PublisherBridge.first(work.find(Filters.and(Filters.eq("_id", searchId.toString), Filters.eq("actorId", actorId.value.toString)))).map {
        case None => Right(None)
        case Some(document) => MongoSearchSessionWorkCodecs.readState(document).leftMap(_ => RepositoryError.Unavailable).map {
          case SearchSessionWorkState.Failed => Some(SearchSessionLookup.Failed)
          case _ => Some(SearchSessionLookup.Pending)
        }
      }
    }.handleError(_ => Left(RepositoryError.Unavailable))

  override def claim(workerId: String, now: Instant, leaseUntil: Instant): IO[Either[RepositoryError, Option[ClaimedSearchSessionWork]]] =
    IO.randomUUID.map(_.toString).flatMap { token =>
      val ready = Filters.and(Filters.in("state", SearchSessionWorkState.Ready.toString, SearchSessionWorkState.Retry.toString), Filters.lte("availableAt", Date.from(now)))
      val expired = Filters.and(Filters.eq("state", SearchSessionWorkState.Processing.toString), Filters.lt("leaseUntil", Date.from(now)))
      val update = Updates.combine(
        Updates.set("state", SearchSessionWorkState.Processing.toString),
        Updates.set("leaseOwner", workerId),
        Updates.set("leaseToken", token),
        Updates.set("leaseUntil", Date.from(leaseUntil)),
        Updates.set("updatedAt", Date.from(now))
      )
      val options = new FindOneAndUpdateOptions().sort(Sorts.ascending("availableAt", "createdAt", "_id")).returnDocument(ReturnDocument.AFTER)
      PublisherBridge.first(work.findOneAndUpdate(Filters.or(ready, expired), update, options)).map {
        case None => Right(None)
        case Some(document) => MongoSearchSessionWorkCodecs.readClaim(document).leftMap(_ => RepositoryError.Unavailable).map(Some(_))
      }.handleError(_ => Left(RepositoryError.Unavailable))
    }

  override def complete(claim: ClaimedSearchSessionWork, now: Instant): IO[Either[RepositoryError, Unit]] =
    transactionRunner.run { active =>
      val lease = leaseFilter(claim)
      val sessionDocument = MongoHiringCodecs.searchSession(claim.work.session.copy(query = None))
      sessionDocument.remove("query")
      val saveSession = updateOne(sessions, active,
        Filters.and(Filters.eq("_id", claim.work.session.id.toString), Filters.eq("actorId", claim.work.session.actorId.value.toString)),
        new Document("$setOnInsert", sessionDocument), new UpdateOptions().upsert(true)
      )
      saveSession.flatMap {
        case None => IO.pure(Left(RepositoryError.Unavailable))
        case Some(_) => insertOperationalEvents(outbox, active, List(claim.work.event), now).flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right(()) => deleteOne(active, lease).map {
            case Some(result) if result.getDeletedCount == 1L => Right(())
            case Some(_) => Left(RepositoryError.Conflict)
            case None => Left(RepositoryError.Unavailable)
          }
        }
      }
    }.handleError(mapWrite)

  override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): IO[Either[RepositoryError, Unit]] =
    transition(claim, Updates.combine(
      Updates.set("state", SearchSessionWorkState.Retry.toString),
      Updates.set("availableAt", Date.from(availableAt)),
      Updates.inc("attempts", java.lang.Integer.valueOf(1)),
      Updates.unset("leaseOwner"), Updates.unset("leaseToken"), Updates.unset("leaseUntil"),
      Updates.set("updatedAt", Date.from(availableAt))
    ))

  override def fail(claim: ClaimedSearchSessionWork, failure: SearchSessionWorkFailure, now: Instant): IO[Either[RepositoryError, Unit]] =
    transition(claim, Updates.combine(
      Updates.set("state", SearchSessionWorkState.Failed.toString),
      Updates.set("failure", failure.toString),
      Updates.set("finishedAt", Date.from(now)),
      Updates.set("retentionExpiresAt", Date.from(now.plusSeconds(7L * 24L * 60L * 60L))),
      Updates.unset("leaseOwner"), Updates.unset("leaseToken"), Updates.unset("leaseUntil"),
      Updates.set("updatedAt", Date.from(now))
    ))

  private def transition(claim: ClaimedSearchSessionWork, update: org.bson.conversions.Bson): IO[Either[RepositoryError, Unit]] =
    updateOne(work, None, leaseFilter(claim), update).map {
      case Some(result) if result.getMatchedCount == 1L => Right(())
      case Some(_) => Left(RepositoryError.Conflict)
      case None => Left(RepositoryError.Unavailable)
    }.handleError(_ => Left(RepositoryError.Unavailable))

  private def leaseFilter(claim: ClaimedSearchSessionWork) = Filters.and(
    Filters.eq("_id", claim.work.session.id.toString),
    Filters.eq("state", SearchSessionWorkState.Processing.toString),
    Filters.eq("leaseToken", claim.leaseToken)
  )

  private def updateOne(collection: MongoCollection[Document], session: Option[ClientSession], filter: org.bson.conversions.Bson, update: org.bson.conversions.Bson, options: UpdateOptions = new UpdateOptions()) =
    session.fold(PublisherBridge.first(collection.updateOne(filter, update, options)))(active => PublisherBridge.first(collection.updateOne(active, filter, update, options)))

  private def deleteOne(session: Option[ClientSession], filter: org.bson.conversions.Bson) =
    session.fold(PublisherBridge.first(work.deleteOne(filter)))(active => PublisherBridge.first(work.deleteOne(active, filter)))
}

object MongoSearchSessionWorkRepository {
  def standalone(database: MongoDatabase): MongoSearchSessionWorkRepository =
    new MongoSearchSessionWorkRepository(database)

  def transactional(database: MongoDatabase, client: MongoClient): MongoSearchSessionWorkRepository =
    new MongoSearchSessionWorkRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict))
}
