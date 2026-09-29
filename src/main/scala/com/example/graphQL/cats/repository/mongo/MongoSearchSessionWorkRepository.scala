package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions}
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

/** Mongo implementation of leased search-session materialization work. */
final class MongoSearchSessionWorkRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics
) extends SearchSessionWorkRepository
    with MongoOperationalEventInsertion
    with MongoConflictWriteMapping {
  private val work = Mongo4catsCollections.documents(database, MongoCollections.SearchSessionWork)
  private val sessions = Mongo4catsCollections.documents(database, MongoCollections.SearchSessions)
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)

  override def enqueue(value: PendingSearchSessionWork, now: Instant): RepositoryIO[Unit] = {
    val sanitized = MongoSearchSessionWorkCodecs.work(value, now)
    val filter = MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, value.session.id.toString),
      MongoFilter.eq(MongoFields.ActorId, value.session.actorId.value.toString)
    )
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSessionWork.enqueue") {
        RepositoryIO
          .lift(
            MongoSessionOperations
              .updateOne(work, None, filter, setOnInsertDocument(sanitized), new UpdateOptions().upsert(true))
          )
          .subflatMap {
            case Some(_) => Right(())
            case None    => Left(RepositoryError.MissingWriteResult)
          }
      }(mapWrite)
  }

  override def findForActor(actorId: UserId, searchId: UUID): RepositoryIO[Option[SearchSessionLookup]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSessionWork.findForActor") {
        RepositoryIO
          .lift(
            MongoSessionOperations
              .findOne(
                sessions,
                None,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, searchId.toString),
                  MongoFilter.eq(MongoFields.ActorId, actorId.value.toString)
                )
              )
          )
          .flatMap {
            case Some(document) =>
              RepositoryIO.fromEither(
                MongoStoredDocumentDecoding
                  .repository(MongoHiringCodecs.readSearchSession(document))
                  .map(session => Some(SearchSessionLookup.Materialized(session)))
              )
            case None =>
              RepositoryIO
                .lift(
                  MongoSessionOperations
                    .findOne(
                      work,
                      None,
                      MongoFilter.and(
                        MongoFilter.eq(MongoFields.Id, searchId.toString),
                        MongoFilter.eq(MongoFields.ActorId, actorId.value.toString)
                      )
                    )
                )
                .subflatMap {
                  case None           => Right(None)
                  case Some(document) =>
                    MongoSearchSessionWorkCodecs
                      .readState(document)
                      .leftMap(_ => RepositoryError.InvalidStoredData)
                      .map {
                        case SearchSessionWorkState.Failed => Some(SearchSessionLookup.Failed)
                        case _                             => Some(SearchSessionLookup.Pending)
                      }
                }
          }
      }(_ => Left(RepositoryError.Unavailable))

  override def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedSearchSessionWork]] =
    RepositoryIO.lift(IO.randomUUID.map(_.toString)).flatMap { token =>
      val ready = MongoFilter.and(
        MongoFilter
          .in(MongoFields.State, List(SearchSessionWorkState.Ready.toString, SearchSessionWorkState.Retry.toString)),
        MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))
      )
      val expired = MongoFilter.and(
        MongoFilter.eq(MongoFields.State, SearchSessionWorkState.Processing.toString),
        MongoFilter.lt(MongoFields.LeaseUntil, Date.from(now))
      )
      val update = MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, SearchSessionWorkState.Processing.toString),
        MongoUpdate.set(MongoFields.LeaseOwner, workerId),
        MongoUpdate.set(MongoFields.LeaseToken, token),
        MongoUpdate.set(MongoFields.LeaseUntil, Date.from(leaseUntil)),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
      )
      val options = new FindOneAndUpdateOptions()
        .sort(Sorts.ascending(MongoFields.AvailableAt, MongoFields.CreatedAt, MongoFields.Id))
        .returnDocument(ReturnDocument.AFTER)
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "searchSessionWork.claim") {
          RepositoryIO
            .lift(
              work
                .flatMap(_.findOneAndUpdate(MongoFilter.or(ready, expired).bson, update.bson, options))
            )
            .subflatMap {
              case None           => Right(None)
              case Some(document) =>
                MongoSearchSessionWorkCodecs
                  .readClaim(document)
                  .leftMap(_ => RepositoryError.InvalidStoredData)
                  .map(Some(_))
            }
        }(_ => Left(RepositoryError.Unavailable))
    }

  override def complete(claim: ClaimedSearchSessionWork, now: Instant): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSessionWork.complete") {
        transactionRunner
          .run { active =>
            val lease = leaseFilter(claim)
            val sessionDocument = MongoHiringCodecs.searchSession(claim.work.session.copy(query = None))
            sessionDocument.remove("query")
            val saveSession = RepositoryIO.lift(
              updateOne(
                sessions,
                active,
                MongoFilter.and(
                  MongoFilter.eq(MongoFields.Id, claim.work.session.id.toString),
                  MongoFilter.eq(MongoFields.ActorId, claim.work.session.actorId.value.toString)
                ),
                setOnInsertDocument(sessionDocument),
                new UpdateOptions().upsert(true)
              )
            )
            saveSession.flatMap {
              case None    => RepositoryIO.fromEither(Left(RepositoryError.MissingWriteResult))
              case Some(_) =>
                insertOperationalEvents(outbox, active, List(claim.work.event), now, diagnostics).flatMap { _ =>
                  RepositoryIO.lift(deleteOne(active, lease)).subflatMap {
                    case result if result.getDeletedCount == 1L => Right(())
                    case _                                      => Left(RepositoryError.Conflict)
                  }
                }
            }
          }
      }(mapWrite)

  override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryIO[Unit] =
    transition(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, SearchSessionWorkState.Retry.toString),
        MongoUpdate.set(MongoFields.AvailableAt, Date.from(availableAt)),
        MongoUpdate.inc(MongoFields.Attempts, java.lang.Integer.valueOf(1)),
        MongoUpdate.unset(MongoFields.LeaseOwner),
        MongoUpdate.unset(MongoFields.LeaseToken),
        MongoUpdate.unset(MongoFields.LeaseUntil),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(availableAt))
      )
    )

  override def fail(
      claim: ClaimedSearchSessionWork,
      failure: SearchSessionWorkFailure,
      now: Instant
  ): RepositoryIO[Unit] =
    transition(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, SearchSessionWorkState.Failed.toString),
        MongoUpdate.set(MongoFields.Failure, failure.toString),
        MongoUpdate.set(MongoFields.FinishedAt, Date.from(now)),
        MongoUpdate.set(MongoFields.RetentionExpiresAt, Date.from(now.plusSeconds(7L * 24L * 60L * 60L))),
        MongoUpdate.unset(MongoFields.LeaseOwner),
        MongoUpdate.unset(MongoFields.LeaseToken),
        MongoUpdate.unset(MongoFields.LeaseUntil),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
      )
    )

  private def transition(
      claim: ClaimedSearchSessionWork,
      update: MongoUpdate
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "searchSessionWork.transition") {
        RepositoryIO
          .lift(updateOne(work, None, leaseFilter(claim), update))
          .subflatMap {
            case Some(result) if result.getMatchedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      }(_ => Left(RepositoryError.Unavailable))

  private def leaseFilter(claim: ClaimedSearchSessionWork): MongoFilter = MongoFilter.and(
    MongoFilter.eq(MongoFields.Id, claim.work.session.id.toString),
    MongoFilter.eq(MongoFields.State, SearchSessionWorkState.Processing.toString),
    MongoFilter.eq(MongoFields.LeaseToken, claim.leaseToken)
  )

  private def setOnInsertDocument(document: Document): MongoUpdate =
    MongoUpdate.combine(
      document.entrySet().asScala.toList.map(field => MongoUpdate.setOnInsert(field.getKey, field.getValue))*
    )

  private def updateOne(
      collection: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: UpdateOptions = new UpdateOptions()
  ) =
    MongoSessionOperations.updateOne(collection, session, filter, update, options)

  private def deleteOne(session: Option[ClientSession[IO]], filter: MongoFilter) =
    work.flatMap(collection =>
      session.fold(
        collection.deleteOne(filter.bson, new com.mongodb.client.model.DeleteOptions)
      )(active => collection.deleteOne(active, filter.sessionFilter, new com.mongodb.client.model.DeleteOptions))
    )
}

object MongoSearchSessionWorkRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics
  ): MongoSearchSessionWorkRepository =
    new MongoSearchSessionWorkRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}
