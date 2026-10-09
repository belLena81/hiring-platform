package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.OperationalEventJson
import com.mongodb.client.model.{Sorts, UpdateOptions}
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*

/** Mongo implementation of leased search-session materialization work. */
final class MongoSearchSessionWorkRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics,
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends SearchSessionWorkRepository
    with MongoOperationalEventInsertion
    with MongoConflictWriteMapping {
  private val work = Mongo4catsCollections.documents(database, MongoCollections.SearchSessionWork)
  private val sessions = Mongo4catsCollections.documents(database, MongoCollections.SearchSessions)
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private val processing = MongoFilter.eq(MongoFields.State, SearchSessionWorkState.Processing.toString)

  override def enqueue(value: PendingSearchSessionWork, now: Instant): RepositoryIO[Unit] =
    RepositoryIO
      .fromEither(
        OperationalEventJson.validate(value.event).leftMap(_ => RepositoryError.InvalidEvent).flatMap { _ =>
          Either.cond(
            value.addressesSession,
            (),
            RepositoryError.InvalidEvent
          )
        }
      )
      .flatMap { _ =>
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
      }

  override def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedSearchSessionWork]] =
    RepositoryIO.lift(uuidGen.randomUUID.map(_.toString)).flatMap { token =>
      val available = MongoFilter.and(
        MongoFilter
          .in(MongoFields.State, List(SearchSessionWorkState.Ready, SearchSessionWorkState.Retry).map(_.toString)),
        MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))
      )
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "searchSessionWork.claim") {
          MongoLeaseQueue.claimNext(
            work,
            None,
            MongoLeaseQueue.claimable(available, processing, now),
            MongoLeaseQueue.leaseStamp(SearchSessionWorkState.Processing.toString, workerId, token, leaseUntil, now),
            Sorts.ascending(MongoFields.AvailableAt, MongoFields.CreatedAt, MongoFields.Id)
          )(document =>
            RepositoryIO.fromEither(
              MongoSearchSessionWorkCodecs.readClaim(document).leftMap(_ => RepositoryError.InvalidStoredData)
            )
          )
        }
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
              MongoSessionOperations.updateOne(
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
                  RepositoryIO.lift(MongoSessionOperations.deleteOne(work, active, lease)).subflatMap {
                    case Some(result) if result.getDeletedCount == 1L => Right(())
                    case Some(_)                                      => Left(RepositoryError.Conflict)
                    case None                                         => Left(RepositoryError.MissingWriteResult)
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
          .lift(MongoSessionOperations.updateOne(work, None, leaseFilter(claim), update))
          .subflatMap {
            case Some(result) if result.getMatchedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      }

  private def leaseFilter(claim: ClaimedSearchSessionWork): MongoFilter = MongoFilter.and(
    MongoFilter.eq(MongoFields.Id, claim.work.session.id.toString),
    processing,
    MongoFilter.eq(MongoFields.LeaseToken, claim.leaseToken)
  )

  private def setOnInsertDocument(document: Document): MongoUpdate =
    MongoUpdate.combine(
      document.entrySet().asScala.toList.map(field => MongoUpdate.setOnInsert(field.getKey, field.getValue))*
    )

}

object MongoSearchSessionWorkRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics,
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): MongoSearchSessionWorkRepository =
    new MongoSearchSessionWorkRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics,
      uuidGen
    )
}
