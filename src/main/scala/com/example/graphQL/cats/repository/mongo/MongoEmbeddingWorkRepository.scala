package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.time.Instant
import java.util.Date

trait MongoEmbeddingWorkEnqueuer {
  def requiresTransaction: Boolean

  def enqueue(
      session: Option[ClientSession[IO]],
      key: EmbeddingWorkKey,
      now: Instant
  ): IO[Either[RepositoryError, Unit]]
}

object MongoEmbeddingWorkEnqueuer {

  /** The configured disabled mode deliberately creates no durable embedding work or backing adapter. */
  val disabled: MongoEmbeddingWorkEnqueuer = new MongoEmbeddingWorkEnqueuer {
    override val requiresTransaction: Boolean = false

    override def enqueue(
        _session: Option[ClientSession[IO]],
        _key: EmbeddingWorkKey,
        _now: Instant
    ): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  }
}

/** Durable, coalesced embedding work. A newer enqueue increments generation so an older lease cannot delete it. */
final class MongoEmbeddingWorkRepository(database: MongoDatabase[IO], diagnostics: Diagnostics = Diagnostics.noop)
    extends EmbeddingWorkRepository
    with MongoEmbeddingWorkEnqueuer {
  override val requiresTransaction: Boolean = true
  private enum StoredWorkError {
    case InvalidDocument
  }

  private val collection = Mongo4catsCollections.documents(database, MongoCollections.EmbeddingWork)

  override def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryIO[Unit] =
    RepositoryIO.fromIOEither(enqueue(None, key, now))

  /** Persists work in the caller's Mongo transaction. This is deliberately a concrete Mongo capability: the generic
    * work port has no transaction/session concept.
    */
  def enqueue(session: ClientSession[IO], key: EmbeddingWorkKey, now: Instant): IO[Either[RepositoryError, Unit]] =
    enqueue(Some(session), key, now)

  override def enqueue(
      session: Option[ClientSession[IO]],
      key: EmbeddingWorkKey,
      now: Instant
  ): IO[Either[RepositoryError, Unit]] = {
    val readyUpdate = MongoUpdate.combine(
      MongoUpdate.setOnInsert(MongoFields.Kind, key.kind.toString),
      MongoUpdate.setOnInsert(MongoFields.WorkEntityId, key.entityId),
      MongoUpdate.setOnInsert(MongoFields.CreatedAt, Date.from(now)),
      MongoUpdate.inc(MongoFields.Generation, java.lang.Long.valueOf(1L)),
      MongoUpdate.set(MongoFields.Attempts, java.lang.Integer.valueOf(0)),
      MongoUpdate.set(MongoFields.State, "Ready"),
      MongoUpdate.set(MongoFields.AvailableAt, Date.from(now)),
      MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now)),
      MongoUpdate.unset(MongoFields.LeaseOwner),
      MongoUpdate.unset(MongoFields.LeaseToken),
      MongoUpdate.unset(MongoFields.LeaseUntil),
      MongoUpdate.unset(MongoFields.Failure)
    )
    val refreshActiveLease = MongoUpdate.combine(
      MongoUpdate.inc(MongoFields.Generation, java.lang.Long.valueOf(1L)),
      MongoUpdate.set(MongoFields.Attempts, java.lang.Integer.valueOf(0)),
      MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
    )
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "embeddingWork.enqueue") {
        updateOne(
          session,
          MongoFilter.and(MongoFilter.eq(MongoFields.Id, key.value), MongoFilter.ne(MongoFields.State, "Processing")),
          readyUpdate,
          new UpdateOptions().upsert(true)
        ).flatMap {
          case Some(result) if result.getMatchedCount == 1L || result.getUpsertedId != null => IO.pure(Right(()))
          case None    => IO.pure(Left(RepositoryError.MissingWriteResult))
          case Some(_) =>
            updateOne(
              session,
              MongoFilter
                .and(MongoFilter.eq(MongoFields.Id, key.value), MongoFilter.eq(MongoFields.State, "Processing")),
              refreshActiveLease
            ).map {
              case Some(result) if result.getMatchedCount == 1L => Right(())
              case Some(_)                                      => Left(RepositoryError.Conflict)
              case None                                         => Left(RepositoryError.MissingWriteResult)
            }
        }
      }(_ => Left(RepositoryError.Unavailable))
      .value
  }

  private def updateOne(
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: UpdateOptions = new UpdateOptions()
  ) =
    MongoSessionOperations.updateOne(collection, session, filter, update, options)

  override def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedEmbeddingWork]] = {
    RepositoryIO.lift(IO.randomUUID.map(_.toString)).flatMap { token =>
      val available = MongoFilter.and(
        MongoFilter.in(MongoFields.State, Seq("Ready", "Retry")),
        MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))
      )
      val expiredLease = MongoFilter.and(
        MongoFilter.eq(MongoFields.State, "Processing"),
        MongoFilter.lt(MongoFields.LeaseUntil, Date.from(now))
      )
      val update = MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, "Processing"),
        MongoUpdate.set(MongoFields.LeaseOwner, workerId),
        MongoUpdate.set(MongoFields.LeaseToken, token),
        MongoUpdate.set(MongoFields.LeaseUntil, Date.from(leaseUntil)),
        MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now))
      )
      val options = new FindOneAndUpdateOptions()
        .returnDocument(ReturnDocument.AFTER)
        .sort(Sorts.ascending(MongoFields.AvailableAt, MongoFields.Id))
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "embeddingWork.claim") {
          collection
            .flatMap(_.findOneAndUpdate(MongoFilter.or(available, expiredLease).bson, update.bson, options))
            .map {
              case Some(document) =>
                readClaim(document) match {
                  case Right(claim) => Right(Some(claim))
                  case Left(_)      => Left(RepositoryError.InvalidStoredData)
                }
              case None => Right(None)
            }
        }(_ => Left(RepositoryError.Unavailable))
    }
  }

  override def complete(claim: ClaimedEmbeddingWork): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "embeddingWork.complete") {
        collection
          .flatMap(_.deleteOne(leaseFilter(claim).bson, new com.mongodb.client.model.DeleteOptions))
          .map(result => if (result.getDeletedCount == 1L) Right(()) else Left(RepositoryError.Conflict))
      }(_ => Left(RepositoryError.Unavailable))

  override def retry(claim: ClaimedEmbeddingWork, availableAt: Instant): RepositoryIO[Unit] =
    transition(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, "Retry"),
        MongoUpdate.set(MongoFields.AvailableAt, Date.from(availableAt)),
        MongoUpdate.inc(MongoFields.Attempts, java.lang.Integer.valueOf(1)),
        MongoUpdate.unset(MongoFields.LeaseOwner),
        MongoUpdate.unset(MongoFields.LeaseToken),
        MongoUpdate.unset(MongoFields.LeaseUntil)
      )
    )

  override def fail(
      claim: ClaimedEmbeddingWork,
      failure: EmbeddingWorkFailure,
      now: Instant
  ): RepositoryIO[Unit] =
    transition(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, "Failed"),
        MongoUpdate.set(MongoFields.Failure, failure.toString),
        MongoUpdate.set(MongoFields.FinishedAt, Date.from(now)),
        MongoUpdate.unset(MongoFields.LeaseOwner),
        MongoUpdate.unset(MongoFields.LeaseToken),
        MongoUpdate.unset(MongoFields.LeaseUntil)
      )
    )

  private def transition(
      claim: ClaimedEmbeddingWork,
      update: MongoUpdate
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "embeddingWork.transition") {
        MongoSessionOperations
          .updateOne(collection, None, leaseFilter(claim), update)
          .map {
            case Some(result) if result.getMatchedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      }(_ => Left(RepositoryError.Unavailable))

  private def leaseFilter(claim: ClaimedEmbeddingWork) =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, claim.key.value),
      MongoFilter.eq(MongoFields.Generation, java.lang.Long.valueOf(claim.generation)),
      MongoFilter.eq(MongoFields.State, "Processing"),
      MongoFilter.eq(MongoFields.LeaseToken, claim.leaseToken)
    )

  private def readClaim(document: Document): Either[StoredWorkError, ClaimedEmbeddingWork] =
    for {
      kind <- requiredString(document, MongoFields.Kind).flatMap(value =>
        EmbeddingWorkKind.values.find(_.toString == value).toRight(StoredWorkError.InvalidDocument)
      )
      entityId <- requiredString(document, MongoFields.WorkEntityId)
      generation <- requiredNumber(document, MongoFields.Generation).map(_.longValue)
      attempts <- requiredNumber(document, MongoFields.Attempts).map(_.intValue)
      leaseToken <- requiredString(document, MongoFields.LeaseToken)
    } yield ClaimedEmbeddingWork(EmbeddingWorkKey(kind, entityId), generation, attempts, leaseToken)

  private def requiredString(document: Document, field: String): Either[StoredWorkError, String] =
    Option(document.get(field)).collect { case value: String => value }.toRight(StoredWorkError.InvalidDocument)

  private def requiredNumber(document: Document, field: String): Either[StoredWorkError, Number] =
    Option(document.get(field)).collect { case value: Number => value }.toRight(StoredWorkError.InvalidDocument)
}
