package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{FindOneAndUpdateOptions, ReturnDocument, Sorts, UpdateOptions}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import org.bson.{Document, BsonDocument, BsonString, BsonInt32, BsonInt64, BsonDateTime, BsonArray, BsonValue}
import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*

trait MongoEmbeddingWorkEnqueuer {
  def requiresTransaction: Boolean

  def enqueue(
      session: Option[ClientSession[IO]],
      key: EmbeddingWorkKey,
      now: Instant
  ): RepositoryIO[Unit]
}

object MongoEmbeddingWorkEnqueuer {

  /** The configured disabled mode deliberately creates no durable embedding work or backing adapter. */
  val disabled: MongoEmbeddingWorkEnqueuer = new MongoEmbeddingWorkEnqueuer {
    override val requiresTransaction: Boolean = false

    override def enqueue(
        _session: Option[ClientSession[IO]],
        _key: EmbeddingWorkKey,
        _now: Instant
    ): RepositoryIO[Unit] = RepositoryIO.fromEither(Right(()))
  }
}

/** Durable, coalesced embedding work. A newer enqueue increments generation so an older lease cannot delete it. */
final class MongoEmbeddingWorkRepository(database: MongoDatabase[IO], diagnostics: Diagnostics)
    extends EmbeddingWorkRepository
    with MongoEmbeddingWorkEnqueuer {
  override val requiresTransaction: Boolean = true
  private enum StoredWorkError {
    case InvalidDocument
  }

  private val collection = Mongo4catsCollections.documents(database, MongoCollections.EmbeddingWork)

  override def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryIO[Unit] =
    enqueue(None, key, now)

  /** Persists work in the caller's Mongo transaction. This is deliberately a concrete Mongo capability: the generic
    * work port has no transaction/session concept.
    */
  def enqueue(session: ClientSession[IO], key: EmbeddingWorkKey, now: Instant): RepositoryIO[Unit] =
    enqueue(Some(session), key, now)

  override def enqueue(
      session: Option[ClientSession[IO]],
      key: EmbeddingWorkKey,
      now: Instant
  ): RepositoryIO[Unit] = {
    val pipeline = List(enqueueStage(key, now))
    // A state predicate in an upsert filter attempts a duplicate insert for an active lease.
    // Conditional pipeline fields instead inspect state within one identity-only atomic write.
    MongoRepositorySupport
      .transactionGuard(diagnostics, "embeddingWork.enqueue", session) {
        RepositoryIO
          .lift(
            MongoSessionOperations.updateOnePipeline(
              collection,
              session,
              MongoFilter.eq(MongoFields.Id, key.value),
              pipeline,
              new UpdateOptions().upsert(true)
            )
          )
          .subflatMap {
            case Some(result) if result.wasAcknowledged() && result.getMatchedCount + result.getUpserts.size == 1 =>
              Right(())
            case Some(_) => Left(RepositoryError.Conflict)
            case None    => Left(RepositoryError.MissingWriteResult)
          }
      }(_ => Left(RepositoryError.Unavailable))
  }

  private def enqueueStage(key: EmbeddingWorkKey, now: Instant): BsonDocument = {
    def reference(field: String): BsonString = new BsonString(s"$$$field")
    def expression(operator: String, arguments: BsonValue*): BsonDocument =
      new BsonDocument(operator, new BsonArray(arguments.toList.asJava))
    def literal(value: BsonValue): BsonDocument = new BsonDocument("$literal", value)
    def initialized(field: String, value: BsonValue): BsonDocument =
      expression(
        "$cond",
        expression("$eq", new BsonDocument("$type", reference(field)), new BsonString("missing")),
        literal(value),
        reference(field)
      )
    val processing = expression("$eq", reference(MongoFields.State), literal(new BsonString("Processing")))
    def retainLease(field: String, otherwise: BsonValue): BsonDocument =
      expression("$cond", processing, reference(field), otherwise)
    val timestamp = literal(new BsonDateTime(now.toEpochMilli))
    val fields = new BsonDocument()
      .append(MongoFields.Kind, initialized(MongoFields.Kind, new BsonString(key.kind.toString)))
      .append(MongoFields.WorkEntityId, initialized(MongoFields.WorkEntityId, new BsonString(key.entityId)))
      .append(MongoFields.CreatedAt, initialized(MongoFields.CreatedAt, new BsonDateTime(now.toEpochMilli)))
      .append(
        MongoFields.Generation,
        expression("$add", initialized(MongoFields.Generation, new BsonInt64(0L)), new BsonInt64(1L))
      )
      .append(MongoFields.Attempts, literal(new BsonInt32(0)))
      .append(MongoFields.UpdatedAt, timestamp)
      .append(MongoFields.State, retainLease(MongoFields.State, literal(new BsonString("Ready"))))
      .append(MongoFields.AvailableAt, retainLease(MongoFields.AvailableAt, timestamp))
    List(
      MongoFields.LeaseOwner,
      MongoFields.LeaseToken,
      MongoFields.LeaseUntil,
      MongoFields.Failure,
      MongoFields.FinishedAt
    )
      .foreach(field => { val _ = fields.append(field, retainLease(field, new BsonString("$$REMOVE"))) })
    new BsonDocument("$set", fields)
  }

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
          RepositoryIO
            .lift(
              collection
                .flatMap(_.findOneAndUpdate(MongoFilter.or(available, expiredLease).bson, update.bson, options))
            )
            .subflatMap {
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
        RepositoryIO
          .lift(
            collection
              .flatMap(_.deleteOne(leaseFilter(claim).bson, new com.mongodb.client.model.DeleteOptions))
          )
          .subflatMap(result => if (result.getDeletedCount == 1L) Right(()) else Left(RepositoryError.Conflict))
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
        RepositoryIO
          .lift(
            MongoSessionOperations
              .updateOne(collection, None, leaseFilter(claim), update)
          )
          .subflatMap {
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
