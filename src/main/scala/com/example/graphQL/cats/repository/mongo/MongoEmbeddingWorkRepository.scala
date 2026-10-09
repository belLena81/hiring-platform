package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Sorts, UpdateOptions}
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
final class MongoEmbeddingWorkRepository(
    database: MongoDatabase[IO],
    diagnostics: Diagnostics,
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends EmbeddingWorkRepository
    with MongoEmbeddingWorkEnqueuer {
  override val requiresTransaction: Boolean = true
  private val processing = MongoFilter.eq(MongoFields.State, EmbeddingWorkState.Processing.toString)

  def inspectForAdmin(
      key: EmbeddingWorkKey,
      adminId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      transactions: MongoTransactionRunner
  ): RepositoryIO[Option[EmbeddingWorkInspection]] = transactions.run { session =>
    for {
      _ <- authorizeAdmin(adminId, session)
      row <- RepositoryIO.lift(
        MongoSessionOperations.findOne(collection, session, MongoFilter.eq(MongoFields.Id, key.value))
      )
      result <- row.traverse(document =>
        RepositoryIO.fromEither(MongoDocumentFields.toRepository(for {
          generation <- MongoDocumentFields.requiredNumberAsLong(document, MongoFields.Generation)
          attempts <- MongoDocumentFields.requiredNumberAsInt(document, MongoFields.Attempts)
          state <- MongoDocumentFields.requiredEnum(document, MongoFields.State)(
            MongoDocumentFields.byName(EmbeddingWorkState.values)
          )
          failure <- MongoDocumentFields.optionalEnum(document, MongoFields.Failure)(
            MongoDocumentFields.byName(EmbeddingWorkFailure.values)
          )
        } yield EmbeddingWorkInspection(generation, attempts, state, failure)))
      )
    } yield result
  }

  private def authorizeAdmin(
      adminId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[Unit] = RepositoryIO
    .lift(
      MongoSessionOperations.updateOne(
        Mongo4catsCollections.documents(database, MongoCollections.Users),
        session,
        MongoFilter.and(
          MongoFilter.eq(MongoFields.Id, adminId.value.toString),
          MongoFilter.eq(MongoFields.Role, "Admin"),
          MongoFilter.eq(MongoFields.AccountStatus, "Active"),
          MongoFilter.eq(MongoFields.AdminSingletonKey, "singleton-admin"),
          MongoFilter.lt(MongoFields.Version, Long.MaxValue)
        ),
        MongoUpdate.inc(MongoFields.Version, 1L)
      )
    )
    .subflatMap(MongoRepositorySupport.matchedOne(_, RepositoryError.AuthorityRevoked))

  /** Maintenance capability: the trusted Admin and failed generation are checked in the same transaction. A
    * changed/repaired generation is never overwritten. No provider calls occur here.
    */
  def repairFailed(
      key: EmbeddingWorkKey,
      expectedGeneration: Long,
      adminId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      now: Instant,
      transactions: MongoTransactionRunner
  ): RepositoryIO[Boolean] =
    if (expectedGeneration < 1L || expectedGeneration == Long.MaxValue)
      RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      transactions.run { session =>
        for {
          _ <- authorizeAdmin(adminId, session)
          repaired <- RepositoryIO.lift(
            MongoSessionOperations.updateOne(
              collection,
              session,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, key.value),
                MongoFilter.eq(MongoFields.State, EmbeddingWorkState.Failed.toString),
                MongoFilter.eq(MongoFields.Generation, expectedGeneration)
              ),
              MongoUpdate.combine(
                MongoUpdate.set(MongoFields.State, EmbeddingWorkState.Ready.toString),
                MongoUpdate.set(MongoFields.AvailableAt, Date.from(now)),
                MongoUpdate.set(MongoFields.UpdatedAt, Date.from(now)),
                MongoUpdate.set(MongoFields.Attempts, 0),
                MongoUpdate.inc(MongoFields.Generation, 1L),
                MongoUpdate.unset(MongoFields.Failure),
                MongoUpdate.unset(MongoFields.FinishedAt),
                MongoLeaseQueue.releaseLease
              )
            )
          )
          result <- RepositoryIO.fromEither(repaired match {
            case Some(result) => Right(result.getMatchedCount == 1L)
            case None         => Left(RepositoryError.MissingWriteResult)
          })
        } yield result
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
      }
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
    val processing = expression(
      "$eq",
      reference(MongoFields.State),
      literal(new BsonString(EmbeddingWorkState.Processing.toString))
    )
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
      .append(
        MongoFields.State,
        retainLease(MongoFields.State, literal(new BsonString(EmbeddingWorkState.Ready.toString)))
      )
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
  ): RepositoryIO[Option[ClaimedEmbeddingWork]] =
    RepositoryIO.lift(uuidGen.randomUUID.map(_.toString)).flatMap { token =>
      val available = MongoFilter.and(
        MongoFilter.in(MongoFields.State, List(EmbeddingWorkState.Ready, EmbeddingWorkState.Retry).map(_.toString)),
        MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))
      )
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "embeddingWork.claim") {
          MongoLeaseQueue.claimNext(
            collection,
            None,
            MongoLeaseQueue.claimable(available, processing, now),
            MongoLeaseQueue.leaseStamp(EmbeddingWorkState.Processing.toString, workerId, token, leaseUntil, now),
            Sorts.ascending(MongoFields.AvailableAt, MongoFields.Id)
          )(document => RepositoryIO.fromEither(readClaim(document)))
        }
    }

  override def renew(claim: ClaimedEmbeddingWork, now: Instant, leaseUntil: Instant): RepositoryIO[Boolean] =
    if (!leaseUntil.isAfter(now)) RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
    else
      MongoRepositorySupport.repositoryGuard(diagnostics, "embeddingWork.renew") {
        MongoLeaseQueue
          .renewHeld(collection, None, MongoFilter.and(leaseFilter(claim), MongoLeaseQueue.leaseHeld(now)), leaseUntil)
      }

  override def complete(claim: ClaimedEmbeddingWork): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "embeddingWork.complete") {
        RepositoryIO
          .lift(MongoSessionOperations.deleteOne(collection, None, leaseFilter(claim)))
          .subflatMap {
            case Some(result) if result.getDeletedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      }

  override def retry(
      claim: ClaimedEmbeddingWork,
      availableAt: Instant,
      chargeAttempt: Boolean = true
  ): RepositoryIO[Unit] = {
    val updates = List(
      MongoUpdate.set(MongoFields.State, EmbeddingWorkState.Retry.toString),
      MongoUpdate.set(MongoFields.AvailableAt, Date.from(availableAt)),
      MongoLeaseQueue.releaseLease
    ) ++ Option.when(chargeAttempt)(MongoUpdate.inc(MongoFields.Attempts, java.lang.Integer.valueOf(1))).toList
    transition(claim, MongoUpdate.combine(updates*))
  }

  override def fail(
      claim: ClaimedEmbeddingWork,
      failure: EmbeddingWorkFailure,
      now: Instant
  ): RepositoryIO[Unit] =
    transition(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, EmbeddingWorkState.Failed.toString),
        MongoUpdate.set(MongoFields.Failure, failure.toString),
        MongoUpdate.set(MongoFields.FinishedAt, Date.from(now)),
        MongoLeaseQueue.releaseLease
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
          .subflatMap(MongoRepositorySupport.matchedOne(_))
      }

  private def leaseFilter(claim: ClaimedEmbeddingWork) =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.Id, claim.key.value),
      MongoFilter.eq(MongoFields.Generation, java.lang.Long.valueOf(claim.generation)),
      processing,
      MongoFilter.eq(MongoFields.LeaseToken, claim.leaseToken)
    )

  private def readClaim(document: Document): Either[RepositoryError, ClaimedEmbeddingWork] =
    MongoDocumentFields.toRepository(for {
      kind <- MongoDocumentFields.requiredEnum(document, MongoFields.Kind)(
        MongoDocumentFields.byName(EmbeddingWorkKind.values)
      )
      entityId <- MongoDocumentFields.requiredString(document, MongoFields.WorkEntityId)
      generation <- MongoDocumentFields.requiredNumberAsLong(document, MongoFields.Generation)
      attempts <- MongoDocumentFields.requiredNumberAsInt(document, MongoFields.Attempts)
      leaseToken <- MongoDocumentFields.requiredString(document, MongoFields.LeaseToken)
    } yield ClaimedEmbeddingWork(EmbeddingWorkKey(kind, entityId), generation, attempts, leaseToken))
}

final case class EmbeddingWorkInspection(
    generation: Long,
    attempts: Int,
    state: EmbeddingWorkState,
    failure: Option[EmbeddingWorkFailure]
)
