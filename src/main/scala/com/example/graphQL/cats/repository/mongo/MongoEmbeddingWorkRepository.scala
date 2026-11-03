package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.MongoF.asRepository
import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{Diagnostics, TransactionOperation}
import com.mongodb.client.model.{ReturnDocument, Sorts, UpdateOptions}
import mongo4cats.client.ClientSession
import mongo4cats.database.MongoDatabase
import org.bson.{Document, BsonDocument, BsonString, BsonInt32, BsonInt64, BsonDateTime, BsonArray, BsonValue}
import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

trait MongoEmbeddingWorkEnqueuer {
  def requiresTransaction: Boolean

  def enqueue(
      session: Option[ClientSession[IO]],
      key: EmbeddingWorkKey,
      now: Instant
  ): RepositoryF[IO, Unit]
}

object MongoEmbeddingWorkEnqueuer {

  /** The configured disabled mode deliberately creates no durable embedding work or backing adapter. */
  val disabled: MongoEmbeddingWorkEnqueuer = new MongoEmbeddingWorkEnqueuer {
    override val requiresTransaction: Boolean = false

    override def enqueue(
        _session: Option[ClientSession[IO]],
        _key: EmbeddingWorkKey,
        _now: Instant
    ): RepositoryF[IO, Unit] = ().asRepository
  }
}

/** Durable, coalesced embedding work. A newer enqueue increments generation so an older lease cannot delete it. */
final class MongoEmbeddingWorkRepository(
    database: MongoDatabase[IO],
    diagnostics: Diagnostics[IO],
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends EmbeddingWorkRepository[IO]
    with MongoEmbeddingWorkEnqueuer {
  override val requiresTransaction: Boolean = true
  private val processing = MongoFilter.eq(MongoFields.State, EmbeddingWorkState.Processing.toString)

  def inspectForAdmin(
      key: EmbeddingWorkKey,
      adminId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      transactions: MongoTransactionRunner
  ): RepositoryF[IO, Option[EmbeddingWorkInspection]] = transactions.run(TransactionOperation.EmbeddingWorkInspect) {
    session =>
      for {
        _ <- authorizeAdmin(adminId, session)
        row <- RepositoryF.lift(
          MongoSessionOperations.findOne(collection, session, MongoFilter.eq(MongoFields.Id, key.value))
        )
        result <- row.traverse(document =>
          MongoF.fromEither(MongoDocumentFields.toRepository(for {
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
  ): RepositoryF[IO, Unit] =
    new MongoSubjectFence(database).requireActiveAndBump(
      session,
      adminId.value.toString,
      List(
        MongoFilter.eq(MongoFields.Role, "Admin"),
        MongoFilter.eq(MongoFields.AdminSingletonKey, "singleton-admin")
      ),
      RepositoryError.AuthorityRevoked
    )

  /** Maintenance capability: the trusted Admin and failed generation are checked in the same transaction. A
    * changed/repaired generation is never overwritten. No provider calls occur here.
    */
  def repairFailed(
      key: EmbeddingWorkKey,
      expectedGeneration: Long,
      adminId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      now: Instant,
      transactions: MongoTransactionRunner
  ): RepositoryF[IO, Boolean] =
    if (expectedGeneration < 1L || expectedGeneration == Long.MaxValue)
      MongoF.raise(RepositoryError.InvalidStoredData)
    else
      transactions.run(TransactionOperation.EmbeddingWorkRepair) { session =>
        for {
          _ <- authorizeAdmin(adminId, session)
          repaired <- RepositoryF.lift(
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
                MongoUpdate.set(MongoFields.AvailableAt, now.toDate),
                MongoUpdate.set(MongoFields.UpdatedAt, now.toDate),
                MongoUpdate.set(MongoFields.Attempts, 0),
                MongoUpdate.inc(MongoFields.Generation, 1L),
                MongoUpdate.unset(MongoFields.Failure),
                MongoUpdate.unset(MongoFields.FinishedAt),
                MongoLeaseQueue.releaseLease
              )
            )
          )
        } yield repaired.getMatchedCount == 1L
      }
  private val collection = Mongo4catsCollections.documents(database, MongoCollections.EmbeddingWork)

  override def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryF[IO, Unit] =
    enqueue(None, key, now)

  /** Persists work in the caller's Mongo transaction. This is deliberately a concrete Mongo capability: the generic
    * work port has no transaction/session concept.
    */
  def enqueue(session: ClientSession[IO], key: EmbeddingWorkKey, now: Instant): RepositoryF[IO, Unit] =
    enqueue(Some(session), key, now)

  override def enqueue(
      session: Option[ClientSession[IO]],
      key: EmbeddingWorkKey,
      now: Instant
  ): RepositoryF[IO, Unit] = {
    val pipeline = List(enqueueStage(now))
    // A state predicate in an upsert filter attempts a duplicate insert for an active lease.
    // Conditional pipeline fields instead inspect state within one identity-only atomic write.
    MongoRepositorySupport
      .transactionGuard(diagnostics, "embeddingWork.enqueue", session) {
        RepositoryF
          .lift(
            MongoSessionOperations.updateOnePipeline(
              collection,
              session,
              MongoFilter.eq(MongoFields.Id, key.value),
              pipeline,
              new UpdateOptions().upsert(true)
            )
          )
          .subflatMap(result =>
            Either.cond(
              result.getMatchedCount + result.getUpserts.size == 1,
              (),
              RepositoryError.Conflict
            )
          )
      }
  }

  private def enqueueStage(now: Instant): BsonDocument = {
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
      leaseDuration: FiniteDuration
  ): RepositoryF[IO, Option[ClaimedEmbeddingWork]] =
    RepositoryF.lift(uuidGen.randomUUID.map(_.toString)).flatMap { token =>
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "embeddingWork.claim") {
          MongoLeaseQueue.claimReady(
            collection,
            List(EmbeddingWorkState.Ready, EmbeddingWorkState.Retry).map(_.toString),
            EmbeddingWorkState.Processing.toString,
            workerId,
            token,
            now,
            leaseDuration,
            Sorts.ascending(MongoFields.AvailableAt, MongoFields.Id),
            // The pre-image tells a takeover of an expired lease (it was Processing) from a fresh claim. The lease stamp
            // changes neither generation nor attempts, and the new token is the one generated here, so nothing else
            // is needed from the post-claim document and no field is stored for this.
            ReturnDocument.BEFORE
          )(document => MongoF.fromEither(readClaim(document, token)))
        }
    }

  override def renew(claim: ClaimedEmbeddingWork, leaseDuration: FiniteDuration): RepositoryF[IO, Boolean] =
    LeaseRequest.whenDurationValid(leaseDuration) {
      MongoRepositorySupport.repositoryGuard(diagnostics, "embeddingWork.renew") {
        MongoLeaseQueue
          .renewHeld(collection, None, MongoFilter.and(leaseFilter(claim), MongoLeaseQueue.leaseHeld), leaseDuration)
      }
    }

  override def complete(claim: ClaimedEmbeddingWork): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "embeddingWork.complete") {
        RepositoryF
          .lift(MongoSessionOperations.deleteOne(collection, None, leaseFilter(claim)))
          .subflatMap(MongoRepositorySupport.deletedOne(_))
      }

  override def retry(
      claim: ClaimedEmbeddingWork,
      availableAt: Instant,
      chargeAttempt: Boolean = true
  ): RepositoryF[IO, Unit] = {
    val updates = List(
      MongoUpdate.set(MongoFields.State, EmbeddingWorkState.Retry.toString),
      MongoUpdate.set(MongoFields.AvailableAt, availableAt.toDate),
      MongoLeaseQueue.releaseLease
    ) ++ Option.when(chargeAttempt)(MongoUpdate.inc(MongoFields.Attempts, java.lang.Integer.valueOf(1))).toList
    transition(claim, MongoUpdate.combine(updates*))
  }

  override def fail(
      claim: ClaimedEmbeddingWork,
      failure: EmbeddingWorkFailure,
      now: Instant
  ): RepositoryF[IO, Unit] =
    transition(
      claim,
      MongoUpdate.combine(
        MongoUpdate.set(MongoFields.State, EmbeddingWorkState.Failed.toString),
        MongoUpdate.set(MongoFields.Failure, failure.toString),
        MongoUpdate.set(MongoFields.FinishedAt, now.toDate),
        MongoLeaseQueue.releaseLease
      )
    )

  private def transition(
      claim: ClaimedEmbeddingWork,
      update: MongoUpdate
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "embeddingWork.transition") {
        RepositoryF
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

  private def readClaim(document: Document, leaseToken: String): Either[RepositoryError, ClaimedEmbeddingWork] =
    MongoDocumentFields.toRepository(
      for {
        id <- MongoDocumentFields.requiredString(document, MongoFields.Id)
        key <- EmbeddingWorkKey
          .fromId(id)
          .toRight(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.Id))
        generation <- MongoDocumentFields.requiredNumberAsLong(document, MongoFields.Generation)
        attempts <- MongoDocumentFields.requiredNumberAsInt(document, MongoFields.Attempts)
        previousState <- MongoDocumentFields.requiredString(document, MongoFields.State)
      } yield ClaimedEmbeddingWork(
        key,
        generation,
        attempts,
        leaseToken,
        reclaimedLease = previousState == EmbeddingWorkState.Processing.toString
      )
    )
}

final case class EmbeddingWorkInspection(
    generation: Long,
    attempts: Int,
    state: EmbeddingWorkState,
    failure: Option[EmbeddingWorkFailure]
)
