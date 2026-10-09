package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.Diagnostics
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant

/** The Mongo-only transaction context used by receipt-aware repository writes. */
private[mongo] final case class MongoMutationWriteContext(session: Option[ClientSession[IO]])
    extends MutationWriteContext

private[mongo] object MongoMutationWriteContext {
  def session(context: MutationWriteContext): IO[Option[ClientSession[IO]]] = context match {
    case MongoMutationWriteContext(value) => IO.pure(value)
    case _                                =>
      IO.raiseError(MongoSetupError("Mutation write context belongs to another repository adapter"))
  }

  def run[A](
      context: MutationWriteContext,
      transactionRunner: MongoTransactionRunner,
      transactionRequired: Boolean
  )(operation: Option[ClientSession[IO]] => RepositoryIO[A]): RepositoryIO[A] =
    context match {
      case MongoMutationWriteContext(value)                   => operation(value)
      case value if value eq MutationWriteContext.directWrite =>
        if (transactionRequired) transactionRunner.run(operation) else operation(None)
      case _ =>
        RepositoryIO.lift(
          IO.raiseError(MongoSetupError("Mutation write context belongs to another repository adapter"))
        )
    }
}

/** Stores only a caller scope, operation, input fingerprint, and authoritative entity reference. It deliberately never
  * stores GraphQL payloads, credentials, or access tokens.
  */
final class MongoMutationReceiptRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics,
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends MutationReceiptRepository {
  private val collection = Mongo4catsCollections.documents(database, MongoCollections.MutationReceipts)

  override def execute[A, E](
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      now: Instant,
      expiresAt: Instant
  )(
      write: MutationWriteContext => RepositoryIO[MutationWriteOutcome[A, E]]
  ): RepositoryIO[MutationReceiptExecution[A, E]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.execute")(transactionRunner.run { session =>
        find(session, key).flatMap {
          case None =>
            for {
              pending <- RepositoryIO.lift(inProgress(key, fingerprint, now, expiresAt))
              _ <- insert(session, pending)
              outcome <- write(MongoMutationWriteContext(session)).recoverWith { error =>
                RepositoryIO.lift(remove(session, key)).flatMap(_ => RepositoryIO.fromEither(Left(error)))
              }
              result <- outcome match {
                case MutationWriteOutcome.Rejected(error) =>
                  RepositoryIO.lift(remove(session, key)).as(MutationReceiptExecution.Rejected(error))
                case MutationWriteOutcome.Applied(completed) =>
                  complete(session, key, fingerprint, completed.entity, now, expiresAt)
                    .as(MutationReceiptExecution.Applied(completed.value, completed.entity))
              }
            } yield result
          case Some(receipt) if receipt.fingerprint != fingerprint =>
            RepositoryIO.fromEither(Right(MutationReceiptExecution.FingerprintMismatch))
          case Some(receipt) if receipt.state == MutationReceiptState.InProgress =>
            RepositoryIO.fromEither(Right(MutationReceiptExecution.InProgress))
          case Some(receipt) =>
            // A completed receipt without its rehydration key is corrupt; do not repeat the write.
            RepositoryIO.fromEither(
              receipt.entity
                .toRight(RepositoryError.InvalidStoredData)
                .map(MutationReceiptExecution.Replay.apply)
            )
        }
        /*
         * The preceding branches keep receipt insertion, the caller write, and completion in the same
         * transaction. A missing stored receipt is the only path that can start the write.
         */
      })(MongoErrors.duplicateAsConflict)

  private def find(
      session: Option[ClientSession[IO]],
      key: MutationReceiptKey
  ): RepositoryIO[Option[MutationReceipt]] =
    RepositoryIO
      .lift(MongoSessionOperations.findOne(collection, session, keyFilter(key)))
      .subflatMap(_.traverse(read))

  private def insert(session: Option[ClientSession[IO]], receipt: Document): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.insert")(
        RepositoryIO
          .lift(MongoSessionOperations.insertOne(collection, session, receipt))
          .void
      )(MongoErrors.duplicateAsConflict)

  private def complete(
      session: Option[ClientSession[IO]],
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      entity: MutationEntityReference,
      now: Instant,
      expiresAt: Instant
  ): RepositoryIO[Unit] =
    val filter = MongoFilter.and(
      keyFilter(key),
      MongoFilter.eq(MongoFields.Fingerprint, fingerprint.value),
      MongoFilter.eq(MongoFields.State, MutationReceiptState.InProgress.toString)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.State, MutationReceiptState.Completed.toString),
      MongoUpdate.set(MongoFields.Entity, entityDocument(entity)),
      MongoUpdate.set(MongoFields.CompletedAt, now.toDate),
      MongoUpdate.set(MongoFields.ExpiresAt, expiresAt.toDate)
    )
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.complete")(
        RepositoryIO
          .lift(MongoSessionOperations.updateOne(collection, session, filter, update))
          .subflatMap(MongoRepositorySupport.matchedOne(_))
      )(MongoErrors.duplicateAsConflict)

  private def remove(session: Option[ClientSession[IO]], key: MutationReceiptKey): IO[Unit] =
    MongoRepositorySupport.guard(diagnostics, "mutationReceipt.remove")(
      MongoSessionOperations.deleteOne(collection, session, keyFilter(key)).void
    )(_ => ())

  private def keyFilter(key: MutationReceiptKey): MongoFilter =
    MongoFilter.and(
      MongoFilter.eq(MongoFields.Operation, key.operation),
      MongoFilter.eq(MongoFields.ActorScope, key.actorScope),
      MongoFilter.eq(MongoFields.IdempotencyKey, key.idempotencyKey.toString)
    )

  private def inProgress(
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      now: Instant,
      expiresAt: Instant
  ): IO[Document] =
    uuidGen.randomUUID.map { id =>
      new Document(MongoFields.Id, id.toString)
        .append(MongoFields.Operation, key.operation)
        .append(MongoFields.ActorScope, key.actorScope)
        .append(MongoFields.IdempotencyKey, key.idempotencyKey.toString)
        .append(MongoFields.Fingerprint, fingerprint.value)
        .append(MongoFields.State, MutationReceiptState.InProgress.toString)
        .append(MongoFields.CreatedAt, now.toDate)
        .append(MongoFields.ExpiresAt, expiresAt.toDate)
    }

  private def entityDocument(entity: MutationEntityReference): Document =
    new Document(MongoFields.Type, entity.entityType).append(MongoFields.EntityId, entity.entityId)

  private def read(document: Document): Either[RepositoryError, MutationReceipt] =
    MongoDocumentFields.toRepository(
      for {
        operation <- MongoDocumentFields.requiredString(document, MongoFields.Operation)
        actorScope <- MongoDocumentFields.requiredString(document, MongoFields.ActorScope)
        idempotencyKey <- MongoDocumentFields.requiredUuid(document, MongoFields.IdempotencyKey)
        fingerprint <- MongoDocumentFields.requiredString(document, MongoFields.Fingerprint)
        state <- MongoDocumentFields.requiredEnum(document, MongoFields.State)(
          MongoDocumentFields.byName(MutationReceiptState.values)
        )
        createdAt <- MongoDocumentFields.requiredInstant(document, MongoFields.CreatedAt)
        expiresAt <- MongoDocumentFields.requiredInstant(document, MongoFields.ExpiresAt)
        completedAt <- MongoDocumentFields.optionalInstant(document, MongoFields.CompletedAt)
        entity <- MongoDocumentFields.optionalDocument(document, MongoFields.Entity).flatMap(_.flatTraverse(readEntity))
      } yield MutationReceipt(
        MutationReceiptKey(operation, actorScope, idempotencyKey),
        MutationReceiptFingerprint.stored(fingerprint),
        state,
        entity,
        createdAt,
        completedAt,
        expiresAt
      )
    )

  /** An entity reference is present only when both parts are; a partial reference reads as absent. */
  private def readEntity(document: Document): MongoDocumentFields.Read[Option[MutationEntityReference]] =
    (
      MongoDocumentFields.optionalString(document, MongoFields.Type),
      MongoDocumentFields.optionalString(document, MongoFields.EntityId)
    ).mapN((entityType, entityId) => (entityType, entityId).mapN(MutationEntityReference.apply))
}

object MongoMutationReceiptRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics,
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): MongoMutationReceiptRepository =
    new MongoMutationReceiptRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics,
      uuidGen
    )
}
