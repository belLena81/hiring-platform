package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.shared.Parsing
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}

/** The Mongo-only transaction context used by receipt-aware repository writes. */
private[mongo] final case class MongoMutationWriteContext(session: Option[ClientSession[IO]])
    extends MutationWriteContext

private[mongo] object MongoMutationWriteContext {
  def session(context: MutationWriteContext): IO[Option[ClientSession[IO]]] = context match {
    case MongoMutationWriteContext(value) => IO.pure(value)
    case _                                =>
      IO.raiseError(new IllegalArgumentException("Mutation write context belongs to another repository adapter"))
  }

  def run[A](
      context: MutationWriteContext,
      transactionRunner: MongoTransactionRunner,
      transactionRequired: Boolean
  )(operation: Option[ClientSession[IO]] => IO[Either[RepositoryError, A]]): IO[Either[RepositoryError, A]] =
    context match {
      case MongoMutationWriteContext(value)                   => operation(value)
      case value if value eq MutationWriteContext.directWrite =>
        if (transactionRequired) transactionRunner.run(operation) else operation(None)
      case _ =>
        IO.raiseError(new IllegalArgumentException("Mutation write context belongs to another repository adapter"))
    }
}

/** Stores only a caller scope, operation, input fingerprint, and authoritative entity reference. It deliberately never
  * stores GraphQL payloads, credentials, or access tokens.
  */
final class MongoMutationReceiptRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics = Diagnostics.noop,
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
          case Left(error) => IO.pure(Left(error))
          case Right(None) =>
            inProgress(key, fingerprint, now, expiresAt).flatMap(insert(session, _)).flatMap {
              case Left(error) => IO.pure(Left(error))
              case Right(())   =>
                write(MongoMutationWriteContext(session)).value.flatMap {
                  case Left(error)                                 => remove(session, key).as(Left(error))
                  case Right(MutationWriteOutcome.Rejected(error)) =>
                    remove(session, key).as(Right(MutationReceiptExecution.Rejected(error)))
                  case Right(MutationWriteOutcome.Applied(completed)) =>
                    complete(session, key, fingerprint, completed.entity, now, expiresAt).map(_.map { _ =>
                      MutationReceiptExecution.Applied(completed.value, completed.entity)
                    })
                }
            }
          case Right(Some(receipt)) if receipt.fingerprint != fingerprint =>
            IO.pure(Right(MutationReceiptExecution.FingerprintMismatch))
          case Right(Some(receipt)) if receipt.state == MutationReceiptState.InProgress =>
            IO.pure(Right(MutationReceiptExecution.InProgress))
          case Right(Some(receipt)) =>
            receipt.entity match {
              case Some(entity) => IO.pure(Right(MutationReceiptExecution.Replay(entity)))
              // A completed receipt without its rehydration key is corrupt; do not repeat the write.
              case None => IO.pure(Left(RepositoryError.InvalidStoredData))
            }
        }
        /*
         * The preceding branches keep receipt insertion, the caller write, and completion in the same
         * transaction. A missing stored receipt is the only path that can start the write.
         */
      })(mapWrite)


  private def find(
      session: Option[ClientSession[IO]],
      key: MutationReceiptKey
  ): IO[Either[RepositoryError, Option[MutationReceipt]]] =
    MongoSessionOperations
      .findOne(collection, session, keyFilter(key))
      .map(_.traverse(read))

  private def insert(session: Option[ClientSession[IO]], receipt: Document): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.insert")(
        MongoSessionOperations
          .insertOne(collection, session, receipt)
          .map(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ => Right(())))
      )(mapWrite)
      .value

  private def complete(
      session: Option[ClientSession[IO]],
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      entity: MutationEntityReference,
      now: Instant,
      expiresAt: Instant
  ): IO[Either[RepositoryError, Unit]] =
    val filter = MongoFilter.and(
      keyFilter(key),
      MongoFilter.eq(MongoFields.Fingerprint, fingerprint.value),
      MongoFilter.eq(MongoFields.State, MutationReceiptState.InProgress.toString)
    )
    val update = MongoUpdate.combine(
      MongoUpdate.set(MongoFields.State, MutationReceiptState.Completed.toString),
      MongoUpdate.set(MongoFields.Entity, entityDocument(entity)),
      MongoUpdate.set(MongoFields.CompletedAt, Date.from(now)),
      MongoUpdate.set(MongoFields.ExpiresAt, Date.from(expiresAt))
    )
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.complete")(
        MongoSessionOperations
          .updateOne(collection, session, filter, update)
          .map {
            case Some(result) if result.getMatchedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      )(mapWrite)
      .value

  private def remove(session: Option[ClientSession[IO]], key: MutationReceiptKey): IO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.remove")(
        collection
          .flatMap(c =>
            session.fold(
              c.deleteOne(keyFilter(key).bson, new com.mongodb.client.model.DeleteOptions)
            )(active => c.deleteOne(active, keyFilter(key).sessionFilter, new com.mongodb.client.model.DeleteOptions))
          )
          .void
          .map(_ => Right(()): Either[RepositoryError, Unit])
      )(_ => Right(()))
      .value
      .void

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
        .append(MongoFields.CreatedAt, Date.from(now))
        .append(MongoFields.ExpiresAt, Date.from(expiresAt))
    }

  private def entityDocument(entity: MutationEntityReference): Document =
    new Document(MongoFields.Type, entity.entityType).append(MongoFields.EntityId, entity.entityId)

  private def read(document: Document): Either[RepositoryError, MutationReceipt] =
    for {
      operation <- Option(document.getString(MongoFields.Operation)).toRight(RepositoryError.InvalidStoredData)
      actorScope <- Option(document.getString(MongoFields.ActorScope)).toRight(RepositoryError.InvalidStoredData)
      idempotencyKey <- Option(document.getString(MongoFields.IdempotencyKey))
        .flatMap(parseUuid)
        .toRight(RepositoryError.InvalidStoredData)
      fingerprint <- Option(document.getString(MongoFields.Fingerprint)).toRight(RepositoryError.InvalidStoredData)
      state <- Option(document.getString(MongoFields.State))
        .flatMap(parseState)
        .toRight(RepositoryError.InvalidStoredData)
      createdAt <- Option(document.getDate(MongoFields.CreatedAt))
        .map(_.toInstant)
        .toRight(RepositoryError.InvalidStoredData)
      expiresAt <- Option(document.getDate(MongoFields.ExpiresAt))
        .map(_.toInstant)
        .toRight(RepositoryError.InvalidStoredData)
    } yield MutationReceipt(
      MutationReceiptKey(operation, actorScope, idempotencyKey),
      MutationReceiptFingerprint.stored(fingerprint),
      state,
      Option(document.get(MongoFields.Entity, classOf[Document])).flatMap(readEntity),
      createdAt,
      Option(document.getDate(MongoFields.CompletedAt)).map(_.toInstant),
      expiresAt
    )

  private def readEntity(document: Document): Option[MutationEntityReference] =
    for {
      entityType <- Option(document.getString(MongoFields.Type))
      entityId <- Option(document.getString(MongoFields.EntityId))
    } yield MutationEntityReference(entityType, entityId)

  private def parseUuid(value: String): Option[UUID] = Parsing.parseUuid(value).toOption

  private def parseState(value: String): Option[MutationReceiptState] =
    MutationReceiptState.values.find(_.toString == value)

  private def mapWrite[A](error: Throwable): Either[RepositoryError, A] =
    error match {
      case write: com.mongodb.MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
      case _ => Left(RepositoryError.Unavailable)
    }
}

object MongoMutationReceiptRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics = Diagnostics.noop,
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): MongoMutationReceiptRepository =
    new MongoMutationReceiptRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics,
      uuidGen
    )
}
