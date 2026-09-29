package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient, MongoDatabase}
import com.mongodb.client.model.{Filters, Updates}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}

/** The Mongo-only transaction context used by receipt-aware repository writes. */
private[mongo] final case class MongoMutationWriteContext(session: Option[ClientSession]) extends MutationWriteContext

private[mongo] object MongoMutationWriteContext {
  def session(context: MutationWriteContext): IO[Option[ClientSession]] = context match {
    case MongoMutationWriteContext(value) => IO.pure(value)
    case _                                =>
      IO.raiseError(new IllegalArgumentException("Mutation write context belongs to another repository adapter"))
  }

  def run[A](
      context: MutationWriteContext,
      transactionRunner: MongoTransactionRunner,
      transactionRequired: Boolean
  )(operation: Option[ClientSession] => IO[Either[RepositoryError, A]]): IO[Either[RepositoryError, A]] =
    context match {
      case MongoMutationWriteContext(value)            => operation(value)
      case value if value eq MutationWriteContext.noop =>
        if (transactionRequired) transactionRunner.run(operation) else operation(None)
      case _ =>
        IO.raiseError(new IllegalArgumentException("Mutation write context belongs to another repository adapter"))
    }
}

/** Stores only a caller scope, operation, input fingerprint, and authoritative entity reference. It deliberately never
  * stores GraphQL payloads, credentials, or access tokens.
  */
final class MongoMutationReceiptRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction,
    diagnostics: Diagnostics = Diagnostics.noop
) extends MutationReceiptRepository {
  private val collection = database.getCollection("mutation_receipts")

  override def execute[A, E](
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      now: Instant,
      expiresAt: Instant
  )(
      write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
  ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.execute")(transactionRunner.run { session =>
        find(session, key).flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right(None) =>
            insert(session, inProgress(key, fingerprint, now, expiresAt)).flatMap {
              case Left(error) => IO.pure(Left(error))
              case Right(())   =>
                write(MongoMutationWriteContext(session)).flatMap {
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
      .value

  private def find(
      session: Option[ClientSession],
      key: MutationReceiptKey
  ): IO[Either[RepositoryError, Option[MutationReceipt]]] =
    session
      .fold(
        PublisherBridge.first(collection.find(keyFilter(key)))
      )(active => PublisherBridge.first(collection.find(active, keyFilter(key))))
      .map(_.traverse(read))

  private def insert(session: Option[ClientSession], receipt: Document): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.insert")(
        session
          .fold(
            PublisherBridge.first(collection.insertOne(receipt))
          )(active => PublisherBridge.first(collection.insertOne(active, receipt)))
          .map(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.MissingWriteResult))(_ => Right(())))
      )(mapWrite)
      .value

  private def complete(
      session: Option[ClientSession],
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      entity: MutationEntityReference,
      now: Instant,
      expiresAt: Instant
  ): IO[Either[RepositoryError, Unit]] =
    val filter = Filters.and(
      keyFilter(key),
      Filters.eq("fingerprint", fingerprint.value),
      Filters.eq("state", MutationReceiptState.InProgress.toString)
    )
    val update = Updates.combine(
      Updates.set("state", MutationReceiptState.Completed.toString),
      Updates.set("entity", entityDocument(entity)),
      Updates.set("completedAt", Date.from(now)),
      Updates.set("expiresAt", Date.from(expiresAt))
    )
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.complete")(
        session
          .fold(
            PublisherBridge.first(collection.updateOne(filter, update))
          )(active => PublisherBridge.first(collection.updateOne(active, filter, update)))
          .map {
            case Some(result) if result.getMatchedCount == 1L => Right(())
            case Some(_)                                      => Left(RepositoryError.Conflict)
            case None                                         => Left(RepositoryError.MissingWriteResult)
          }
      )(mapWrite)
      .value

  private def remove(session: Option[ClientSession], key: MutationReceiptKey): IO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "mutationReceipt.remove")(
        session
          .fold(
            PublisherBridge.first(collection.deleteOne(keyFilter(key)))
          )(active => PublisherBridge.first(collection.deleteOne(active, keyFilter(key))))
          .void
          .map(_ => Right(()): Either[RepositoryError, Unit])
      )(_ => Right(()))
      .value
      .void

  private def keyFilter(key: MutationReceiptKey): Bson =
    Filters.and(
      Filters.eq("operation", key.operation),
      Filters.eq("actorScope", key.actorScope),
      Filters.eq("idempotencyKey", key.idempotencyKey.toString)
    )

  private def inProgress(
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      now: Instant,
      expiresAt: Instant
  ): Document =
    new Document("_id", UUID.randomUUID().toString)
      .append("operation", key.operation)
      .append("actorScope", key.actorScope)
      .append("idempotencyKey", key.idempotencyKey.toString)
      .append("fingerprint", fingerprint.value)
      .append("state", MutationReceiptState.InProgress.toString)
      .append("createdAt", Date.from(now))
      .append("expiresAt", Date.from(expiresAt))

  private def entityDocument(entity: MutationEntityReference): Document =
    new Document("type", entity.entityType).append("id", entity.entityId)

  private def read(document: Document): Either[RepositoryError, MutationReceipt] =
    for {
      operation <- Option(document.getString("operation")).toRight(RepositoryError.InvalidStoredData)
      actorScope <- Option(document.getString("actorScope")).toRight(RepositoryError.InvalidStoredData)
      idempotencyKey <- Option(document.getString("idempotencyKey"))
        .flatMap(parseUuid)
        .toRight(RepositoryError.InvalidStoredData)
      fingerprint <- Option(document.getString("fingerprint")).toRight(RepositoryError.InvalidStoredData)
      state <- Option(document.getString("state")).flatMap(parseState).toRight(RepositoryError.InvalidStoredData)
      createdAt <- Option(document.getDate("createdAt")).map(_.toInstant).toRight(RepositoryError.InvalidStoredData)
      expiresAt <- Option(document.getDate("expiresAt")).map(_.toInstant).toRight(RepositoryError.InvalidStoredData)
    } yield MutationReceipt(
      MutationReceiptKey(operation, actorScope, idempotencyKey),
      MutationReceiptFingerprint.stored(fingerprint),
      state,
      Option(document.get("entity", classOf[Document])).flatMap(readEntity),
      createdAt,
      Option(document.getDate("completedAt")).map(_.toInstant),
      expiresAt
    )

  private def readEntity(document: Document): Option[MutationEntityReference] =
    for {
      entityType <- Option(document.getString("type"))
      entityId <- Option(document.getString("id"))
    } yield MutationEntityReference(entityType, entityId)

  private def parseUuid(value: String): Option[UUID] = scala.util.Try(UUID.fromString(value)).toOption

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
      database: MongoDatabase,
      client: MongoClient,
      diagnostics: Diagnostics = Diagnostics.noop
  ): MongoMutationReceiptRepository =
    new MongoMutationReceiptRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}
