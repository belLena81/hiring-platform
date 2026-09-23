package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.repository.protocol.*
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
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction
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
    transactionRunner.run { session =>
      find(session, key).flatMap {
        case Some(receipt) if receipt.fingerprint != fingerprint =>
          IO.pure(Right(MutationReceiptExecution.FingerprintMismatch))
        case Some(receipt) if receipt.state == MutationReceiptState.InProgress =>
          IO.pure(Right(MutationReceiptExecution.InProgress))
        case Some(receipt) =>
          receipt.entity match {
            case Some(entity) => IO.pure(Right(MutationReceiptExecution.Replay(entity)))
            // A completed receipt without its rehydration key is corrupt; do not repeat the write.
            case None => IO.pure(Left(RepositoryError.Unavailable))
          }
        case None =>
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
      }
    }

  private def find(session: Option[ClientSession], key: MutationReceiptKey): IO[Option[MutationReceipt]] =
    session
      .fold(
        PublisherBridge.first(collection.find(keyFilter(key)))
      )(active => PublisherBridge.first(collection.find(active, keyFilter(key))))
      .map(_.flatMap(read))

  private def insert(session: Option[ClientSession], receipt: Document): IO[Either[RepositoryError, Unit]] =
    session
      .fold(
        PublisherBridge.first(collection.insertOne(receipt))
      )(active => PublisherBridge.first(collection.insertOne(active, receipt)))
      .as(Right(()))
      .handleError(mapWrite)

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
    session
      .fold(
        PublisherBridge.first(collection.updateOne(filter, update))
      )(active => PublisherBridge.first(collection.updateOne(active, filter, update)))
      .map {
        case Some(result) if result.getMatchedCount == 1L => Right(())
        case Some(_)                                      => Left(RepositoryError.Conflict)
        case None                                         => Left(RepositoryError.Unavailable)
      }
      .handleError(mapWrite)

  private def remove(session: Option[ClientSession], key: MutationReceiptKey): IO[Unit] =
    session
      .fold(
        PublisherBridge.first(collection.deleteOne(keyFilter(key)))
      )(active => PublisherBridge.first(collection.deleteOne(active, keyFilter(key))))
      .void
      .handleError(_ => ())

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

  private def read(document: Document): Option[MutationReceipt] =
    for {
      operation <- Option(document.getString("operation"))
      actorScope <- Option(document.getString("actorScope"))
      idempotencyKey <- Option(document.getString("idempotencyKey")).flatMap(parseUuid)
      fingerprint <- Option(document.getString("fingerprint"))
      state <- Option(document.getString("state")).flatMap(parseState)
      createdAt <- Option(document.getDate("createdAt")).map(_.toInstant)
      expiresAt <- Option(document.getDate("expiresAt")).map(_.toInstant)
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
  def transactional(database: MongoDatabase, client: MongoClient): MongoMutationReceiptRepository =
    new MongoMutationReceiptRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict))
}
