package com.example.graphQL.cats.service.mutation

import cats.data.EitherT
import cats.effect.{Clock, IO}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}

import com.example.graphQL.cats.domain.model.Identifiers
import java.util.{Locale, UUID}
import scala.concurrent.duration.*

final class Idempotent private (receipts: MutationReceiptRepository, clock: Clock[IO]) {

  /** [[execute]] scoped to the authenticated actor. */
  def executeFor[A](
      actor: ActorContext,
      operation: String,
      request: IdempotencyRequest,
      entity: A => MutationEntityReference,
      replay: MutationEntityReference => UseCaseIO[A]
  )(write: MutationWriteContext => UseCaseIO[A]): UseCaseIO[A] =
    execute(operation, Idempotent.actorScope(actor), request, entity, replay)(write)

  def execute[A](
      operation: String,
      actorScope: String,
      request: IdempotencyRequest,
      entity: A => MutationEntityReference,
      replay: MutationEntityReference => UseCaseIO[A]
  )(write: MutationWriteContext => UseCaseIO[A]): UseCaseIO[A] =
    EitherT.liftF(clock.realTimeInstant).flatMap { now =>
      val key = MutationReceiptKey(operation, actorScope, request.idempotencyKey)
      UseCaseIO
        .repository(
          receipts.execute(key, request.fingerprint, now, now.plusSeconds(Idempotent.ReceiptTtl.toSeconds)) { context =>
            write(context).biflatMap(
              {
                case UseCaseError.Repository(error) => RepositoryIO.fromEither(Left(error))
                case error => RepositoryIO.fromEither(Right(MutationWriteOutcome.Rejected(error)))
              },
              value =>
                RepositoryIO.fromEither(Right(MutationWriteOutcome.Applied(MutationReceiptWrite(value, entity(value)))))
            )
          }
        )
        .flatMap {
          case MutationReceiptExecution.Applied(value, _)   => EitherT.rightT(value)
          case MutationReceiptExecution.Replay(reference)   => replay(reference)
          case MutationReceiptExecution.Rejected(error)     => EitherT.leftT(error)
          case MutationReceiptExecution.FingerprintMismatch =>
            EitherT.leftT(UseCaseError.Repository(RepositoryError.Conflict))
          case MutationReceiptExecution.InProgress =>
            EitherT.leftT(UseCaseError.Repository(RepositoryError.Unavailable))
        }
    }
}

object Idempotent {
  private[mutation] val ReceiptTtl = 7.days

  def apply(receipts: MutationReceiptRepository, clock: Clock[IO] = Clock[IO]): Idempotent =
    new Idempotent(receipts, clock)

  /** A stored reference that no replay can resolve is a repository fault, never a client error. */
  def corruptReference[A]: UseCaseIO[A] = EitherT.leftT(UseCaseError.Repository(RepositoryError.Unavailable))

  /** Replays a receipt that references an entity of `kind` by identifier. */
  def replayById[Id, A](kind: String, wrap: UUID => Id)(load: Id => UseCaseIO[A])(
      reference: MutationEntityReference
  ): UseCaseIO[A] =
    Option
      .when(reference.entityType == kind)(reference.entityId)
      .flatMap(Identifiers.parse(_)(wrap))
      .fold(corruptReference)(load)

  def actorScope(actor: ActorContext): String = actor.userId.value.toString

  def publicActorScope(name: String): String =
    s"public:${name.trim.toLowerCase(Locale.ROOT)}"
}
