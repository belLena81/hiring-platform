package com.example.graphQL.cats.service.mutation

import cats.Monad
import cats.data.EitherT
import cats.effect.Clock
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseF}

import com.example.graphQL.cats.domain.model.Identifiers
import java.util.{Locale, UUID}
import scala.concurrent.duration.*

final class Idempotent[F[_]: Monad] private (receipts: MutationReceiptRepository[F], clock: Clock[F]) {

  /** [[execute]] scoped to the authenticated actor. */
  def executeFor[A](
      actor: ActorContext,
      operation: String,
      request: IdempotencyRequest,
      entity: A => MutationEntityReference,
      replay: MutationEntityReference => UseCaseF[F, A]
  )(write: MutationWriteContext => UseCaseF[F, A]): UseCaseF[F, A] =
    execute(operation, Idempotent.actorScope(actor), request, entity, replay)(write)

  def execute[A](
      operation: String,
      actorScope: String,
      request: IdempotencyRequest,
      entity: A => MutationEntityReference,
      replay: MutationEntityReference => UseCaseF[F, A]
  )(write: MutationWriteContext => UseCaseF[F, A]): UseCaseF[F, A] =
    EitherT.liftF[F, UseCaseError, java.time.Instant](clock.realTimeInstant).flatMap { now =>
      val key = MutationReceiptKey(operation, actorScope, request.idempotencyKey)
      UseCaseF
        .repository(
          receipts.execute(key, request.fingerprint, now, now.plusSeconds(Idempotent.ReceiptTtl.toSeconds)) { context =>
            write(context).biflatMap(
              {
                case UseCaseError.Repository(error) =>
                  RepositoryF.raise[F, MutationWriteOutcome[A, UseCaseError]](error)
                case error =>
                  RepositoryF.pure[F, MutationWriteOutcome[A, UseCaseError]](MutationWriteOutcome.Rejected(error))
              },
              value =>
                RepositoryF.pure[F, MutationWriteOutcome[A, UseCaseError]](
                  MutationWriteOutcome.Applied(MutationReceiptWrite(value, entity(value)))
                )
            )
          }
        )
        .flatMap {
          case MutationReceiptExecution.Applied(value, _)   => EitherT.rightT[F, UseCaseError](value)
          case MutationReceiptExecution.Replay(reference)   => replay(reference)
          case MutationReceiptExecution.Rejected(error)     => EitherT.leftT[F, A](error)
          case MutationReceiptExecution.FingerprintMismatch =>
            EitherT.leftT[F, A](UseCaseError.Repository(RepositoryError.Conflict))
          case MutationReceiptExecution.InProgress =>
            EitherT.leftT[F, A](UseCaseError.Repository(RepositoryError.Unavailable))
        }
    }
}

object Idempotent {
  private[mutation] val ReceiptTtl = 7.days

  def apply[F[_]: Monad](receipts: MutationReceiptRepository[F], clock: Clock[F]): Idempotent[F] =
    new Idempotent(receipts, clock)

  def apply[F[_]: Monad: Clock](receipts: MutationReceiptRepository[F]): Idempotent[F] =
    new Idempotent(receipts, Clock[F])

  /** A stored reference that no replay can resolve is a repository fault, never a client error. */
  def corruptReference[F[_]: Monad, A]: UseCaseF[F, A] =
    EitherT.leftT[F, A](UseCaseError.Repository(RepositoryError.Unavailable))

  /** Replays a receipt that references an entity of `kind` by identifier. */
  def replayById[F[_]: Monad, Id, A](kind: String, wrap: UUID => Id)(load: Id => UseCaseF[F, A])(
      reference: MutationEntityReference
  ): UseCaseF[F, A] =
    Option
      .when(reference.entityType == kind)(reference.entityId)
      .flatMap(Identifiers.parse(_)(wrap))
      .fold(corruptReference[F, A])(load)

  def actorScope(actor: ActorContext): String = actor.userId.value.toString

  def publicActorScope(name: String): String =
    s"public:${name.trim.toLowerCase(Locale.ROOT)}"
}
