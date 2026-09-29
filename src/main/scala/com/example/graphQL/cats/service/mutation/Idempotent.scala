package com.example.graphQL.cats.service.mutation

import cats.effect.{Clock, IO}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}

import java.util.Locale
import scala.concurrent.duration.*

final class Idempotent private (
    receipts: MutationReceiptRepository,
    clock: Clock[IO],
    receiptTtl: FiniteDuration
) {
  def execute[A](
      operation: String,
      actorScope: String,
      request: IdempotencyRequest,
      entity: A => MutationEntityReference,
      replay: MutationEntityReference => UseCaseIO[A]
  )(write: MutationWriteContext => UseCaseIO[A]): UseCaseIO[A] =
    UseCaseIO.liftIO(clock.realTimeInstant).flatMap { now =>
      val key = MutationReceiptKey(operation, actorScope, request.idempotencyKey)
      UseCaseIO
        .repository(
          receipts.execute(key, request.fingerprint, now, now.plusSeconds(receiptTtl.toSeconds)) { context =>
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
          case MutationReceiptExecution.Applied(value, _)   => UseCaseIO.pure(value)
          case MutationReceiptExecution.Replay(reference)   => replay(reference)
          case MutationReceiptExecution.Rejected(error)     => UseCaseIO.left(error)
          case MutationReceiptExecution.FingerprintMismatch =>
            UseCaseIO.left(UseCaseError.Repository(RepositoryError.Conflict))
          case MutationReceiptExecution.InProgress =>
            UseCaseIO.left(UseCaseError.Repository(RepositoryError.Unavailable))
        }
    }
}

object Idempotent {
  private val ReceiptTtl = 7.days

  def apply(receipts: MutationReceiptRepository): Idempotent =
    new Idempotent(receipts, Clock[IO], ReceiptTtl)

  private[service] def withClock(
      receipts: MutationReceiptRepository,
      clock: Clock[IO]
  ): Idempotent =
    new Idempotent(receipts, clock, ReceiptTtl)

  def actorScope(actor: ActorContext): String = actor.userId.value.toString

  def publicActorScope(name: String): String =
    s"public:${name.trim.toLowerCase(Locale.ROOT)}"
}
