package com.example.graphQL.cats.service.mutation

import cats.data.EitherT
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
    EitherT {
      clock.realTimeInstant.flatMap { now =>
        val key = MutationReceiptKey(operation, actorScope, request.idempotencyKey)
        receipts
          .execute(key, request.fingerprint, now, now.plusSeconds(receiptTtl.toSeconds)) { context =>
            RepositoryIO.fromIOEither(
              write(context).value.map {
                case Left(UseCaseError.Repository(error)) => Left(error)
                case Left(error)                          => Right(MutationWriteOutcome.Rejected(error))
                case Right(value)                         =>
                  Right(MutationWriteOutcome.Applied(MutationReceiptWrite(value, entity(value))))
              }
            )
          }
          .value
          .flatMap {
            case Left(error)                                         => IO.pure(Left(UseCaseError.Repository(error)))
            case Right(MutationReceiptExecution.Applied(value, _))   => IO.pure(Right(value))
            case Right(MutationReceiptExecution.Replay(reference))   => replay(reference).value
            case Right(MutationReceiptExecution.Rejected(error))     => IO.pure(Left(error))
            case Right(MutationReceiptExecution.FingerprintMismatch) =>
              IO.pure(Left(UseCaseError.Repository(RepositoryError.Conflict)))
            case Right(MutationReceiptExecution.InProgress) =>
              IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable)))
          }
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
