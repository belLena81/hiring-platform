package com.example.graphQL.cats.service.mutation

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}

import java.time.Instant
import java.util.Locale
import scala.concurrent.duration.*

final class Idempotent private (
    receipts: MutationReceiptRepository,
    currentTime: IO[Instant],
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
      currentTime.flatMap { now =>
        val key = MutationReceiptKey(operation, actorScope, request.idempotencyKey)
        receipts
          .execute(key, request.fingerprint, now, now.plusSeconds(receiptTtl.toSeconds)) { context =>
            write(context).value.map {
              case Left(UseCaseError.Repository(error)) => Left(error)
              case Left(error)                          => Right(Left(error))
              case Right(value)                         => Right(Right(MutationReceiptWrite(value, entity(value))))
            }
          }
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
    new Idempotent(receipts, IO.realTimeInstant, ReceiptTtl)

  private[service] def withClock(
      receipts: MutationReceiptRepository,
      currentTime: IO[Instant]
  ): Idempotent =
    new Idempotent(receipts, currentTime, ReceiptTtl)

  val noop: Idempotent = apply(MutationReceiptRepository.noop)

  def actorScope(actor: ActorContext): String = actor.userId.value.toString

  def publicActorScope(name: String): String =
    s"public:${name.trim.toLowerCase(Locale.ROOT)}"
}
