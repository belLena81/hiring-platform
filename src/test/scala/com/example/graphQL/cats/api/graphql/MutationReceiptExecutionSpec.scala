package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.repository.protocol.*
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

final class MutationReceiptExecutionSpec extends CatsEffectSuite {
  private final class RecordingReceiptRepository(
      state: Ref[IO, Option[MutationEntityReference]]
  ) extends MutationReceiptRepository {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(write: MutationWriteContext => IO[Either[RepositoryError, Either[E, MutationReceiptWrite[A]]]]):
        IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] =
      val _ = (key, fingerprint, now, expiresAt)
      state.get.flatMap {
        case Some(entity) => IO.pure(Right(MutationReceiptExecution.Replay(entity)))
        case None => write(MutationWriteContext.noop).flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right(Left(error)) => IO.pure(Right(MutationReceiptExecution.Rejected(error)))
          case Right(Right(value)) => state.set(Some(value.entity)).as(Right(MutationReceiptExecution.Applied(value.value, value.entity)))
        }
      }
  }

  test("same idempotency key replays the stored entity without rerunning the write") {
    for {
      state <- Ref.of[IO, Option[MutationEntityReference]](None)
      writes <- Ref.of[IO, Int](0)
      services = TestGraphQLSupport.emptyServices.copy(mutationReceipts = new RecordingReceiptRepository(state))
      key = UUID.fromString("00000000-0000-0000-0000-000000000001")
      first <- HiringGraphQLResolverSupport.executeMutation[String](
        services,
        "createJob",
        "actor-1",
        key,
        Json.fromString("same-input"),
        value => MutationEntityReference("job", value),
        reference => IO.pure(Right(s"replayed:${reference.entityId}"))
      ) { _ => writes.update(_ + 1).as(Right("job-1")) }
      second <- HiringGraphQLResolverSupport.executeMutation[String](
        services,
        "createJob",
        "actor-1",
        key,
        Json.fromString("same-input"),
        value => MutationEntityReference("job", value),
        reference => IO.pure(Right(s"replayed:${reference.entityId}"))
      ) { _ => writes.update(_ + 1).as(Right("job-2")) }
      count <- writes.get
    } yield {
      assertEquals(first, Right("job-1"))
      assertEquals(second, Right("replayed:job-1"))
      assertEquals(count, 1)
    }
  }
}
