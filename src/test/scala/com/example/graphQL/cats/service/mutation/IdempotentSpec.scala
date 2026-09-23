package com.example.graphQL.cats.service.mutation

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

final class IdempotentSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-22T12:00:00Z")
  private val request = IdempotencyRequest.fromCanonicalInput(
    UUID.fromString("00000000-0000-0000-0000-000000000001"),
    "same-input"
  )

  test("replays the stored entity without evaluating the write twice") {
    for {
      state <- Ref.of[IO, Option[MutationEntityReference]](None)
      writes <- Ref.of[IO, Int](0)
      idempotent = Idempotent.withClock(new RecordingReceiptRepository(state), IO.pure(now))
      run = idempotent.execute[String](
        "createJob",
        "actor-1",
        request,
        value => MutationEntityReference("job", value),
        reference => UseCaseIO.pure(s"replayed:${reference.entityId}")
      )(_ => UseCaseIO.liftIO(writes.updateAndGet(_ + 1).map(index => s"job-$index")))
      first <- run.value
      second <- run.value
      count <- writes.get
    } yield {
      assertEquals(first, Right("job-1"))
      assertEquals(second, Right("replayed:job-1"))
      assertEquals(count, 1)
    }
  }

  test("passes the repository transaction context to the write") {
    val expected = new MutationWriteContext {}
    val receipts = new MutationReceiptRepository {
      override def execute[A, E](
          key: MutationReceiptKey,
          fingerprint: MutationReceiptFingerprint,
          currentTime: Instant,
          expiresAt: Instant
      )(
          write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
      ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] = {
        val _ = (key, fingerprint, currentTime, expiresAt)
        write(expected).map(
          _.map {
            case MutationWriteOutcome.Rejected(error) => MutationReceiptExecution.Rejected(error)
            case MutationWriteOutcome.Applied(value)  =>
              MutationReceiptExecution.Applied(value.value, value.entity)
          }
        )
      }
    }
    Idempotent
      .withClock(receipts, IO.pure(now))
      .execute[String](
        "createJob",
        "actor-1",
        request,
        value => MutationEntityReference("job", value),
        reference => UseCaseIO.pure(reference.entityId)
      )(context => UseCaseIO.pure(if (context eq expected) "same-context" else "different-context"))
      .value
      .map { result =>
        assertEquals(result, Right("same-context"))
      }
  }

  test("maps fingerprint conflicts and in-progress receipts to stable use-case errors") {
    def result(execution: MutationReceiptExecution[Nothing, Nothing]) =
      Idempotent
        .withClock(new FixedReceiptRepository(execution), IO.pure(now))
        .execute[String](
          "createJob",
          "actor-1",
          request,
          value => MutationEntityReference("job", value),
          reference => UseCaseIO.pure(reference.entityId)
        )(_ => UseCaseIO.pure("unused"))
        .value

    (result(MutationReceiptExecution.FingerprintMismatch), result(MutationReceiptExecution.InProgress)).mapN {
      (conflict, inProgress) =>
        assertEquals(conflict, Left(UseCaseError.Repository(RepositoryError.Conflict)))
        assertEquals(inProgress, Left(UseCaseError.Repository(RepositoryError.Unavailable)))
    }
  }

  test("keeps repository failures in the transaction-aborting outer channel") {
    val receipts = new MutationReceiptRepository {
      override def execute[A, E](
          key: MutationReceiptKey,
          fingerprint: MutationReceiptFingerprint,
          currentTime: Instant,
          expiresAt: Instant
      )(
          write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
      ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] = {
        val _ = (key, fingerprint, currentTime, expiresAt)
        write(MutationWriteContext.noop).flatMap {
          case Left(error) => IO.pure(Left(error))
          case other       => IO.raiseError(new AssertionError(s"expected outer repository failure, received $other"))
        }
      }
    }
    Idempotent
      .withClock(receipts, IO.pure(now))
      .execute[String](
        "createJob",
        "actor-1",
        request,
        value => MutationEntityReference("job", value),
        reference => UseCaseIO.pure(reference.entityId)
      )(_ => UseCaseIO.left(UseCaseError.Repository(RepositoryError.Conflict)))
      .value
      .map { result =>
        assertEquals(result, Left(UseCaseError.Repository(RepositoryError.Conflict)))
      }
  }

  test("keeps business rejection distinct from repository failure") {
    val rejection = UseCaseError.Domain(DomainError.Forbidden)
    Idempotent
      .withClock(MutationReceiptRepository.noop, IO.pure(now))
      .execute[String](
        "createJob",
        "actor-1",
        request,
        value => MutationEntityReference("job", value),
        reference => UseCaseIO.pure(reference.entityId)
      )(_ => UseCaseIO.left(rejection))
      .value
      .map(result => assertEquals(result, Left(rejection)))
  }

  test("preserves operation scope fingerprint entity and seven-day expiry") {
    final case class Observed(
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        currentTime: Instant,
        expiresAt: Instant,
        entity: MutationEntityReference
    )

    for {
      observed <- Ref.of[IO, Option[Observed]](None)
      receipts = new MutationReceiptRepository {
        override def execute[A, E](
            key: MutationReceiptKey,
            fingerprint: MutationReceiptFingerprint,
            currentTime: Instant,
            expiresAt: Instant
        )(
            write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
        ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] =
          write(MutationWriteContext.noop).flatMap {
            case Right(MutationWriteOutcome.Applied(value)) =>
              observed.set(Some(Observed(key, fingerprint, currentTime, expiresAt, value.entity))) *>
                IO.pure(Right(MutationReceiptExecution.Applied(value.value, value.entity)))
            case other => IO.raiseError(new AssertionError(s"expected successful write, received $other"))
          }
      }
      result <- Idempotent
        .withClock(receipts, IO.pure(now))
        .execute[String](
          "createJob",
          "actor-1",
          request,
          value => MutationEntityReference("job", value),
          reference => UseCaseIO.pure(reference.entityId)
        )(_ => UseCaseIO.pure("job-1"))
        .value
      captured <- observed.get
    } yield {
      assertEquals(result, Right("job-1"))
      assertEquals(
        captured,
        Some(
          Observed(
            MutationReceiptKey("createJob", "actor-1", request.idempotencyKey),
            request.fingerprint,
            now,
            now.plusSeconds(7.days.toSeconds),
            MutationEntityReference("job", "job-1")
          )
        )
      )
    }
  }

  private final class RecordingReceiptRepository(
      state: Ref[IO, Option[MutationEntityReference]]
  ) extends MutationReceiptRepository {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        currentTime: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
    ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] = {
      val _ = (key, fingerprint, currentTime, expiresAt)
      state.get.flatMap {
        case Some(entity) => IO.pure(Right(MutationReceiptExecution.Replay(entity)))
        case None         =>
          write(MutationWriteContext.noop).flatMap {
            case Left(error)                                 => IO.pure(Left(error))
            case Right(MutationWriteOutcome.Rejected(error)) =>
              IO.pure(Right(MutationReceiptExecution.Rejected(error)))
            case Right(MutationWriteOutcome.Applied(value)) =>
              state.set(Some(value.entity)).as(Right(MutationReceiptExecution.Applied(value.value, value.entity)))
          }
      }
    }
  }

  private final class FixedReceiptRepository(
      execution: MutationReceiptExecution[Nothing, Nothing]
  ) extends MutationReceiptRepository {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        currentTime: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
    ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] = {
      val _ = (key, fingerprint, currentTime, expiresAt, write)
      IO.pure(Right(execution))
    }
  }
}
