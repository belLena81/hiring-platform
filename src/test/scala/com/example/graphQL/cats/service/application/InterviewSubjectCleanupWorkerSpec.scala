package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref, Deferred}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{
  InterviewPublisherFencer,
  InterviewSubjectCleanupRepository,
  InterviewCleanupUpdate,
  RepositoryIO,
  RepositoryError
}
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite

final class InterviewSubjectCleanupWorkerSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val subject = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  private val initial = InterviewSubjectCleanup(
    subject,
    0L,
    now,
    Vector("hiring-interview-worker-00000000-0000-0000-0000-000000000002"),
    InterviewCleanupState.Pending
  )
  private val barriers = Vector(
    InterviewRetentionBarrier("hiring.interview-commands", 0, 4L),
    InterviewRetentionBarrier("hiring.interview-results", 0, 5L)
  )

  private final class Store(state: Ref[IO, InterviewSubjectCleanup], calls: Ref[IO, Vector[String]])
      extends InterviewSubjectCleanupRepository {
    override def pending: RepositoryIO[Vector[InterviewSubjectCleanup]] = RepositoryIO.lift(state.get.map { value =>
      value.state match {
        case InterviewCleanupState.Complete(_) => Vector.empty
        case _                                 => Vector(value)
      }
    })
    override def find(id: UserId): RepositoryIO[Option[InterviewSubjectCleanup]] =
      RepositoryIO.lift(state.get.map(value => Option.when(value.subjectId == id)(value)))
    override def transition(
        expected: InterviewSubjectCleanup,
        next: InterviewSubjectCleanup
    ): RepositoryIO[InterviewCleanupUpdate] =
      RepositoryIO.lift(state.modify { stored =>
        if (stored == expected) (next, InterviewCleanupUpdate.Applied)
        else (stored, InterviewCleanupUpdate.StaleRevision)
      })
    override def purge(id: UserId): RepositoryIO[Unit] = RepositoryIO.lift(calls.update(_ :+ s"purge:${id.value}"))
    override def absent(id: UserId): RepositoryIO[Boolean] =
      RepositoryIO.lift(calls.get.map(_.contains(s"purge:${id.value}")))
  }

  test("fencer rejection never purges or captures a barrier") {
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      fencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]): RepositoryIO[Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
        }
      }
      worker = new InterviewSubjectCleanupWorker(
        new Store(state, calls),
        fencer,
        calls.update(_ :+ "capture").as(barriers),
        _ => IO.pure(true),
        Diagnostics.noop
      )
      result <- worker.runOnce.value
      current <- state.get
      effects <- calls.get
    } yield {
      assertEquals(result, Left(RepositoryError.Unavailable))
      assertEquals(current, initial)
      assertEquals(effects, Vector.empty)
    }
  }

  test("an outstanding broker fence withholds purge and completion until confirmed") {
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      started <- Deferred[IO, Unit]
      confirmed <- Deferred[IO, Unit]
      fencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]): RepositoryIO[Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryIO.lift(started.complete(()).void *> confirmed.get)
        }
      }
      worker = new InterviewSubjectCleanupWorker(
        new Store(state, calls),
        fencer,
        calls.update(_ :+ "capture").as(barriers),
        _ => IO.pure(true),
        Diagnostics.noop
      )
      fiber <- worker.runOnce.value.start
      _ <- started.get
      held <- state.get
      heldEffects <- calls.get
      _ <- confirmed.complete(())
      result <- fiber.joinWithNever
      _ <- worker.runOnce.value
      _ <- worker.runOnce.value
      _ <- worker.runOnce.value
      completed <- state.get
      effects <- calls.get
    } yield {
      assertEquals(held, initial)
      assertEquals(heldEffects, Vector.empty)
      assertEquals(result, Right(()))
      assertEquals(completed.revision, 4L)
      assert(completed.state.isInstanceOf[InterviewCleanupState.Complete])
      assertEquals(effects.count(_ == "capture"), 1)
    }
  }

  test("a crash after accepted fencing repeats fencing before advancing durable state") {
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      first <- Ref.of[IO, Boolean](true)
      store = new Store(state, calls)
      failingStore = new InterviewSubjectCleanupRepository {
        override def pending = store.pending
        override def find(id: UserId) = store.find(id)
        override def purge(id: UserId) = store.purge(id)
        override def absent(id: UserId) = store.absent(id)
        override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
          RepositoryIO.lift(first.getAndSet(false)).flatMap {
            case true  => RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
            case false => store.transition(expected, next)
          }
      }
      fencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]): RepositoryIO[Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryIO.lift(calls.update(_ :+ "fence"))
        }
      }
      worker = new InterviewSubjectCleanupWorker(
        failingStore,
        fencer,
        IO.pure(barriers),
        _ => IO.pure(false),
        Diagnostics.noop
      )
      failed <- worker.runOnce.value
      resumed <- worker.runOnce.value
      current <- state.get
      effects <- calls.get
    } yield {
      assertEquals(failed, Left(RepositoryError.Unavailable))
      assertEquals(resumed, Right(()))
      assertEquals(current.state, InterviewCleanupState.ProducersFenced)
      assertEquals(effects, Vector("fence", "fence"))
    }
  }

  test("cancellation before fencing confirmation preserves pending work for restart") {
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      started <- Deferred[IO, Unit]
      blocked <- Deferred[IO, Unit]
      interruptedFencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]): RepositoryIO[Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryIO.lift(started.complete(()).void *> blocked.get)
        }
      }
      store = new Store(state, calls)
      interrupted = new InterviewSubjectCleanupWorker(
        store,
        interruptedFencer,
        calls.update(_ :+ "capture").as(barriers),
        _ => IO.pure(true),
        Diagnostics.noop
      )
      fiber <- interrupted.runOnce.value.start
      _ <- started.get
      _ <- fiber.cancel
      outcome <- fiber.join
      pending <- state.get
      successfulFencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]): RepositoryIO[Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryIO.lift(calls.update(_ :+ "fence"))
        }
      }
      resumed = new InterviewSubjectCleanupWorker(
        store,
        successfulFencer,
        calls.update(_ :+ "capture").as(barriers),
        _ => IO.pure(true),
        Diagnostics.noop
      )
      result <- resumed.runOnce.value
      current <- state.get
      effects <- calls.get
    } yield {
      assert(outcome.isCanceled)
      assertEquals(pending, initial)
      assertEquals(result, Right(()))
      assertEquals(current.state, InterviewCleanupState.ProducersFenced)
      assertEquals(effects, Vector("fence"))
    }
  }
}
