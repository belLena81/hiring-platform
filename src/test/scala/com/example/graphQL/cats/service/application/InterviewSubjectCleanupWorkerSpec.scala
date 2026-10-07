package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref, Deferred}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.port.{
  InterviewPublisherFencer,
  InterviewSubjectCleanupRepository,
  InterviewCleanupUpdate,
  InterviewCleanupCursor,
  InterviewCleanupPage,
  RepositoryIO,
  RepositoryError
}
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

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

  private def page(
      values: Vector[InterviewSubjectCleanup],
      cursor: Option[InterviewCleanupCursor]
  ): Either[RepositoryError, InterviewCleanupPage] =
    Either.cond(cursor.isEmpty, InterviewCleanupPage(values.map(Right(_)), None), RepositoryError.InvalidStoredData)

  private final class Store(
      state: Ref[IO, InterviewSubjectCleanup],
      calls: Ref[IO, Vector[String]],
      registrations: Ref[IO, Vector[String]]
  ) extends InterviewSubjectCleanupRepository {
    override def producerBatch(id: UserId) = RepositoryIO.lift(registrations.get.map(_.take(64)))
    override def markProducersFenced(id: UserId, ids: Vector[String], at: Instant) =
      RepositoryIO.lift(registrations.update(_.filterNot(ids.contains)))
    override def pendingPage(
        cursor: Option[InterviewCleanupCursor],
        observedAt: Instant
    ): RepositoryIO[InterviewCleanupPage] = RepositoryIO.fromIOEither(state.get.map { value =>
      value.state match {
        case InterviewCleanupState.Complete(_) => page(Vector.empty, cursor)
        case _                                 => page(Vector(value), cursor)
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

  List(false, true).foreach { unexpectedFailure =>
    val failureKind = if (unexpectedFailure) "unexpected" else "typed"
    test(
      s"a $failureKind subject failure does not block later cleanup and failed subjects remain available for retry"
    ) {
      val second = initial.copy(
        subjectId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000003")),
        transactionalIds = Vector("hiring-interview-worker-00000000-0000-0000-0000-000000000004")
      )
      val third = initial.copy(
        subjectId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000005")),
        transactionalIds = Vector("hiring-interview-worker-00000000-0000-0000-0000-000000000006")
      )
      for {
        state <- Ref.of[IO, Vector[InterviewSubjectCleanup]](Vector(initial, second, third))
        registrations <- Ref.of[IO, Map[UserId, Vector[String]]](
          Vector(initial, second, third).map(v => v.subjectId -> v.transactionalIds).toMap
        )
        calls <- Ref.of[IO, Vector[String]](Vector.empty)
        events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
        store = new InterviewSubjectCleanupRepository {
          override def producerBatch(id: UserId) =
            RepositoryIO.lift(registrations.get.map(_.getOrElse(id, Vector.empty).take(64)))
          override def markProducersFenced(id: UserId, ids: Vector[String], at: Instant) =
            RepositoryIO.lift(
              registrations.update(values =>
                values.updated(id, values.getOrElse(id, Vector.empty).filterNot(ids.contains))
              )
            )
          override def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
            RepositoryIO.fromIOEither(state.get.map(values => page(values, cursor)))
          override def find(id: UserId) = RepositoryIO.lift(state.get.map(_.find(_.subjectId == id)))
          override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
            RepositoryIO.lift(state.modify { values =>
              if (values.contains(expected))
                (values.map(value => if (value == expected) next else value), InterviewCleanupUpdate.Applied)
              else (values, InterviewCleanupUpdate.StaleRevision)
            })
          override def purge(id: UserId) = RepositoryIO.lift(calls.update(_ :+ s"purge:${id.value}"))
          override def absent(id: UserId) = RepositoryIO.lift(calls.get.map(_.contains(s"purge:${id.value}")))
        }
        fencer = new InterviewPublisherFencer {
          override def fence(ids: Vector[String]): RepositoryIO[Unit] =
            RepositoryIO.lift(calls.update(_ :+ s"fence:${ids.headOption.getOrElse("missing")}")).flatMap { _ =>
              if (ids == initial.transactionalIds && unexpectedFailure)
                RepositoryIO.lift(IO.raiseError(new IllegalStateException("private provider details")))
              else if (ids == initial.transactionalIds) RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
              else if (ids == third.transactionalIds) RepositoryIO.fromEither(Left(RepositoryError.Conflict))
              else RepositoryIO.fromEither(Right(()))
            }
        }
        diagnostics = new Diagnostics {
          override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
            events.update(_ :+ (event, fields))
        }
        worker = new InterviewSubjectCleanupWorker(store, fencer, IO.pure(barriers), _ => IO.pure(true), diagnostics)
        first <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
        afterFirst <- state.get
        retried <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
        afterRetry <- state.get
        effects <- calls.get
        recorded <- events.get
      } yield {
        assertEquals(first, Left(RepositoryError.Unavailable))
        assertEquals(retried, Left(RepositoryError.Unavailable))
        assertEquals(
          afterFirst,
          Vector(initial, second.copy(revision = 1L, state = InterviewCleanupState.ProducersFenced), third)
        )
        assertEquals(
          afterRetry,
          Vector(initial, second.copy(revision = 2L, state = InterviewCleanupState.MongoPurged), third)
        )
        assertEquals(effects.count(_ == s"fence:${initial.transactionalIds.headOption.getOrElse("missing")}"), 2)
        assert(effects.contains(s"purge:${second.subjectId.value}"))
        assert(!effects.contains(s"purge:${initial.subjectId.value}"))
        assert(!effects.contains(s"purge:${third.subjectId.value}"))
        assertEquals(recorded.map(_._1), Vector.fill(4)(LogEvent.RuntimeFailed))
        assert(recorded.forall(_._2.get(LogField.SpanName).contains("interviewCleanup.process")))
        assert(recorded.forall(_._2.forall { case (field, value) => LogFields.validPublic(field, value) }))
        assert(!recorded.exists(_._2.values.exists(_.contains("private provider details"))))
        if (!unexpectedFailure)
          assertEquals(recorded.map(_._2), Vector.fill(4)(Map(LogField.SpanName -> "interviewCleanup.process")))
      }
    }
  }

  test("generation churn is fenced in bounded durable batches before Mongo purge") {
    val ids = Vector.fill(130)("hiring-interview-worker-" + UUID.randomUUID())
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial.copy(transactionalIds = Vector.empty))
      registrations <- Ref.of[IO, Vector[String]](ids)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      batches <- Ref.of[IO, Vector[Int]](Vector.empty)
      fencer = new InterviewPublisherFencer {
        def fence(values: Vector[String]) = RepositoryIO.lift(batches.update(_ :+ values.size))
      }
      worker = new InterviewSubjectCleanupWorker(
        new Store(state, calls, registrations),
        fencer,
        IO.pure(barriers),
        _ => IO.pure(false),
        Diagnostics.noop,
        IO.pure(now)
      )
      _ <- worker.runOnce(None).value
      afterFirst <- state.get
      _ <- worker.runOnce(None).value
      afterSecond <- state.get
      _ <- worker.runOnce(None).value
      afterThird <- state.get
      observed <- batches.get
      effects <- calls.get
    } yield {
      assertEquals(afterFirst.state, InterviewCleanupState.Pending)
      assertEquals(afterSecond.state, InterviewCleanupState.Pending)
      assertEquals(afterThird.state, InterviewCleanupState.ProducersFenced)
      assertEquals(observed, Vector(64, 64, 2))
      assertEquals(effects, Vector.empty)
    }
  }

  test("fencer rejection never purges or captures a barrier") {
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial)
      registrations <- Ref.of[IO, Vector[String]](initial.transactionalIds)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      fencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]): RepositoryIO[Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
        }
      }
      worker = new InterviewSubjectCleanupWorker(
        new Store(state, calls, registrations),
        fencer,
        calls.update(_ :+ "capture").as(barriers),
        _ => IO.pure(true),
        Diagnostics.noop
      )
      result <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
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
      registrations <- Ref.of[IO, Vector[String]](initial.transactionalIds)
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
        new Store(state, calls, registrations),
        fencer,
        calls.update(_ :+ "capture").as(barriers),
        _ => IO.pure(true),
        Diagnostics.noop
      )
      fiber <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(()))).start
      _ <- started.get
      held <- state.get
      heldEffects <- calls.get
      _ <- confirmed.complete(())
      result <- fiber.joinWithNever
      _ <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
      _ <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
      _ <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
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
      registrations <- Ref.of[IO, Vector[String]](initial.transactionalIds)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      first <- Ref.of[IO, Boolean](true)
      store = new Store(state, calls, registrations)
      failingStore = new InterviewSubjectCleanupRepository {
        override def producerBatch(id: UserId) = store.producerBatch(id)
        override def markProducersFenced(id: UserId, ids: Vector[String], at: Instant) =
          RepositoryIO.lift(first.getAndSet(false)).flatMap {
            case true  => RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
            case false => store.markProducersFenced(id, ids, at)
          }
        override def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
          store.pendingPage(cursor, observedAt)
        override def find(id: UserId) = store.find(id)
        override def purge(id: UserId) = store.purge(id)
        override def absent(id: UserId) = store.absent(id)
        override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
          store.transition(expected, next)
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
      failed <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
      resumed <- worker.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
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
      registrations <- Ref.of[IO, Vector[String]](initial.transactionalIds)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      started <- Deferred[IO, Unit]
      blocked <- Deferred[IO, Unit]
      interruptedFencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]): RepositoryIO[Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryIO.lift(started.complete(()).void *> blocked.get)
        }
      }
      store = new Store(state, calls, registrations)
      interrupted = new InterviewSubjectCleanupWorker(
        store,
        interruptedFencer,
        calls.update(_ :+ "capture").as(barriers),
        _ => IO.pure(true),
        Diagnostics.noop
      )
      fiber <- interrupted.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(()))).start
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
      result <- resumed.runOnce(None).value.map(_.flatMap(progress => progress.firstFailure.toLeft(())))
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

  test("managed cleanup polling advances past row errors and preserves the cursor across failed page reads") {
    val token = InterviewCleanupCursor.fromEncoded("opaque-test-continuation")
    for {
      calls <- Ref.of[IO, Vector[Option[InterviewCleanupCursor]]](Vector.empty)
      continuationCalls <- Ref.of[IO, Int](0)
      finished <- Deferred[IO, Unit]
      repository = new InterviewSubjectCleanupRepository {
        override def producerBatch(id: UserId) = RepositoryIO.fromEither(Right(Vector.empty))
        override def markProducersFenced(id: UserId, ids: Vector[String], at: Instant) =
          RepositoryIO.fromEither(Right(()))
        override def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
          RepositoryIO.lift(calls.update(_ :+ cursor)).flatMap { _ =>
            assertEquals(observedAt, now)
            cursor match {
              case None =>
                RepositoryIO.fromEither(
                  Right(InterviewCleanupPage(Vector(Left(RepositoryError.InvalidStoredData)), Some(token)))
                )
              case Some(value) =>
                assertEquals(value, token)
                RepositoryIO.lift(continuationCalls.getAndUpdate(_ + 1)).flatMap {
                  case 0 => RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
                  case 1 => RepositoryIO.lift(IO.raiseError(new IllegalStateException("private driver detail")))
                  case _ =>
                    RepositoryIO
                      .lift(finished.complete(()).void)
                      .map(_ => InterviewCleanupPage(Vector.empty, None))
                }
            }
          }
        override def find(id: UserId) = RepositoryIO.fromEither(Right(None))
        override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
          RepositoryIO.fromEither(Right(InterviewCleanupUpdate.StaleRevision))
        override def purge(id: UserId) = RepositoryIO.fromEither(Right(()))
        override def absent(id: UserId) = RepositoryIO.fromEither(Right(false))
      }
      fencer = new InterviewPublisherFencer {
        override def fence(ids: Vector[String]) = RepositoryIO.fromEither(Right(()))
      }
      worker = new InterviewSubjectCleanupWorker(
        repository,
        fencer,
        IO.pure(barriers),
        _ => IO.pure(false),
        Diagnostics.noop,
        IO.pure(now)
      )
      _ <- worker.resource.use(_ => finished.get.timeout(10.seconds))
      observed <- calls.get
    } yield assertEquals(observed, Vector(None, Some(token), Some(token), Some(token)))
  }
}
