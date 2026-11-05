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
  ProducerGeneration,
  ProducerGenerationId,
  RepositoryF
}
import com.example.graphQL.cats.service.RepositoryError
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
  private def generation(transactionalId: String, n: Long = 1L) =
    ProducerGeneration(transactionalId, ProducerGenerationId(new UUID(0L, n)))
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
      registrations: Ref[IO, Vector[ProducerGeneration]]
  ) extends InterviewSubjectCleanupRepository[IO] {
    override def producerBatch(id: UserId) = RepositoryF.lift(registrations.get.map(_.take(64)))
    override def markProducersFenced(id: UserId, ids: Vector[ProducerGeneration]) =
      RepositoryF.lift(registrations.update(_.filterNot(ids.contains)))
    override def pendingPage(
        cursor: Option[InterviewCleanupCursor],
        observedAt: Instant
    ): RepositoryF[IO, InterviewCleanupPage] = RepositoryF.fromFEither(state.get.map { value =>
      value.state match {
        case InterviewCleanupState.Complete(_) => page(Vector.empty, cursor)
        case _                                 => page(Vector(value), cursor)
      }
    })
    override def find(id: UserId): RepositoryF[IO, Option[InterviewSubjectCleanup]] =
      RepositoryF.lift(state.get.map(value => Option.when(value.subjectId == id)(value)))
    override def transition(
        expected: InterviewSubjectCleanup,
        next: InterviewSubjectCleanup
    ): RepositoryF[IO, InterviewCleanupUpdate] =
      RepositoryF.lift(state.modify { stored =>
        if (stored == expected) (next, InterviewCleanupUpdate.Applied)
        else (stored, InterviewCleanupUpdate.StaleRevision)
      })
    override def purge(id: UserId): RepositoryF[IO, Unit] = RepositoryF.lift(calls.update(_ :+ s"purge:${id.value}"))
    override def absent(id: UserId): RepositoryF[IO, Boolean] =
      RepositoryF.lift(calls.get.map(_.contains(s"purge:${id.value}")))
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
        registrations <- Ref.of[IO, Map[UserId, Vector[ProducerGeneration]]](
          Vector(initial, second, third).map(v => v.subjectId -> v.transactionalIds.map(generation(_))).toMap
        )
        calls <- Ref.of[IO, Vector[String]](Vector.empty)
        events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
        store = new InterviewSubjectCleanupRepository[IO] {
          override def producerBatch(id: UserId) =
            RepositoryF.lift(registrations.get.map(_.getOrElse(id, Vector.empty).take(64)))
          override def markProducersFenced(id: UserId, ids: Vector[ProducerGeneration]) =
            RepositoryF.lift(
              registrations.update(values =>
                values.updated(id, values.getOrElse(id, Vector.empty).filterNot(ids.contains))
              )
            )
          override def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
            RepositoryF.fromFEither(state.get.map(values => page(values, cursor)))
          override def find(id: UserId) = RepositoryF.lift(state.get.map(_.find(_.subjectId == id)))
          override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
            RepositoryF.lift(state.modify { values =>
              if (values.contains(expected))
                (values.map(value => if (value == expected) next else value), InterviewCleanupUpdate.Applied)
              else (values, InterviewCleanupUpdate.StaleRevision)
            })
          override def purge(id: UserId) = RepositoryF.lift(calls.update(_ :+ s"purge:${id.value}"))
          override def absent(id: UserId) = RepositoryF.lift(calls.get.map(_.contains(s"purge:${id.value}")))
        }
        fencer = new InterviewPublisherFencer[IO] {
          override def fence(ids: Vector[String]): RepositoryF[IO, Unit] =
            RepositoryF.lift(calls.update(_ :+ s"fence:${ids.headOption.getOrElse("missing")}")).flatMap { _ =>
              if (ids == initial.transactionalIds && unexpectedFailure)
                RepositoryF.lift(IO.raiseError(new IllegalStateException("private provider details")))
              else if (ids == initial.transactionalIds) RepositoryF.at[IO].raise(RepositoryError.Unavailable)
              else if (ids == third.transactionalIds) RepositoryF.at[IO].raise(RepositoryError.Conflict)
              else RepositoryF.at[IO].pure(())
            }
        }
        diagnostics = new Diagnostics[IO] {
          override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
            events.update(_ :+ (event, fields))
        }
        worker = new InterviewSubjectCleanupWorker[IO](
          store,
          fencer,
          IO.pure(barriers),
          _ => IO.pure(true),
          diagnostics
        )
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
          // A typed failure keeps its safe error code, so the cause is not lost with the operation name.
          assertEquals(
            recorded.map(_._2.get(LogField.ErrorCode)),
            Vector("UNAVAILABLE", "CONFLICT", "UNAVAILABLE", "CONFLICT").map(Some(_))
          )
      }
    }
  }

  test("generation churn is fenced in bounded durable batches before Mongo purge") {
    val ids = Vector.tabulate(130)(n => generation("hiring-interview-worker-w" + n, n.toLong))
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial.copy(transactionalIds = Vector.empty))
      registrations <- Ref.of[IO, Vector[ProducerGeneration]](ids)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      batches <- Ref.of[IO, Vector[Int]](Vector.empty)
      fencer = new InterviewPublisherFencer[IO] {
        def fence(values: Vector[String]) = RepositoryF.lift(batches.update(_ :+ values.size))
      }
      worker = new InterviewSubjectCleanupWorker[IO](
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

  test(
    "KFH-04.3 erasure marks only the generations it read as Fenced while a later generation stays Active"
  ) {
    val id = "hiring-interview-worker-interview-a"
    val earlier = generation(id, 1L)
    val later = generation(id, 2L)
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial.copy(transactionalIds = Vector.empty))
      registrations <- Ref.of[IO, Vector[ProducerGeneration]](Vector(earlier))
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      fenced <- Ref.of[IO, Vector[Vector[String]]](Vector.empty)
      // The successor generation registers while the broker fence of the earlier one is in flight.
      fencer = new InterviewPublisherFencer[IO] {
        def fence(values: Vector[String]) =
          RepositoryF.lift(
            fenced
              .getAndUpdate(_ :+ values)
              .flatMap(previous => IO.whenA(previous.isEmpty)(registrations.update(_ :+ later)))
          )
      }
      worker = new InterviewSubjectCleanupWorker[IO](
        new Store(state, calls, registrations),
        fencer,
        IO.pure(barriers),
        _ => IO.pure(false),
        Diagnostics.noop,
        IO.pure(now)
      )
      _ <- worker.runOnce(None).value
      afterFirst <- state.get
      activeAfterFirst <- registrations.get
      _ <- worker.runOnce(None).value
      afterSecond <- state.get
      activeAfterSecond <- registrations.get
      observed <- fenced.get
    } yield {
      assertEquals(activeAfterFirst, Vector(later))
      assertEquals(afterFirst.state, InterviewCleanupState.Pending)
      assertEquals(activeAfterSecond, Vector.empty)
      assertEquals(afterSecond.state, InterviewCleanupState.ProducersFenced)
      assertEquals(observed, Vector(Vector(id), Vector(id)))
    }
  }

  test("worker persists captured physical topic identities through the configured pure policy") {
    val topics = InterviewTopicPair("hiring-test-worker.commands", "hiring-test-worker.results")
    val captured = Vector(
      InterviewRetentionBarrier(topics.commands, 0, 7L),
      InterviewRetentionBarrier(topics.results, 0, 8L)
    )
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial.copy(state = InterviewCleanupState.MongoPurged))
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      registrations <- Ref.of[IO, Vector[ProducerGeneration]](Vector.empty)
      fencer = new InterviewPublisherFencer[IO] {
        def fence(ids: Vector[String]) = RepositoryF.at[IO].pure(())
      }
      worker = new InterviewSubjectCleanupWorker[IO](
        new Store(state, calls, registrations),
        fencer,
        IO.pure(captured),
        _ => IO.pure(false),
        Diagnostics.noop,
        IO.pure(now),
        topics
      )
      progressed <- worker.runOnce(None).value
      stored <- state.get
    } yield {
      assertEquals(progressed.toOption.flatMap(_.firstFailure), None)
      assertEquals(stored.state, InterviewCleanupState.AwaitingRetention(captured))
    }
  }

  test("fencer rejection never purges or captures a barrier") {
    for {
      state <- Ref.of[IO, InterviewSubjectCleanup](initial)
      registrations <- Ref.of[IO, Vector[ProducerGeneration]](initial.transactionalIds.map(generation(_)))
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      fencer = new InterviewPublisherFencer[IO] {
        override def fence(ids: Vector[String]): RepositoryF[IO, Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryF.at[IO].raise(RepositoryError.Unavailable)
        }
      }
      worker = new InterviewSubjectCleanupWorker[IO](
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
      registrations <- Ref.of[IO, Vector[ProducerGeneration]](initial.transactionalIds.map(generation(_)))
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      started <- Deferred[IO, Unit]
      confirmed <- Deferred[IO, Unit]
      fencer = new InterviewPublisherFencer[IO] {
        override def fence(ids: Vector[String]): RepositoryF[IO, Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryF.lift(started.complete(()).void *> confirmed.get)
        }
      }
      worker = new InterviewSubjectCleanupWorker[IO](
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
      registrations <- Ref.of[IO, Vector[ProducerGeneration]](initial.transactionalIds.map(generation(_)))
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      first <- Ref.of[IO, Boolean](true)
      store = new Store(state, calls, registrations)
      failingStore = new InterviewSubjectCleanupRepository[IO] {
        override def producerBatch(id: UserId) = store.producerBatch(id)
        override def markProducersFenced(id: UserId, ids: Vector[ProducerGeneration]) =
          RepositoryF.lift(first.getAndSet(false)).flatMap {
            case true  => RepositoryF.at[IO].raise(RepositoryError.Unavailable)
            case false => store.markProducersFenced(id, ids)
          }
        override def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
          store.pendingPage(cursor, observedAt)
        override def find(id: UserId) = store.find(id)
        override def purge(id: UserId) = store.purge(id)
        override def absent(id: UserId) = store.absent(id)
        override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
          store.transition(expected, next)
      }
      fencer = new InterviewPublisherFencer[IO] {
        override def fence(ids: Vector[String]): RepositoryF[IO, Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryF.lift(calls.update(_ :+ "fence"))
        }
      }
      worker = new InterviewSubjectCleanupWorker[IO](
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
      registrations <- Ref.of[IO, Vector[ProducerGeneration]](initial.transactionalIds.map(generation(_)))
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      started <- Deferred[IO, Unit]
      blocked <- Deferred[IO, Unit]
      interruptedFencer = new InterviewPublisherFencer[IO] {
        override def fence(ids: Vector[String]): RepositoryF[IO, Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryF.lift(started.complete(()).void *> blocked.get)
        }
      }
      store = new Store(state, calls, registrations)
      interrupted = new InterviewSubjectCleanupWorker[IO](
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
      successfulFencer = new InterviewPublisherFencer[IO] {
        override def fence(ids: Vector[String]): RepositoryF[IO, Unit] = {
          assertEquals(ids, initial.transactionalIds)
          RepositoryF.lift(calls.update(_ :+ "fence"))
        }
      }
      resumed = new InterviewSubjectCleanupWorker[IO](
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
      repository = new InterviewSubjectCleanupRepository[IO] {
        override def producerBatch(id: UserId) = RepositoryF.at[IO].pure(Vector.empty)
        override def markProducersFenced(id: UserId, ids: Vector[ProducerGeneration]) =
          RepositoryF.at[IO].pure(())
        override def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
          RepositoryF.lift(calls.update(_ :+ cursor)).flatMap { _ =>
            assertEquals(observedAt, now)
            cursor match {
              case None =>
                RepositoryF
                  .at[IO]
                  .fromEither(
                    Right(InterviewCleanupPage(Vector(Left(RepositoryError.InvalidStoredData)), Some(token)))
                  )
              case Some(value) =>
                assertEquals(value, token)
                RepositoryF.lift(continuationCalls.getAndUpdate(_ + 1)).flatMap {
                  case 0 => RepositoryF.at[IO].raise(RepositoryError.Unavailable)
                  case 1 => RepositoryF.lift(IO.raiseError(new IllegalStateException("private driver detail")))
                  case _ =>
                    RepositoryF
                      .lift(finished.complete(()).void)
                      .map(_ => InterviewCleanupPage(Vector.empty, None))
                }
            }
          }
        override def find(id: UserId) = RepositoryF.at[IO].pure(None)
        override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
          RepositoryF.at[IO].pure(InterviewCleanupUpdate.StaleRevision)
        override def purge(id: UserId) = RepositoryF.at[IO].pure(())
        override def absent(id: UserId) = RepositoryF.at[IO].pure(false)
      }
      fencer = new InterviewPublisherFencer[IO] {
        override def fence(ids: Vector[String]) = RepositoryF.at[IO].pure(())
      }
      worker = new InterviewSubjectCleanupWorker[IO](
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
