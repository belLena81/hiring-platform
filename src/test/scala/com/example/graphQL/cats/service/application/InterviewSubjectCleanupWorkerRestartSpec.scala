package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
import java.time.Instant
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, RepositoryError}
import com.example.graphQL.cats.service.port.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** ARR-06.1: a failure outside a single cleanup step is reported and the worker resumes after its restart delay. */
final class InterviewSubjectCleanupWorkerRestartSpec extends CatsEffectSuite {

  /** Never consulted by these tests; every answer is the empty one. */
  private object IdleRepository extends InterviewSubjectCleanupRepository[IO] {
    override def producerBatch(subject: UserId) = RepositoryF.pure[IO, Vector[String]](Vector.empty)
    override def markProducersFenced(subject: UserId, ids: Vector[String], now: Instant) =
      RepositoryF.pure[IO, Unit](())
    override def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
      RepositoryF.pure[IO, InterviewCleanupPage](InterviewCleanupPage(Vector.empty, None))
    override def find(subject: UserId) = RepositoryF.pure[IO, Option[InterviewSubjectCleanup]](None)
    override def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
      RepositoryF.pure[IO, InterviewCleanupUpdate](InterviewCleanupUpdate.Applied)
    override def purge(subject: UserId) = RepositoryF.pure[IO, Unit](())
    override def absent(subject: UserId) = RepositoryF.pure[IO, Boolean](true)
  }

  private object NoopFencer extends InterviewPublisherFencer[IO] {
    override def fence(transactionalIds: Vector[String]) = RepositoryF.pure[IO, Unit](())
  }

  private def worker(events: Ref[IO, Vector[(LogEvent, Map[LogField, String])]]) =
    new InterviewSubjectCleanupWorker[IO](
      IdleRepository,
      NoopFencer,
      IO.pure(Vector.empty),
      _ => IO.pure(true),
      new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ (event -> fields))
      },
      restartDelay = 5.seconds
    )

  test("ARR-06.1 a thrown cleanup polling failure is logged and polling resumes after the restart delay") {
    val program = for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      runs <- Ref.of[IO, Vector[FiniteDuration]](Vector.empty)
      stream = fs2.Stream.eval(IO.monotonic.flatMap(at => runs.updateAndGet(_ :+ at))).flatMap { seen =>
        if (seen.size == 1) fs2.Stream.raiseError[IO](new IllegalStateException("boom"))
        else fs2.Stream.never[IO]
      }
      _ <- worker(events).supervised(stream).use(_ => IO.sleep(20.seconds))
      started <- runs.get
      logged <- events.get
    } yield (started, logged.map(_._1))

    TestControl.executeEmbed(program).map { case (started, logged) =>
      assertEquals(started, Vector(Duration.Zero, 5.seconds))
      assertEquals(logged, Vector(LogEvent.RuntimeFailed))
    }
  }

  test("ARR-06.1 releasing the resource stops the cleanup polling loop") {
    val program = for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      runs <- Ref.of[IO, Int](0)
      stream = fs2.Stream.eval(runs.update(_ + 1)) ++ fs2.Stream.raiseError[IO](new IllegalStateException("boom"))
      _ <- worker(events).supervised(stream).use(_ => IO.sleep(7.seconds))
      afterRelease <- runs.get
      _ <- IO.sleep(60.seconds)
      later <- runs.get
    } yield (afterRelease, later)

    TestControl.executeEmbed(program).map { case (afterRelease, later) =>
      assertEquals(afterRelease, 2)
      assertEquals(later, 2)
    }
  }

  private def beats(entries: Vector[Either[RepositoryError, InterviewSubjectCleanup]]): IO[Int] =
    Ref.of[IO, Int](0).flatMap { count =>
      val repository = new InterviewSubjectCleanupRepository[IO] {
        def producerBatch(subject: UserId) = IdleRepository.producerBatch(subject)
        def markProducersFenced(subject: UserId, ids: Vector[String], now: Instant) =
          IdleRepository.markProducersFenced(subject, ids, now)
        def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant) =
          RepositoryF.pure[IO, InterviewCleanupPage](InterviewCleanupPage(entries, None))
        def find(subject: UserId) = IdleRepository.find(subject)
        def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup) =
          IdleRepository.transition(expected, next)
        def purge(subject: UserId) = IdleRepository.purge(subject)
        def absent(subject: UserId) = IdleRepository.absent(subject)
      }
      new InterviewSubjectCleanupWorker[IO](
        repository,
        NoopFencer,
        IO.pure(Vector.empty),
        _ => IO.pure(true),
        Diagnostics.noop[IO],
        restartDelay = 5.seconds,
        alive = count.update(_ + 1)
      ).resource.surround(IO.sleep(10.seconds)) *> count.get
    }

  test(
    "ARR-07.1 the cleanup worker reports progress for a page it handled and none for a page with a failing subject"
  ) {
    TestControl
      .executeEmbed((beats(Vector.empty), beats(Vector(Left(RepositoryError.Unavailable)))).tupled)
      .map { case (healthy, failing) =>
        assert(healthy >= 8, clue(healthy))
        assertEquals(failing, 0)
      }
  }
}
