package com.example.graphQL.cats.service

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** ARR-07.1 and ARR-07.2: degraded components are decided by staleness and age thresholds on a controlled clock. */
final class WorkerHeartbeatsSpec extends CatsEffectSuite {
  private val thresholds = ReadinessThresholds(60.seconds, 300.seconds, 1800.seconds)

  private def ages(value: IO[Option[FiniteDuration]]): PendingWorkAge[IO] = _ => value
  private val nothingWaiting = ages(IO.pure(None))

  private def collector(
      heartbeats: WorkerHeartbeats[IO],
      embedding: IO[Boolean] = IO.pure(true),
      outbox: PendingWorkAge[IO] = nothingWaiting,
      closure: PendingWorkAge[IO] = nothingWaiting,
      diagnostics: Diagnostics[IO] = Diagnostics.noop
  ): IO[DegradedComponents[IO]] =
    DegradedComponents.create[IO](heartbeats, thresholds, embedding, outbox, closure, diagnostics)

  test("ARR-07.1 a publisher that stops reporting becomes degraded after the staleness threshold and recovers") {
    TestControl.executeEmbed(
      for {
        heartbeats <- WorkerHeartbeats.create[IO]
        degraded <- collector(heartbeats)
        _ <- heartbeats.register(ReadinessComponent.KafkaPublisher)
        _ <- IO.sleep(60.seconds)
        atThreshold <- degraded.current
        _ <- IO.sleep(1.second)
        stale <- degraded.current
        _ <- heartbeats.beat(ReadinessComponent.KafkaPublisher)
        recovered <- degraded.current
      } yield {
        assertEquals(atThreshold, Nil)
        assertEquals(stale, List(ReadinessComponent.KafkaPublisher))
        assertEquals(recovered, Nil)
      }
    )
  }

  test("ARR-07.1 a worker that was never started is not reported") {
    TestControl.executeEmbed(
      for {
        heartbeats <- WorkerHeartbeats.create[IO]
        degraded <- collector(heartbeats)
        _ <- IO.sleep(1.hour)
        current <- degraded.current
      } yield assertEquals(current, Nil)
    )
  }

  test("ARR-07.1 components are listed once each in declaration order") {
    TestControl.executeEmbed(
      for {
        heartbeats <- WorkerHeartbeats.create[IO]
        degraded <- collector(heartbeats, embedding = IO.pure(false))
        _ <- heartbeats.register(ReadinessComponent.RecruiterJobClosure)
        _ <- heartbeats.register(ReadinessComponent.KafkaConsumer)
        _ <- IO.sleep(2.minutes)
        current <- degraded.current
      } yield assertEquals(
        current,
        List(
          ReadinessComponent.KafkaConsumer,
          ReadinessComponent.EmbeddingWorker,
          ReadinessComponent.RecruiterJobClosure
        )
      )
    )
  }

  test("ARR-07.2 outbox age and closure-row age are reported only beyond their thresholds") {
    for {
      heartbeats <- WorkerHeartbeats.create[IO]
      within <- collector(
        heartbeats,
        outbox = ages(IO.pure(Some(300.seconds))),
        closure = ages(IO.pure(Some(1800.seconds)))
      ).flatMap(_.current)
      beyond <- collector(
        heartbeats,
        outbox = ages(IO.pure(Some(301.seconds))),
        closure = ages(IO.pure(Some(1801.seconds)))
      ).flatMap(_.current)
      outboxOnly <- collector(heartbeats, outbox = ages(IO.pure(Some(1.hour)))).flatMap(_.current)
    } yield {
      assertEquals(within, Nil)
      assertEquals(beyond, List(ReadinessComponent.OutboxAge, ReadinessComponent.ClosureWorkAge))
      assertEquals(outboxOnly, List(ReadinessComponent.OutboxAge))
    }
  }

  test("ARR-07.2 a failing age query degrades its component without failing the observation") {
    for {
      heartbeats <- WorkerHeartbeats.create[IO]
      current <- collector(heartbeats, outbox = ages(IO.raiseError(new IllegalStateException("synthetic-secret"))))
        .flatMap(_.current)
    } yield assertEquals(current, List(ReadinessComponent.OutboxAge))
  }

  test("ARR-07.1 a change of the degraded set is logged once with the stable name only") {
    for {
      logged <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      sink = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          logged.update(_ :+ (event -> fields))
      }
      heartbeats <- WorkerHeartbeats.create[IO]
      degraded <- collector(heartbeats, embedding = IO.pure(false), diagnostics = sink)
      _ <- degraded.current.replicateA_(3)
      events <- logged.get
    } yield assertEquals(
      events,
      List(LogEvent.ReadinessDegraded -> Map(LogField.Worker -> "embedding-worker"))
    )
  }

  test("ARR-07.1 every component name is a valid public log value") {
    ReadinessComponent.values.foreach(component =>
      assert(LogFields.validPublic(LogField.Worker, component.wire), component.wire)
    )
  }
}
