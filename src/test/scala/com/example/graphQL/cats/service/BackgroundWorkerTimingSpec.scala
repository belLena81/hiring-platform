package com.example.graphQL.cats.service

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** EPS-05.5: worker outcomes are reported from virtual time; the worker only needs `Spawn`. */
final class BackgroundWorkerTimingSpec extends CatsEffectSuite {
  private def run(work: IO[Unit], holdFor: FiniteDuration) =
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ (event -> fields))
      }
      _ <- BackgroundWorker.resource[IO]("timed-worker", diagnostics)(work).use(_ => IO.sleep(holdFor))
      observed <- events.get
    } yield observed

  test("EPS-05.5 an errored worker is reported once with its name") {
    TestControl
      .executeEmbed(run(IO.sleep(5.seconds) *> IO.raiseError(new IllegalStateException("boom")), 20.seconds))
      .map { observed =>
        assertEquals(observed.map(_._1), Vector(LogEvent.RuntimeFailed))
        assertEquals(observed.head._2.get(LogField.Worker), Some("timed-worker"))
      }
  }

  test("EPS-05.5 a worker that completes is silent") {
    TestControl.executeEmbed(run(IO.sleep(5.seconds), 20.seconds)).map(observed => assertEquals(observed, Vector.empty))
  }

  test("EPS-05.5 a worker cancelled when the resource is released is silent") {
    TestControl.executeEmbed(run(IO.never, 20.seconds)).map(observed => assertEquals(observed, Vector.empty))
  }
}
