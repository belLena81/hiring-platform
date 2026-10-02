package com.example.hiring.analytics.cli

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import munit.CatsEffectSuite
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

final class StreamingProcessTerminationSpec extends CatsEffectSuite {
  private final class Signals(
      callbacks: Ref[IO, Map[String, () => Unit]],
      events: Ref[IO, Vector[String]],
      failInt: Boolean
  ) extends StreamingProcessSignals {
    override def install(name: String, requestStop: () => Unit): Resource[IO, Unit] =
      Resource.make(
        if (failInt && name == "INT")
          IO.raiseError[Unit](new IllegalArgumentException("synthetic installation failure"))
        else callbacks.update(_ + (name -> requestStop)) *> events.update(_ :+ s"install:$name")
      )(_ => events.update(_ :+ s"restore:$name"))
  }

  private def fixture(
      failInt: Boolean = false
  ): IO[(Signals, Ref[IO, Map[String, () => Unit]], Ref[IO, Vector[String]])] =
    for {
      callbacks <- Ref.of[IO, Map[String, () => Unit]](Map.empty)
      events <- Ref.of[IO, Vector[String]](Vector.empty)
    } yield (new Signals(callbacks, events, failInt), callbacks, events)

  test("normal completion restores both previous handlers in reverse acquisition order") {
    fixture().flatMap { case (signals, callbacks, events) =>
      for {
        _ <- StreamingProcessTermination.run(IO.unit, signals)
        observed <- events.get
        installed <- callbacks.get
        _ <- IO.delay { installed("TERM")(); installed("INT")() }
      } yield assertEquals(observed, Vector("install:TERM", "install:INT", "restore:INT", "restore:TERM"))
    }
  }

  test("program failures remain visible while handlers are restored") {
    val failure = new IllegalStateException("synthetic program failure")
    fixture().flatMap { case (signals, _, events) =>
      for {
        result <- StreamingProcessTermination.run(IO.raiseError(failure), signals).attempt
        observed <- events.get
      } yield {
        assertEquals(result, Left(failure))
        assertEquals(observed.takeRight(2), Vector("restore:INT", "restore:TERM"))
      }
    }
  }

  test("partial installation restores TERM and does not start the program") {
    fixture(failInt = true).flatMap { case (signals, _, events) =>
      for {
        started <- Ref.of[IO, Boolean](false)
        result <- StreamingProcessTermination.run(started.set(true), signals).attempt
        observed <- events.get
        ran <- started.get
      } yield {
        assert(result.isLeft)
        assert(!ran)
        assertEquals(observed, Vector("install:TERM", "restore:TERM"))
      }
    }
  }

  test("repeated TERM and INT requests join program cleanup before restoring handlers") {
    fixture().flatMap { case (signals, callbacks, events) =>
      for {
        started <- Deferred[IO, Unit]
        releasing <- Deferred[IO, Unit]
        allowRelease <- Deferred[IO, Unit]
        program = Resource
          .make(started.complete(()).void)(_ =>
            releasing.complete(()).void *> allowRelease.get *> events.update(_ :+ "program:released")
          )
          .use(_ => IO.never[Unit])
        fiber <- StreamingProcessTermination.run(program, signals).start
        _ <- started.get
        installed <- callbacks.get
        _ <- IO.delay((1 to 20).foreach(_ => { installed("TERM")(); installed("INT")() }))
        _ <- releasing.get
        before <- events.get
        _ = assert(!before.exists(_.startsWith("restore:")))
        _ <- allowRelease.complete(())
        _ <- fiber.joinWithNever
        after <- events.get
        _ = assertEquals(after.takeRight(3), Vector("program:released", "restore:INT", "restore:TERM"))
        // A callback already obtained by the JVM cannot submit after its Dispatcher has been released.
        _ <- IO.delay { installed("TERM")(); installed("INT")() }
      } yield ()
    }
  }

  test("external cancellation joins cleanup and restores handlers") {
    fixture().flatMap { case (signals, _, events) =>
      for {
        started <- Deferred[IO, Unit]
        fiber <- StreamingProcessTermination
          .run(
            Resource
              .make(started.complete(()).void)(_ => events.update(_ :+ "program:released"))
              .use(_ => IO.never[Unit]),
            signals
          )
          .start
        _ <- started.get
        _ <- fiber.cancel
        observed <- events.get
      } yield assertEquals(observed.takeRight(3), Vector("program:released", "restore:INT", "restore:TERM"))
    }
  }

  test("normal resource finalizer failures remain visible") {
    val failure = new IllegalStateException("synthetic finalizer failure")
    fixture().flatMap { case (signals, _, events) =>
      for {
        result <- StreamingProcessTermination
          .run(
            Resource.make(IO.unit)(_ => IO.raiseError(failure)).use(_ => IO.unit),
            signals
          )
          .attempt
        observed <- events.get
      } yield {
        assertEquals(result, Left(failure))
        assertEquals(observed.takeRight(2), Vector("restore:INT", "restore:TERM"))
      }
    }
  }

  test("signal-triggered cancellation finalizer errors reach the runtime failure reporter") {
    val failure = new IllegalStateException("synthetic cancellation finalizer failure")
    for {
      delegate <- IO.executionContext
      reported <- IO.delay(new ConcurrentLinkedQueue[Throwable]())
      reporter = new ExecutionContext {
        override def execute(runnable: Runnable): Unit = delegate.execute(runnable)
        override def reportFailure(error: Throwable): Unit = { reported.add(error); () }
      }
      _ <- fixture()
        .flatMap { case (signals, callbacks, events) =>
          for {
            started <- Deferred[IO, Unit]
            fiber <- StreamingProcessTermination
              .run(
                Resource.make(started.complete(()).void)(_ => IO.raiseError(failure)).use(_ => IO.never[Unit]),
                signals
              )
              .start
            _ <- started.get
            installed <- callbacks.get
            _ <- IO.delay(installed("TERM")())
            _ <- fiber.joinWithNever
            observed <- events.get
            failures <- IO.delay(reported.iterator().asScala.toVector)
          } yield {
            assert(failures.contains(failure), "canceled resource failures must remain visible to the runtime")
            assertEquals(observed.takeRight(2), Vector("restore:INT", "restore:TERM"))
          }
        }
        .evalOn(reporter)
    } yield ()
  }
}
