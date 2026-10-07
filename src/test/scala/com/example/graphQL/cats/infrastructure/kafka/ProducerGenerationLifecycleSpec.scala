package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref, Resource, Deferred}
import cats.syntax.apply.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import munit.CatsEffectSuite

final class ProducerGenerationLifecycleSpec extends CatsEffectSuite {
  private val id = "hiring-interview-worker-00000000-0000-0000-0000-000000000001"

  test("generation lifecycle diagnostics follow initialization and actual producer release") {
    for {
      released <- Ref.of[IO, Boolean](false)
      observations <- Ref.of[IO, Vector[(LogEvent, Boolean, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          released.get.flatMap(closed => observations.update(_ :+ ((event, closed, fields))))
      }
      _ <- OperationalEventKafkaRuntime
        .observedGeneration(id, diagnostics)(
          Resource.make(IO.unit)(_ => released.set(true))
        )
        .use(_ => IO.unit)
      events <- observations.get
    } yield assertEquals(
      events,
      Vector(
        (LogEvent.ProducerGenerationStarted, false, Map(LogField.TransactionalId -> id)),
        (LogEvent.ProducerGenerationClosed, true, Map(LogField.TransactionalId -> id))
      )
    )
  }

  test("failed acquisition emits no initialized or closed generation") {
    for {
      count <- Ref.of[IO, Int](0)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) = count.update(_ + 1)
      }
      result <- OperationalEventKafkaRuntime
        .observedGeneration(id, diagnostics)(
          Resource.eval(IO.raiseError[Unit](new IllegalStateException("initialization failed")))
        )
        .use(_ => IO.unit)
        .attempt
      calls <- count.get
    } yield {
      assert(result.isLeft)
      assertEquals(calls, 0)
    }
  }

  test("cancellation during producer acquisition releases partial resources without lifecycle success events") {
    for {
      acquiring <- Deferred[IO, Unit]
      released <- Ref.of[IO, Boolean](false)
      events <- Ref.of[IO, Vector[LogEvent]](Vector.empty)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ event)
      }
      partial = Resource.make(IO.unit)(_ => released.set(true)) *>
        Resource.eval(acquiring.complete(()).void *> IO.never[Unit])
      fiber <- OperationalEventKafkaRuntime.observedGeneration(id, diagnostics)(partial).use(_ => IO.unit).start
      _ <- acquiring.get
      _ <- fiber.cancel
      closed <- released.get
      observed <- events.get
    } yield {
      assert(closed)
      assertEquals(observed, Vector.empty)
    }
  }

  test("diagnostic failures cannot prevent producer release") {
    for {
      released <- Ref.of[IO, Boolean](false)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          IO.raiseError(new IllegalStateException("diagnostics unavailable"))
      }
      _ <- OperationalEventKafkaRuntime
        .observedGeneration(id, diagnostics)(
          Resource.make(IO.unit)(_ => released.set(true))
        )
        .use(_ => IO.unit)
      closed <- released.get
    } yield assert(closed)
  }

  test("failed producer release does not emit a successful close event") {
    for {
      events <- Ref.of[IO, Vector[LogEvent]](Vector.empty)
      diagnostics = new Diagnostics {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ event)
      }
      result <- OperationalEventKafkaRuntime
        .observedGeneration(id, diagnostics)(
          Resource.make(IO.unit)(_ => IO.raiseError(new IllegalStateException("close failed")))
        )
        .use(_ => IO.unit)
        .attempt
      observed <- events.get
    } yield {
      assert(result.isLeft)
      assertEquals(observed, Vector(LogEvent.ProducerGenerationStarted))
    }
  }

  test("only canonical bounded generation IDs survive public diagnostic validation") {
    assert(LogFields.validPublic(LogField.TransactionalId, id))
    List("arbitrary", "hiring-interview-worker-1-1-1-1-1", id + "\nsecret").foreach { invalid =>
      assert(!LogFields.validPublic(LogField.TransactionalId, invalid))
    }
  }
}
