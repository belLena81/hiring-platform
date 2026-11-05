package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref, Resource, Deferred}
import cats.syntax.apply.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import munit.CatsEffectSuite

final class ProducerGenerationLifecycleSpec extends CatsEffectSuite {
  private val id = "hiring-interview-worker-00000000-0000-0000-0000-000000000001"
  private val generationId = java.util.UUID.fromString("00000000-0000-0000-0000-0000000000a1")
  private val fields = Map(LogField.TransactionalId -> id, LogField.GenerationId -> generationId.toString)

  test("generation lifecycle diagnostics follow initialization and actual producer release") {
    for {
      released <- Ref.of[IO, Boolean](false)
      observations <- Ref.of[IO, Vector[(LogEvent, Boolean, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          released.get.flatMap(closed => observations.update(_ :+ ((event, closed, fields))))
      }
      _ <- KafkaRuntimeSupport
        .observedGeneration(id, generationId, diagnostics)(
          Resource.make(IO.unit)(_ => released.set(true))
        )
        .use(_ => IO.unit)
      events <- observations.get
    } yield assertEquals(
      events,
      Vector(
        (LogEvent.ProducerGenerationStarted, false, fields),
        (LogEvent.ProducerGenerationClosed, true, fields)
      )
    )
  }

  test("failed acquisition emits no initialized or closed generation") {
    for {
      count <- Ref.of[IO, Int](0)
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) = count.update(_ + 1)
      }
      result <- KafkaRuntimeSupport
        .observedGeneration(id, generationId, diagnostics)(
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
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ event)
      }
      partial = Resource.make(IO.unit)(_ => released.set(true)) *>
        Resource.eval(acquiring.complete(()).void *> IO.never[Unit])
      fiber <- KafkaRuntimeSupport.observedGeneration(id, generationId, diagnostics)(partial).use(_ => IO.unit).start
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
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          IO.raiseError(new IllegalStateException("diagnostics unavailable"))
      }
      _ <- KafkaRuntimeSupport
        .observedGeneration(id, generationId, diagnostics)(
          Resource.make(IO.unit)(_ => released.set(true))
        )
        .use(_ => IO.unit)
      closed <- released.get
    } yield assert(closed)
  }

  test("failed producer release does not emit a successful close event") {
    for {
      events <- Ref.of[IO, Vector[LogEvent]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ event)
      }
      result <- KafkaRuntimeSupport
        .observedGeneration(id, generationId, diagnostics)(
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

  test("only known-prefix bounded worker IDs and canonical generation UUIDs survive public diagnostic validation") {
    assert(LogFields.validPublic(LogField.TransactionalId, id))
    assert(LogFields.validPublic(LogField.TransactionalId, "hiring-publisher-publisher-a"))
    List("arbitrary", "hiring-interview-worker-Worker_1", "hiring-interview-worker-a:b", id + "\nsecret").foreach {
      invalid => assert(!LogFields.validPublic(LogField.TransactionalId, invalid))
    }
    assert(LogFields.validPublic(LogField.GenerationId, java.util.UUID.randomUUID().toString))
    List("not-a-uuid", "1-1-1-1-1", java.util.UUID.randomUUID().toString.toUpperCase + "\nx").foreach { invalid =>
      assert(!LogFields.validPublic(LogField.GenerationId, invalid))
    }
  }
}
