package com.example.hiring.analytics

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.*
import munit.CatsEffectSuite

final class StreamingQueryLifecycleSpec extends CatsEffectSuite {
  test("query cancellation stops the handle and releases maintenance") {
    for {
      waiting <- Deferred[IO, Unit]
      stopped <- Ref.of[IO, Int](0)
      released <- Ref.of[IO, Boolean](false)
      factory = new StreamingQueryFactory[IO] {
        def start: IO[StreamingQueryHandle[IO]] = IO.pure(new StreamingQueryHandle[IO] {
          def awaitTermination: IO[Unit] = waiting.complete(()).void *> IO.never
          def stop: IO[Unit] = stopped.update(_ + 1)
        })
      }
      maintenance = Resource.make(IO.unit)(_ => released.set(true)).as(IO.never[Unit])
      running <- StreamingQueryLifecycle.resource(factory, IO.never, maintenance).use(_ => IO.unit).start
      _ <- waiting.get
      _ <- running.cancel
      count <- stopped.get
      cleaned <- released.get
    } yield { assertEquals(count, 1); assert(cleaned) }
  }

  test("grant expiry and maintenance failure terminate and stop the query") {
    List(true, false).traverse_ { expiryFails =>
      for {
        waiting <- Deferred[IO, Unit]
        stopCount <- Ref.of[IO, Int](0)
        factory = new StreamingQueryFactory[IO] {
          def start: IO[StreamingQueryHandle[IO]] = IO.pure(new StreamingQueryHandle[IO] {
            def awaitTermination: IO[Unit] = waiting.complete(()).void *> IO.never
            def stop: IO[Unit] = stopCount.update(_ + 1)
          })
        }
        failure = waiting.get *> IO.raiseError[Unit](new IllegalStateException("controlled lifecycle failure"))
        expiry = if (expiryFails) failure else IO.never[Unit]
        maintenance = Resource.pure[IO, IO[Unit]](if (expiryFails) IO.never else failure)
        outcome <- StreamingQueryLifecycle.resource(factory, expiry, maintenance).use(_ => IO.unit).attempt
        stops <- stopCount.get
      } yield { assert(outcome.isLeft); assertEquals(stops, 1) }
    }
  }

  test("cancelled startup releases its acquired resources") {
    for {
      started <- Deferred[IO, Unit]
      released <- Ref.of[IO, Boolean](false)
      factory = new StreamingQueryFactory[IO] {
        def start: IO[StreamingQueryHandle[IO]] =
          Resource.make(IO.unit)(_ => released.set(true)).use(_ => started.complete(()).void *> IO.never)
      }
      running <- StreamingQueryLifecycle
        .resource(factory, IO.never, Resource.pure[IO, IO[Unit]](IO.never))
        .use(_ => IO.unit)
        .start
      _ <- started.get
      _ <- running.cancel
      cleaned <- released.get
    } yield assert(cleaned)
  }
  test("expiry cancels blocked startup and releases partial acquisition") {
    for {
      started <- Deferred[IO, Unit]
      released <- Ref.of[IO, Boolean](false)
      factory = new StreamingQueryFactory[IO] {
        def start: IO[StreamingQueryHandle[IO]] =
          Resource.make(IO.unit)(_ => released.set(true)).use(_ => started.complete(()).void *> IO.never)
      }
      expiry = started.get *> IO.raiseError[Unit](new IllegalStateException("controlled grant expiry"))
      outcome <- StreamingQueryLifecycle
        .resource(factory, expiry, Resource.pure[IO, IO[Unit]](IO.never))
        .use(_ => IO.unit)
        .attempt
      cleaned <- released.get
    } yield { assert(outcome.isLeft); assert(cleaned) }
  }

  test("shutdown stops callbacks before waiting for maintenance release") {
    for {
      waiting <- Deferred[IO, Unit]
      stopped <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      factory = new StreamingQueryFactory[IO] {
        def start: IO[StreamingQueryHandle[IO]] = IO.pure(new StreamingQueryHandle[IO] {
          def awaitTermination: IO[Unit] = waiting.complete(()).void *> IO.never
          def stop: IO[Unit] = stopped.complete(()).void
        })
      }
      maintenance = Resource.make(IO.unit)(_ => stopped.get *> released.complete(()).void).as(IO.never[Unit])
      running <- StreamingQueryLifecycle.resource(factory, IO.never, maintenance).use(_ => IO.unit).start
      _ <- waiting.get
      _ <- running.cancel
      _ <- released.get
    } yield ()
  }

}
