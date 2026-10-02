package com.example.hiring.analytics

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.syntax.all.*
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock
import com.example.hiring.analytics.service.streaming.AnalyticsStreamingMaintenance
import com.example.hiring.analytics.errors.AnalyticsError
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class AnalyticsStreamingMaintenanceSpec extends CatsEffectSuite {
  test("idle maintenance repeats under ownership and cancellation releases its resource") {
    TestControl.executeEmbed {
      for {
        held <- Ref.of[IO, Boolean](false)
        calls <- Ref.of[IO, Int](0)
        lock = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] = Resource.make(held.set(true))(_ => held.set(false))
        }
        maintenance = new AnalyticsStreamingMaintenance[IO](
          "hiring",
          lock,
          60.seconds,
          _ => held.get.flatMap(owned => IO(assert(owned))) *> calls.update(_ + 1)
        )
        count <- maintenance.resource.use(_ => IO.sleep(125.seconds) *> calls.get)
        owned <- held.get
        _ <- IO { assertEquals(count, 2); assertEquals(owned, false) }
      } yield ()
    }
  }

  test("maintenance failure reaches the joined owner and does not silently retry") {
    val failure = new IllegalStateException("maintenance failed")
    TestControl.executeEmbed {
      AnalyticsLakehouseLock.processLocal[IO].use { lock =>
        new AnalyticsStreamingMaintenance[IO]("hiring", lock, 60.seconds, _ => IO.raiseError(failure)).resource
          .use(_.attempt)
          .flatMap(result => IO(assertEquals(result, Left(failure))))
      }
    }
  }

  test("maintenance shares callback ownership and a cancelled queued operation never runs") {
    for {
      calls <- Ref.of[IO, Int](0)
      _ <- AnalyticsLakehouseLock.processLocal[IO].use { lock =>
        val maintenance = new AnalyticsStreamingMaintenance[IO]("hiring", lock, 60.seconds, _ => calls.update(_ + 1))
        lock.resource("hiring").use { _ =>
          maintenance.runOnce.start.flatMap(fiber => IO.cede *> fiber.cancel)
        } *> maintenance.runOnce
      }
      count <- calls.get
      _ <- IO(assertEquals(count, 1))
    } yield ()
  }

  test("a mutex acquisition timeout defers maintenance without entering its body") {
    for {
      calls <- Ref.of[IO, Int](0)
      lock = new AnalyticsLakehouseLock[IO] {
        override def resource(root: String): Resource[IO, Unit] =
          Resource.eval(IO.raiseError[Unit](AnalyticsError.LakehouseLockTimeout))
      }
      maintenance = new AnalyticsStreamingMaintenance[IO]("hiring", lock, 60.seconds, _ => calls.update(_ + 1))
      _ <- maintenance.runOnce
      count <- calls.get
      _ <- IO(assertEquals(count, 0))
    } yield ()
  }

  test("the next maintenance tick retries ownership after an acquisition timeout") {
    TestControl.executeEmbed {
      for {
        attempts <- Ref.of[IO, Int](0)
        held <- Ref.of[IO, Boolean](false)
        calls <- Ref.of[IO, Int](0)
        releases <- Ref.of[IO, Int](0)
        lock = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] =
            Resource.eval(attempts.getAndUpdate(_ + 1)).flatMap {
              case 0 => Resource.eval(IO.raiseError[Unit](AnalyticsError.LakehouseLockTimeout))
              case _ => Resource.make(held.set(true))(_ => held.set(false) *> releases.update(_ + 1))
            }
        }
        maintenance = new AnalyticsStreamingMaintenance[IO](
          "hiring",
          lock,
          60.seconds,
          _ => held.get.flatMap(owned => IO(assert(owned))) *> calls.update(_ + 1)
        )
        _ <- maintenance.resource.use(_ => IO.sleep(125.seconds))
        observed <- (attempts.get, calls.get, releases.get, held.get).tupled
        _ <- IO(assertEquals(observed, (2, 1, 1, false)))
      } yield ()
    }
  }

  test("a timeout raised by the owned maintenance body remains fatal") {
    TestControl.executeEmbed {
      for {
        held <- Ref.of[IO, Boolean](false)
        calls <- Ref.of[IO, Int](0)
        lock = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] = Resource.make(held.set(true))(_ => held.set(false))
        }
        maintenance = new AnalyticsStreamingMaintenance[IO](
          "hiring",
          lock,
          60.seconds,
          _ => calls.update(_ + 1) *> IO.raiseError[Unit](AnalyticsError.LakehouseLockTimeout)
        )
        result <- maintenance.resource.use(_.attempt)
        observed <- (calls.get, held.get).tupled
        _ <- IO {
          assertEquals(result, Left(AnalyticsError.LakehouseLockTimeout))
          assertEquals(observed, (1, false))
        }
      } yield ()
    }
  }

  test("an acquisition failure other than mutex timeout remains fatal") {
    val failure = new IllegalStateException("mutex storage unavailable")
    for {
      calls <- Ref.of[IO, Int](0)
      lock = new AnalyticsLakehouseLock[IO] {
        override def resource(root: String): Resource[IO, Unit] = Resource.eval(IO.raiseError[Unit](failure))
      }
      result <- new AnalyticsStreamingMaintenance[IO](
        "hiring",
        lock,
        60.seconds,
        _ => calls.update(_ + 1)
      ).runOnce.attempt
      count <- calls.get
      _ <- IO { assertEquals(result, Left(failure)); assertEquals(count, 0) }
    } yield ()
  }

  test("cancelling owned maintenance releases the real local mutex before another owner enters") {
    AnalyticsLakehouseLock.processLocal[IO].use { underlying =>
      for {
        entered <- Deferred[IO, Unit]
        held <- Ref.of[IO, Boolean](false)
        lock = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] =
            underlying.resource(root) *> Resource.make(held.set(true))(_ => held.set(false))
        }
        maintenance = new AnalyticsStreamingMaintenance[IO](
          "hiring",
          lock,
          60.seconds,
          _ => entered.complete(()).void *> IO.never
        )
        _ <- maintenance.runOnce.background.use { _ => entered.get }
        owned <- held.get
        _ <- underlying.resource("hiring").use(_ => IO(assertEquals(owned, false))).timeout(1.second)
      } yield ()
    }
  }

  test("mutex finalizer timeout remains fatal after successful maintenance") {
    val lock = new AnalyticsLakehouseLock[IO] {
      override def resource(root: String): Resource[IO, Unit] =
        Resource.make(IO.unit)(_ => IO.raiseError[Unit](AnalyticsError.LakehouseLockTimeout))
    }
    new AnalyticsStreamingMaintenance[IO]("hiring", lock, 60.seconds, _ => IO.unit).runOnce.attempt.flatMap { result =>
      IO(assertEquals(result, Left(AnalyticsError.LakehouseLockTimeout)))
    }
  }
}
