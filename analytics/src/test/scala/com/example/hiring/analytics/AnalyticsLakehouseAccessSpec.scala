package com.example.hiring.analytics

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{AnalyticsLakehouseLock, AnalyticsStreamingRegistry}
import com.example.hiring.analytics.service.streaming.AnalyticsStreamingMaintenance
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class AnalyticsLakehouseAccessSpec extends CatsEffectSuite {
  test("a lifetime stream-owner root does not block callback or maintenance ownership for the lakehouse root") {
    TestControl.executeEmbed {
      AnalyticsTestLakehouseLocks.processLocal[IO].use { persistent =>
        AnalyticsLakehouseLock.serialized(persistent).use { lock =>
          val lakehouseRoot = "file:///tmp/hiring-access-fixture"
          IO.fromEither(AnalyticsStreamingRegistry.ownerLockRoot(lakehouseRoot)).flatMap { ownerRoot =>
            lock.resource(ownerRoot).use { _ =>
              for {
                calls <- Ref.of[IO, Int](0)
                _ <- lock.resource(lakehouseRoot).use(_ => calls.update(_ + 1)).timeout(1.second)
                maintenance = new AnalyticsStreamingMaintenance[IO](
                  lakehouseRoot,
                  lock,
                  60.seconds,
                  _ => calls.update(_ + 1)
                )
                _ <- maintenance.resource.use(_ => IO.sleep(65.seconds))
                count <- calls.get
                _ <- IO(assertEquals(count, 2))
              } yield ()
            }
          }
        }
      }
    }
  }

  test("queued maintenance precedes an immediately reacquiring callback under sustained contention") {
    TestControl.executeEmbed {
      for {
        events <- Ref.of[IO, Vector[String]](Vector.empty)
        observations <- Ref.of[IO, Vector[AnalyticsStreamingMaintenance.Observation]](Vector.empty)
        _ <- AnalyticsTestLakehouseLocks.processLocal[IO].use { persistent =>
          AnalyticsLakehouseLock.serialized(persistent).use { lock =>
            val maintenance = new AnalyticsStreamingMaintenance[IO](
              "file:///tmp/hiring-access-fifo",
              lock,
              60.seconds,
              _ => events.update(_ :+ "maintenance") *> IO.sleep(10.seconds),
              Some(value => observations.update(_ :+ value))
            )
            val callbacks = lock
              .resource("file:///tmp/hiring-access-fifo")
              .use { _ =>
                events.update(_ :+ "callback") *> IO.sleep(90.seconds)
              }
              .foreverM
            IO.fromEither(AnalyticsStreamingRegistry.ownerLockRoot("file:///tmp/hiring-access-fifo")).flatMap {
              ownerRoot =>
                lock.resource(ownerRoot).use { _ =>
                  callbacks.background.use { _ => maintenance.resource.use(_ => IO.sleep(380.seconds)) }
                }
            }
          }
        }
        actual <- events.get
        progress <- observations.get
        _ <- IO {
          assertEquals(actual.take(5), Vector("callback", "maintenance", "callback", "maintenance", "callback"))
          assert(progress.count(_.outcome.contains(AnalyticsStreamingMaintenance.TickOutcome.Succeeded)) >= 3)
          assert(progress.forall(_.maximumElapsedSinceSuccess < 300.seconds))
        }
      } yield ()
    }
  }

  test("cancelling a queued owner never acquires the persistent mutex and removes it from the FIFO queue") {
    TestControl.executeEmbed {
      for {
        acquisitions <- Ref.of[IO, Int](0)
        queued <- Deferred[IO, Unit]
        delegate = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] = Resource.eval(acquisitions.update(_ + 1))
        }
        _ <- AnalyticsLakehouseLock.serialized(delegate).use { lock =>
          lock.resource("hiring").use { _ =>
            (queued.complete(()).void *> lock.resource("hiring").use(_ => IO.unit)).background.use { _ =>
              queued.get *> IO.sleep(1.second)
            }
          } *> lock.resource("hiring").use(_ => IO.unit)
        }
        count <- acquisitions.get
        _ <- IO(assertEquals(count, 2))
      } yield ()
    }
  }

  test("active cancellation finishes persistent release before the next local permit owner enters") {
    TestControl.executeEmbed {
      for {
        events <- Ref.of[IO, Vector[String]](Vector.empty)
        entered <- Deferred[IO, Unit]
        delegate = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] =
            Resource.make(events.update(_ :+ "acquire"))(_ => IO.sleep(2.seconds) *> events.update(_ :+ "release"))
        }
        _ <- AnalyticsLakehouseLock.serialized(delegate).use { lock =>
          Resource
            .make(lock.resource("hiring").use(_ => entered.complete(()).void *> IO.never[Unit]).start)(_.cancel)
            .use { first =>
              entered.get *> Resource
                .make(lock.resource("hiring").use(_ => events.update(_ :+ "next")).start)(_.cancel)
                .use { next =>
                  // The second contender queues while the first is active, before its slow finalizer starts.
                  IO.sleep(1.second) *> first.cancel *> next.joinWithNever
                }
            }
        }
        actual <- events.get
        _ <- IO(assertEquals(actual, Vector("acquire", "release", "acquire", "next", "release")))
      } yield ()
    }
  }

  test("persistent acquisition timeout retains its original duration and returns the local permit") {
    TestControl.executeEmbed {
      for {
        attempts <- Ref.of[IO, Int](0)
        delegate = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] =
            Resource.eval(attempts.getAndUpdate(_ + 1)).flatMap {
              case 0 => Resource.eval(IO.sleep(120.seconds) *> IO.raiseError[Unit](AnalyticsError.LakehouseLockTimeout))
              case _ => Resource.unit[IO]
            }
        }
        _ <- AnalyticsLakehouseLock.serialized(delegate).use { lock =>
          for {
            started <- IO.monotonic
            result <- lock.resource("hiring").use(_ => IO.unit).attempt
            ended <- IO.monotonic
            _ <- IO {
              assertEquals(result, Left(AnalyticsError.LakehouseLockTimeout))
              assertEquals(ended - started, 120.seconds)
            }
            _ <- lock.resource("hiring").use(_ => IO.unit)
          } yield ()
        }
      } yield ()
    }
  }

  test("waiting locally does not consume the persistent mutex acquisition timeout") {
    TestControl.executeEmbed {
      for {
        entered <- Deferred[IO, Unit]
        attempts <- Ref.of[IO, Int](0)
        delegate = new AnalyticsLakehouseLock[IO] {
          override def resource(root: String): Resource[IO, Unit] =
            Resource.eval(attempts.getAndUpdate(_ + 1)).flatMap {
              case 0 => Resource.unit[IO]
              case _ => Resource.eval(IO.sleep(120.seconds) *> IO.raiseError[Unit](AnalyticsError.LakehouseLockTimeout))
            }
        }
        _ <- AnalyticsLakehouseLock.serialized(delegate).use { lock =>
          lock.resource("hiring").use(_ => entered.complete(()).void *> IO.sleep(90.seconds)).background.use { _ =>
            entered.get *> IO.monotonic.flatMap { started =>
              lock.resource("hiring").use(_ => IO.unit).attempt.flatMap { result =>
                IO.monotonic.flatMap { ended =>
                  IO {
                    assertEquals(result, Left(AnalyticsError.LakehouseLockTimeout))
                    assertEquals(ended - started, 210.seconds)
                  }
                }
              }
            }
          }
        }
      } yield ()
    }
  }

  test("acquisition body and persistent release failures preserve errors and free the local permit") {
    val failure = new IllegalStateException("owned operation failed")
    TestControl.executeEmbed {
      Vector("acquire", "body", "release").traverse_ { failedAt =>
        for {
          attempts <- Ref.of[IO, Int](0)
          delegate = new AnalyticsLakehouseLock[IO] {
            override def resource(root: String): Resource[IO, Unit] =
              Resource.eval(attempts.getAndUpdate(_ + 1)).flatMap { attempt =>
                Resource.make(if (attempt == 0 && failedAt == "acquire") IO.raiseError[Unit](failure) else IO.unit)(_ =>
                  if (attempt == 0 && failedAt == "release") IO.raiseError[Unit](failure) else IO.unit
                )
              }
          }
          _ <- AnalyticsLakehouseLock.serialized(delegate).use { lock =>
            for {
              result <- lock
                .resource("hiring")
                .use(_ => if (failedAt == "body") IO.raiseError[Unit](failure) else IO.unit)
                .attempt
              _ <- IO(assertEquals(result, Left(failure)))
              _ <- lock.resource("hiring").use(_ => IO.unit).timeout(1.second)
            } yield ()
          }
        } yield ()
      }
    }
  }

  test("cancelling persistent acquisition frees the local permit before another attempt") {
    for {
      entered <- Deferred[IO, Unit]
      attempts <- Ref.of[IO, Int](0)
      delegate = new AnalyticsLakehouseLock[IO] {
        override def resource(root: String): Resource[IO, Unit] =
          Resource.eval(attempts.getAndUpdate(_ + 1)).flatMap {
            case 0 => Resource.eval(entered.complete(()).void *> IO.never[Unit])
            case _ => Resource.unit[IO]
          }
      }
      _ <- AnalyticsLakehouseLock.serialized(delegate).use { lock =>
        lock.resource("hiring").use(_ => IO.unit).background.use(_ => entered.get) *>
          lock.resource("hiring").use(_ => IO.unit).timeout(1.second)
      }
      count <- attempts.get
      _ <- IO(assertEquals(count, 2))
    } yield ()
  }

  test("each runtime allocation has its own local access permit") {
    val delegate = new AnalyticsLakehouseLock[IO] {
      override def resource(root: String): Resource[IO, Unit] = Resource.unit[IO]
    }
    val resource = AnalyticsLakehouseLock.serialized(delegate)
    resource.use { first =>
      resource.use { second =>
        first.resource("hiring").use(_ => second.resource("hiring").use(_ => IO.unit).timeout(1.second))
      }
    }
  }
}
