package com.example.hiring.analytics

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Mutex
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
import munit.CatsEffectSuite
import org.apache.spark.sql.SparkSession

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class SparkDriverExecutionSpec extends CatsEffectSuite {
  private def await(latch: CountDownLatch): IO[Unit] =
    IO.blocking(latch.await(10L, TimeUnit.SECONDS)).flatMap(ready => IO(assert(ready, "worker signal timed out")))

  test("repeated, retried and concurrent Spark effects own fresh job groups and observe attachment at evaluation") {
    SparkBlockingExecution.resource[IO].use { execution =>
      Resource
        .make(execution {
          SparkSession
            .builder()
            .master("local[1]")
            .appName("SparkDriverExecutionSpec")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        })(spark => execution(spark.stop()))
        .use { spark =>
          val context = spark.sparkContext
          val group = execution(Option(context.getLocalProperty("spark.jobGroup.id")))
          val attempts = new AtomicInteger(0)
          for {
            _ <- execution.attachSparkContext(context)
            beforeAttachment <- group
            attached = execution(Option(context.getLocalProperty("spark.jobGroup.id")))
            retried = execution {
              if (attempts.incrementAndGet() == 1) throw new IllegalStateException("first attempt fails")
              Option(context.getLocalProperty("spark.jobGroup.id"))
            }
            first <- attached
            second <- attached
            retry <- retried.handleErrorWith(_ => retried)
            concurrent <- (attached, attached).parTupled
            after <- execution.blocking(Option(context.getLocalProperty("spark.jobGroup.id")))
          } yield {
            val groups = Vector(beforeAttachment, first, second, retry, concurrent._1, concurrent._2)
            assert(groups.forall(_.nonEmpty), "even effects built before attachment need Spark job groups")
            assertEquals(groups.flatten.distinct.size, groups.size)
            assertEquals(attempts.get(), 2)
            assertEquals(after, None)
          }
        }
    }
  }

  test("cancelled running driver work keeps its resource and lock until interruption-resistant work exits") {
    SparkBlockingExecution.resource[IO].use { execution =>
      val entered = new CountDownLatch(1)
      val interrupted = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val exited = new AtomicBoolean(false)
      val work = execution.blocking {
        entered.countDown()
        var done = false
        while (!done) {
          try {
            release.await()
            done = true
          } catch { case _: InterruptedException => interrupted.countDown() }
        }
        exited.set(true)
      }
      for {
        mutex <- Mutex[IO]
        finalized <- Deferred[IO, Boolean]
        secondEntered <- Deferred[IO, Boolean]
        result <- Resource
          .make(
            mutex.lock
              .flatMap(_ => Resource.make(IO.unit)(_ => IO(exited.get()).flatMap(finalized.complete).void))
              .use(_ => work)
              .start
          )(fiber => IO(release.countDown()) *> fiber.cancel)
          .use { first =>
            for {
              _ <- await(entered)
              outcome <- Resource.make(first.cancel.start)(fiber => IO(release.countDown()) *> fiber.cancel).use {
                cancellation =>
                  for {
                    _ <- await(interrupted)
                    observed <- mutex.lock
                      .use(_ => IO(exited.get()).flatMap(secondEntered.complete).void)
                      .background
                      .use { second =>
                        for {
                          // A bounded observation checks non-completion; latches control worker progress.
                          early <- finalized.get.map(Option(_)).timeoutTo(200.millis, IO.pure(None))
                          lockBeforeExit <- secondEntered.tryGet
                          _ <- IO(release.countDown())
                          _ <- cancellation.joinWithNever
                          _ <- second.flatMap(_.embedNever)
                          finalizerSawExit <- finalized.get
                          secondSawExit <- secondEntered.get
                        } yield (early, lockBeforeExit, finalizerSawExit, secondSawExit)
                      }
                  } yield observed
              }
            } yield outcome
          }
      } yield {
        assertEquals(result._1, None)
        assertEquals(result._2, None)
        assert(result._3, "resource finalization preceded worker exit")
        assert(result._4, "another lock owner entered before worker exit")
      }
    }
  }

  test("an interrupted driver call reports its error and leaves the executor available") {
    SparkBlockingExecution.resource[IO].use { execution =>
      val interrupted = new InterruptedException("driver interrupted")
      for {
        result <- execution.blocking[Unit](throw interrupted).attempt.timeout(2.seconds)
        next <- execution.blocking("available").timeout(2.seconds)
      } yield {
        assert(result.left.exists(_ eq interrupted))
        assertEquals(next, "available")
      }
    }
  }

  test("queued cancellation completes without waiting for an occupied executor and never runs its work") {
    Resource
      .make(IO(java.util.concurrent.Executors.newSingleThreadExecutor()))(executor => IO(executor.shutdown()))
      .use { executor =>
        val entered = new CountDownLatch(1)
        val release = new CountDownLatch(1)
        val queued = new CountDownLatch(1)
        val ran = new AtomicBoolean(false)
        // The queue signal occurs after the executor accepted the task, before cancellation is requested.
        val observing = new ExecutionContext {
          override def execute(task: Runnable): Unit = {
            executor.execute(task)
            queued.countDown()
          }
          override def reportFailure(error: Throwable): Unit = ExecutionContext.global.reportFailure(error)
        }
        val execution = SparkBlockingExecution.forTests[IO](observing)
        Resource
          .make(IO {
            executor.execute(() => {
              entered.countDown()
              release.await()
            })
          })(_ => IO(release.countDown()))
          .use { _ =>
            for {
              _ <- await(entered)
              fiber <- execution.blocking(ran.set(true)).start
              _ <- await(queued)
              _ <- fiber.cancel.timeout(2.seconds)
              _ <- IO(release.countDown())
              _ <- execution.blocking(())
            } yield assert(!ran.get(), "cancelled queued work ran after the executor became available")
          }
      }
  }
}
