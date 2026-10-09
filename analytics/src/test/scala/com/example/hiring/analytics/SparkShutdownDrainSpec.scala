package com.example.hiring.analytics

import cats.effect.{IO, Ref}
import com.example.hiring.analytics.adapter.spark.SparkShutdownDrain
import com.example.hiring.analytics.adapter.spark.SparkShutdownDrain.Attempt
import com.example.hiring.analytics.app.AppModule
import mongo4cats.client.MongoClient
import org.apache.spark.scheduler.*
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import munit.CatsEffectSuite
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration.*

final class SparkShutdownDrainSpec extends CatsEffectSuite {
  private def control(tracker: SparkShutdownDrain.Tracker): Unit = {
    tracker.begin()
    tracker.controlStarted(9, Set(90))
    tracker.taskStarted(Attempt(90, 0, 900L))
    tracker.taskEnded(Attempt(90, 0, 900L))
    tracker.jobEnded(9, succeeded = true)
  }

  test("control barrier cannot forget a cancelled physical task that outlives its job") {
    IO {
      val tracker = new SparkShutdownDrain.Tracker(8)
      tracker.taskStarted(Attempt(1, 0, 10L))
      control(tracker)
      assertEquals(tracker.snapshot()._1, Right(false))
      tracker.taskEnded(Attempt(1, 0, 10L))
      assertEquals(tracker.snapshot()._1, Right(true))
    }
  }

  test("all task attempts including a later start remain owned until their terminal event") {
    IO {
      val tracker = new SparkShutdownDrain.Tracker(8)
      control(tracker)
      tracker.taskStarted(Attempt(1, 1, 11L))
      tracker.taskStarted(Attempt(1, 2, 12L))
      tracker.taskEnded(Attempt(1, 1, 11L))
      assertEquals(tracker.snapshot()._1, Right(false))
      tracker.taskEnded(Attempt(1, 2, 12L))
      assertEquals(tracker.snapshot()._1, Right(true))
    }
  }

  test("foreign job completion cannot release the exact control barrier") {
    IO {
      val tracker = new SparkShutdownDrain.Tracker(8)
      tracker.begin()
      tracker.controlStarted(9, Set(90))
      tracker.taskStarted(Attempt(90, 0, 900L))
      tracker.taskEnded(Attempt(90, 0, 900L))
      tracker.jobEnded(8, succeeded = true)
      assertEquals(tracker.snapshot()._1, Right(false))
      tracker.jobEnded(9, succeeded = true)
      assertEquals(tracker.snapshot()._1, Right(true))
    }
  }

  test("duplicate starts unmatched ends and accounting bounds fail closed") {
    IO {
      val duplicate = new SparkShutdownDrain.Tracker(8)
      duplicate.taskStarted(Attempt(1, 0, 10L))
      duplicate.taskStarted(Attempt(1, 0, 10L))
      assert(duplicate.snapshot()._1.isLeft)
      val unmatched = new SparkShutdownDrain.Tracker(8)
      unmatched.taskEnded(Attempt(1, 0, 10L))
      assert(unmatched.snapshot()._1.isLeft)
      val bounded = new SparkShutdownDrain.Tracker(1)
      bounded.taskStarted(Attempt(1, 0, 10L))
      bounded.taskStarted(Attempt(1, 0, 11L))
      assert(bounded.snapshot()._1.isLeft)
    }
  }

  test("failed duplicate or malformed control jobs and retries cannot claim successful drain") {
    IO {
      val failed = new SparkShutdownDrain.Tracker(8)
      failed.begin(); failed.controlStarted(9, Set(90)); failed.jobEnded(9, succeeded = false)
      assert(failed.snapshot()._1.isLeft)
      val malformed = new SparkShutdownDrain.Tracker(8)
      malformed.begin(); malformed.controlStarted(9, Set(90, 91))
      assert(malformed.snapshot()._1.isLeft)
      val duplicate = new SparkShutdownDrain.Tracker(8)
      duplicate.begin(); duplicate.controlStarted(9, Set(90)); duplicate.controlStarted(10, Set(91))
      assert(duplicate.snapshot()._1.isLeft)
      val retry = new SparkShutdownDrain.Tracker(8)
      retry.begin(); retry.controlStarted(9, Set(90))
      retry.taskStarted(Attempt(90, 0, 900L)); retry.taskEnded(Attempt(90, 0, 900L))
      retry.taskStarted(Attempt(90, 1, 901L))
      assert(retry.snapshot()._1.isLeft)
    }
  }

  test("task lifecycle notifies a waiter without losing the transition") {
    for {
      tracker <- IO(new SparkShutdownDrain.Tracker(8))
      snapshot <- IO(tracker.snapshot())
      waiter <- IO.fromCompletableFuture(IO.pure(snapshot._2)).start
      _ <- IO(control(tracker))
      _ <- waiter.joinWithNever
      _ <- IO(assertEquals(tracker.snapshot()._1, Right(true)))
    } yield ()
  }

  test("drain failure still stops context and removes the listener in order") {
    for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      drainError = new IllegalStateException("drain")
      result <- SparkShutdownDrain
        .close(
          calls.update(_ :+ "drain") *> IO.raiseError[Unit](drainError),
          calls.update(_ :+ "stop"),
          calls.update(_ :+ "remove")
        )
        .attempt
      observed <- calls.get
      _ <- IO(assertEquals(observed, Vector("drain", "stop", "remove")))
      _ <- IO(assertEquals(result, Left(drainError)))
    } yield ()
  }

  test("stop remains primary and combines drain and listener-removal failures") {
    val drained = new IllegalStateException("drain")
    val stopped = new IllegalStateException("stop")
    val removed = new IllegalStateException("remove")
    SparkShutdownDrain.close[IO](IO.raiseError(drained), IO.raiseError(stopped), IO.raiseError(removed)).attempt.map {
      result =>
        assertEquals(result, Left(stopped))
        assertEquals(stopped.getSuppressed.toVector, Vector(drained, removed))
    }
  }

  test("control action and listener drain share a bounded deadline and preserve action errors") {
    val tracker = new SparkShutdownDrain.Tracker(8)
    for {
      expired <- SparkShutdownDrain.awaitKnownTasks[IO](tracker, IO.never, 10.millis).attempt
      _ <- IO(assert(expired.isLeft))
      expected = new IllegalStateException("control failed")
      failed <- SparkShutdownDrain
        .awaitKnownTasks[IO](new SparkShutdownDrain.Tracker(8), IO.raiseError(expected), 1.second)
        .attempt
      _ <- IO(assertEquals(failed, Left(expected)))
      missing <- SparkShutdownDrain.awaitKnownTasks[IO](new SparkShutdownDrain.Tracker(8), IO.unit, 10.millis).attempt
      _ <- IO(assert(missing.isLeft))
    } yield ()
  }

  test("native one-task listener barrier drains and restores thread properties on success and error") {
    val original = new AtomicReference[Option[SparkSession]](None)
    val jobs = new AtomicInteger(0)
    val tasks = new AtomicInteger(0)
    AppModule
      .managedSparkMongo[IO](
        execution =>
          execution {
            org.apache.spark.sql.classic.SparkSession
              .builder()
              .master("local[1]")
              .appName("SparkShutdownDrainSpec")
              .config("spark.ui.enabled", "false")
              .getOrCreate(): SparkSession
          },
        MongoClient.fromConnectionString[IO]("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200")
      )
      .use { case (session, _, execution) =>
        execution {
          original.set(Some(session))
          val context = session.sparkContext
          context.addSparkListener(new SparkListener {
            override def onJobStart(event: SparkListenerJobStart): Unit =
              if (Option(event.properties).exists(_.getProperty(SparkShutdownDrain.BarrierProperty) != null))
                jobs.incrementAndGet(): Unit
            override def onTaskStart(event: SparkListenerTaskStart): Unit = tasks.incrementAndGet(): Unit
          })
          context.setLocalProperty(SparkShutdownDrain.BarrierProperty, "parent")
          assertEquals(
            SparkShutdownDrain.withBarrierProperty(context, "test") {
              context.getLocalProperty(SparkShutdownDrain.BarrierProperty)
            },
            "test"
          )
          assertEquals(context.getLocalProperty(SparkShutdownDrain.BarrierProperty), "parent")
          intercept[IllegalStateException] {
            SparkShutdownDrain.withBarrierProperty(context, "test") {
              throw new IllegalStateException("injected property-body failure")
            }
          }
          assertEquals(context.getLocalProperty(SparkShutdownDrain.BarrierProperty), "parent")
          context.setLocalProperty(SparkShutdownDrain.BarrierProperty, null)
        }
      } *> IO {
      assertEquals(jobs.get(), 1)
      assertEquals(tasks.get(), 1)
      assert(original.get().exists(_.sparkContext.isStopped))
    }
  }

  test("actual masked close owns timeout cancellation before stop and listener removal") {
    for {
      cancelled <- Ref.of[IO, Boolean](false)
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- IO.uncancelable { _ =>
        SparkShutdownDrain.close[IO](
          SparkShutdownDrain.awaitKnownTasks(
            new SparkShutdownDrain.Tracker(8),
            IO.never[Unit].onCancel(cancelled.set(true)),
            10.millis
          ),
          cancelled.get.flatMap(value => IO(assert(value))) *> calls.update(_ :+ "stop"),
          calls.update(_ :+ "remove")
        )
      }.attempt
      observed <- calls.get
      _ <- IO(assert(result.isLeft))
      _ <- IO(assertEquals(observed, Vector("stop", "remove")))
      missing <- IO.uncancelable { _ =>
        SparkShutdownDrain.close[IO](
          SparkShutdownDrain.awaitKnownTasks(new SparkShutdownDrain.Tracker(8), IO.unit, 10.millis),
          calls.update(_ :+ "missing-stop"),
          calls.update(_ :+ "missing-remove")
        )
      }.attempt
      finalCalls <- calls.get
      _ <- IO(assert(missing.isLeft))
      _ <- IO(assertEquals(finalCalls, Vector("stop", "remove", "missing-stop", "missing-remove")))
    } yield ()
  }

}
