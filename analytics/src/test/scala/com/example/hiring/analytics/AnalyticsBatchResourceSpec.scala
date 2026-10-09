package com.example.hiring.analytics
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.errors.*

import cats.effect.{Deferred, IO, Resource}
import cats.effect.unsafe.implicits.global
import mongo4cats.client.MongoClient
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import scala.concurrent.duration.*

class AnalyticsBatchResourceSpec extends FunSuite {
  test("Spark driver operations run on the dedicated execution context") {
    val result = com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
      .resource[IO]
      .use { execution =>
        for {
          first <- execution(Thread.currentThread().getName)
          second <- execution(Thread.currentThread().getName)
          kafkaDriver <- execution.blocking(Thread.currentThread().getName)
          expected <- execution.either(Right("expected result"))
          rejected <- execution
            .either[Unit](Left(AnalyticsError.InvalidConfiguration("expected rejection")))
            .attempt
          failed <- execution(throw new IllegalStateException("injected Spark failure")).attempt
        } yield (first, second, kafkaDriver, expected, rejected, failed)
      }
      .unsafeRunSync()

    assertEquals(result._1, "analytics-spark-driver")
    assertEquals(result._2, "analytics-spark-driver")
    assertEquals(result._3, "analytics-spark-driver")
    assertEquals(result._4, "expected result")
    assert(result._5.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
    assert(result._6.left.exists(_.isInstanceOf[IllegalStateException]))
  }

  test("canceling a Spark action cancels its job group and releases the driver executor") {
    val result = com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
      .resource[IO]
      .use { execution =>
        Resource
          .make(
            execution {
              org.apache.spark.sql.classic.SparkSession
                .builder()
                .master("local[2]")
                .appName("AnalyticsSparkCancellationSpec")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            }
          )(spark => execution(spark.stop()))
          .use { spark =>
            val jobStarted = new CountDownLatch(1)
            val listener = new SparkListener {
              override def onJobStart(event: SparkListenerJobStart): Unit = jobStarted.countDown()
            }
            val context = spark.sparkContext
            for {
              _ <- execution.attachSparkContext(context)
              successfulGroup <- execution(Option(context.getLocalProperty("spark.jobGroup.id")))
              groupAfterSuccess <- execution.blocking(Option(context.getLocalProperty("spark.jobGroup.id")))
              expectedFailure = new IllegalStateException("injected Spark failure")
              failed <- execution[Unit](throw expectedFailure).attempt
              groupAfterFailure <- execution.blocking(Option(context.getLocalProperty("spark.jobGroup.id")))
              _ <- IO.delay(context.addSparkListener(listener))
              fiber <- execution {
                context
                  .parallelize(Seq(1), 1)
                  .mapPartitions { values =>
                    values.map { value =>
                      try Thread.sleep(TimeUnit.DAYS.toMillis(1L))
                      catch { case _: InterruptedException => () }
                      value
                    }
                  }
                  .count()
              }.start
              started <- IO.blocking(jobStarted.await(10L, TimeUnit.SECONDS))
              _ <- IO(assert(started, "Spark action did not start"))
              _ <- fiber.cancel
              groupAfterCancellation <- execution.blocking(Option(context.getLocalProperty("spark.jobGroup.id")))
              next <- execution("driver executor available").timeout(10.seconds)
              _ <- IO.delay(context.removeSparkListener(listener))
            } yield (
              successfulGroup,
              groupAfterSuccess,
              failed.swap.toOption.exists(_ eq expectedFailure),
              groupAfterFailure,
              groupAfterCancellation,
              next
            )
          }
      }
      .unsafeRunSync()

    assert(result._1.exists(_.nonEmpty), "Spark work should receive a job group")
    assertEquals(result._2, None)
    assert(result._3, "the injected Spark failure should be preserved")
    assertEquals(result._4, None)
    assertEquals(result._5, None)
    assertEquals(result._6, "driver executor available")
  }

  test("process-local test lock serializes same-process access") {
    val root = java.nio.file.Files.createTempDirectory("analytics-lock").toUri.toString
    val result = AnalyticsTestLakehouseLocks
      .processLocal[IO]
      .use { lock =>
        for {
          firstEntered <- Deferred[IO, Unit]
          releaseFirst <- Deferred[IO, Unit]
          secondEntered <- Deferred[IO, Unit]
          first <- lock.resource(root).use(_ => firstEntered.complete(()) *> releaseFirst.get).start
          _ <- firstEntered.get
          second <- lock.resource(root).use(_ => secondEntered.complete(())).start
          beforeRelease <- secondEntered.tryGet
          _ <- releaseFirst.complete(())
          _ <- secondEntered.get.timeout(5.seconds)
          _ <- first.joinWithNever
          _ <- second.joinWithNever
        } yield beforeRelease
      }
      .unsafeRunSync()

    assertEquals(result, None)
  }

  test("waiting for the process-local test lock is cancellable") {
    val root = java.nio.file.Files.createTempDirectory("analytics-lock-cancel").toUri.toString
    val cancelledWaiterDidNotEnter = AnalyticsTestLakehouseLocks
      .processLocal[IO]
      .use { lock =>
        for {
          releaseHolder <- Deferred[IO, Unit]
          holderEntered <- Deferred[IO, Unit]
          waiterEntered <- Deferred[IO, Unit]
          holder <- lock.resource(root).use(_ => holderEntered.complete(()) *> releaseHolder.get).start
          _ <- holderEntered.get
          waiter <- lock.resource(root).use(_ => waiterEntered.complete(())).start
          _ <- waiter.cancel
          _ <- releaseHolder.complete(())
          _ <- holder.joinWithNever
          entered <- waiterEntered.tryGet
        } yield entered.isEmpty
      }
      .unsafeRunSync()

    assert(cancelledWaiterDidNotEnter)
  }

  test("application resources close Spark and Mongo after a failed run") {
    val acquired = new AtomicReference[
      Option[(SparkSession, MongoClient[IO], com.example.hiring.analytics.adapter.spark.SparkBlockingExecution[IO])]
    ](None)
    val resources = AppModule.managedSparkMongo[IO](
      execution =>
        execution {
          org.apache.spark.sql.classic.SparkSession
            .builder()
            .master("local[1]")
            .appName("AnalyticsBatchResourceSpec")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        },
      MongoClient.fromConnectionString[IO]("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200")
    )

    val result = resources
      .use { pair =>
        IO.delay(acquired.set(Some(pair))) *> IO.raiseError[Unit](new IllegalStateException("injected run failure"))
      }
      .attempt
      .unsafeRunSync()

    assert(result.isLeft)
    val (spark, mongo, sparkExecution) = acquired.get().getOrElse(fail("resources were not acquired"))
    assert(spark.sparkContext.isStopped)
    assert(sparkExecution.isShutdown)
    assert(
      AnalyticsTestOperationalConfig.streams
        .drain[IO, String](mongo.underlying.listDatabaseNames())
        .attempt
        .unsafeRunSync()
        .isLeft
    )
  }

  test("Spark driver execution context is released when Spark startup fails") {
    val acquired =
      new AtomicReference[Option[com.example.hiring.analytics.adapter.spark.SparkBlockingExecution[IO]]](None)
    val resources = AppModule.managedSparkMongo[IO](
      execution =>
        IO.delay(acquired.set(Some(execution))) *> IO.raiseError(new IllegalStateException("startup failed")),
      MongoClient.fromConnectionString[IO]("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200")
    )

    val result = resources.use(_ => IO.unit).attempt.unsafeRunSync()

    assert(result.isLeft)
    assert(acquired.get().exists(_.isShutdown))
  }
}
