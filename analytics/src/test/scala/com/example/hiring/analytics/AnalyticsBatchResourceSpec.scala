package com.example.hiring.analytics
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

class AnalyticsBatchResourceSpec extends FunSuite {
  test("Spark driver operations run on the dedicated execution context") {
    val result = com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
      .resource[IO]
      .use { execution =>
        for {
          first <- execution(Thread.currentThread().getName)
          second <- execution(Thread.currentThread().getName)
          expected <- execution.either(Right("expected result"))
          rejected <- execution
            .either[Unit](Left(AnalyticsError.InvalidConfiguration("expected rejection")))
            .attempt
          failed <- execution(throw new IllegalStateException("injected Spark failure")).attempt
        } yield (first, second, expected, rejected, failed)
      }
      .unsafeRunSync()

    assertEquals(result._1, "analytics-spark-driver")
    assertEquals(result._2, "analytics-spark-driver")
    assertEquals(result._3, "expected result")
    assert(result._4.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
    assert(result._5.left.exists(_.isInstanceOf[IllegalStateException]))
  }

  test("process-local test lock serializes same-process access") {
    val root = java.nio.file.Files.createTempDirectory("analytics-lock").toUri.toString
    val result = AnalyticsLakehouseLock
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
    val cancelledWaiterDidNotEnter = AnalyticsLakehouseLock
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
