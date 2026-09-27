package com.example.hiring.analytics

import com.example.hiring.analytics.batch.*
import com.example.hiring.analytics.erasure.*
import com.example.hiring.analytics.mongo.*

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients}
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

class AnalyticsBatchResourceSpec extends FunSuite {
  test("process-local test lock serializes same-process access") {
    val root = java.nio.file.Files.createTempDirectory("analytics-lock").toUri.toString
    val result = (for {
      firstEntered <- Deferred[IO, Unit]
      releaseFirst <- Deferred[IO, Unit]
      secondEntered <- Deferred[IO, Unit]
      first <- AnalyticsLakehouseLock.resource(root).use(_ => firstEntered.complete(()) *> releaseFirst.get).start
      _ <- firstEntered.get
      second <- AnalyticsLakehouseLock.resource(root).use(_ => secondEntered.complete(())).start
      beforeRelease <- secondEntered.tryGet
      _ <- releaseFirst.complete(())
      _ <- secondEntered.get.timeout(5.seconds)
      _ <- first.joinWithNever
      _ <- second.joinWithNever
    } yield beforeRelease).unsafeRunSync()

    assertEquals(result, None)
  }

  test("batch entry point closes Spark and Mongo after a failed run") {
    val acquired = new AtomicReference[Option[(SparkSession, MongoClient)]](None)
    val resources = HiringAnalyticsBatchMain.managedResources(
      IO.blocking(
        org.apache.spark.sql.classic.SparkSession
          .builder()
          .master("local[1]")
          .appName("AnalyticsBatchResourceSpec")
          .config("spark.ui.enabled", "false")
          .getOrCreate()
      ),
      IO.delay(MongoClients.create("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200"))
    )

    val result = resources
      .use { pair =>
        IO.delay(acquired.set(Some(pair))) *> IO.raiseError[Unit](new IllegalStateException("injected run failure"))
      }
      .attempt
      .unsafeRunSync()

    assert(result.isLeft)
    val (spark, mongo) = acquired.get().getOrElse(fail("resources were not acquired"))
    assert(spark.sparkContext.isStopped)
    assert(
      MongoPublisherStream.stream(mongo.listDatabaseNames()).compile.drain.attempt.unsafeRunSync().isLeft
    )
  }
}
