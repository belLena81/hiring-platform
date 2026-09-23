package com.example.hiring.analytics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients}
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicReference

class AnalyticsBatchResourceSpec extends FunSuite {
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
      IO.blocking(MongoClients.create("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200"))
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
    intercept[IllegalStateException](mongo.listDatabaseNames().first())
  }
}
