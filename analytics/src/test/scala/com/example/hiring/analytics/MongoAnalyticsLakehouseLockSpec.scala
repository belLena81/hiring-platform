package com.example.hiring.analytics
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

import cats.effect.{Clock, IO}
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import munit.FunSuite
import mongo4cats.client.MongoClient

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

final class MongoAnalyticsLakehouseLockSpec extends FunSuite {
  test("constructing a lock resource does not read its injected clock") {
    val reads = new AtomicInteger(0)
    val clock = new Clock[IO] {
      override val applicative: cats.Applicative[IO] = summon[cats.Applicative[IO]]
      override def realTime: IO[FiniteDuration] = IO.pure(0.seconds)
      override def monotonic: IO[FiniteDuration] = IO.delay {
        reads.incrementAndGet()
        0.seconds
      }
    }
    MongoClient
      .fromConnectionString[IO]("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=1")
      .use(_.getDatabase("analytics_lock_laziness").flatMap { database =>
        IO {
          val lock = new MongoAnalyticsLakehouseLock(
            database,
            AnalyticsTestOperationalConfig.streams,
            Some(clock.realTimeInstant),
            Some(clock.monotonic)
          )
          val resource = lock.resource("file:///tmp/analytics-lock-laziness")
          assert(resource != null)
          assertEquals(reads.get(), 0)
        }
      })
      .unsafeRunSync()
  }

  test("lakehouse mutex keys canonicalize URI scheme, authority casing, and trailing slashes") {
    val first = MongoAnalyticsLakehouseLock.lockId("s3a://BUCKET-a/lakehouse/")
    val equivalent = MongoAnalyticsLakehouseLock.lockId("S3A://bucket-a/lakehouse")
    val different = MongoAnalyticsLakehouseLock.lockId("s3a://bucket-a/another-lakehouse")

    assertEquals(first, equivalent)
    assertNotEquals(first, different)
    assertEquals(
      MongoAnalyticsLakehouseLock.lockId("s3a://bucket-a"),
      MongoAnalyticsLakehouseLock.lockId("s3a://bucket-a/")
    )
    val originalLocale = java.util.Locale.getDefault
    try {
      java.util.Locale.setDefault(new java.util.Locale("tr", "TR"))
      assertEquals(
        MongoAnalyticsLakehouseLock.lockId("HDFS://NAMENODE/lakehouse"),
        MongoAnalyticsLakehouseLock.lockId("hdfs://namenode/lakehouse")
      )
    } finally java.util.Locale.setDefault(originalLocale)
  }

  test("lakehouse mutex keys reject URI credentials and query strings") {
    assert(MongoAnalyticsLakehouseLock.lockId("s3a://user:secret@bucket/lakehouse").isLeft)
    assert(MongoAnalyticsLakehouseLock.lockId("s3a://bucket/lakehouse?token=secret").isLeft)
  }
}
