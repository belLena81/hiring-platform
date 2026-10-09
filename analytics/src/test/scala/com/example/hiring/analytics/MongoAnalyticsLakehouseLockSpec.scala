package com.example.hiring.analytics

import cats.effect.{Clock, IO, Ref}
import cats.effect.std.Random
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import retry.{PolicyDecision, RetryStatus}
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
          val lock = new MongoAnalyticsLakehouseLock[IO](database, AnalyticsTestOperationalConfig.streams, clock)
          val resource = lock.resource("file:///tmp/analytics-lock-laziness")
          assert(resource != null)
          assertEquals(reads.get(), 0)
        }
      })
      .unsafeRunSync()
  }

  // The pre-retry-library schedule: 100ms doubled after each contended attempt and capped at 5s.
  private def legacyCeilings(attempts: Int): Vector[FiniteDuration] =
    Iterator.iterate(100.millis)(delay => (delay * 2).min(5.seconds)).take(attempts).toVector

  private def decisions(policy: retry.RetryPolicy[IO, Any], attempts: Int): IO[Vector[PolicyDecision]] =
    (0 until attempts).toVector.traverse { retries =>
      policy.decideNextRetry((), RetryStatus(retries, 0.seconds, None))
    }

  test("contention ceilings equal the legacy doubling schedule capped at five seconds") {
    decisions(MongoAnalyticsLakehouseLock.contentionCeilings[IO], 20)
      .map { actual =>
        assertEquals(actual, legacyCeilings(20).map(PolicyDecision.DelayAndRetry(_)))
        assertEquals(
          actual.take(8),
          Vector(100, 200, 400, 800, 1600, 3200, 5000, 5000).map(ms => PolicyDecision.DelayAndRetry(ms.millis))
        )
      }
      .unsafeRunSync()
  }

  test("contention waits are uniform full jitter between zero and the legacy ceiling") {
    (for {
      random <- Random.scalaUtilRandomSeedInt[IO](7)
      policy = MongoAnalyticsLakehouseLock.contentionBackoff[IO](random)
      draws <- (0 until 12).toVector.traverse { retries =>
        (0 until 200).toVector.traverse(_ => policy.decideNextRetry((), RetryStatus(retries, 0.seconds, None)))
      }
    } yield {
      draws.zip(legacyCeilings(12)).foreach { case (observed, ceiling) =>
        val delays = observed.collect { case PolicyDecision.DelayAndRetry(delay) => delay }
        assertEquals(delays.size, observed.size)
        assert(delays.forall(delay => delay >= Duration.Zero && delay <= ceiling))
        assert(delays.max > ceiling / 2, s"jitter never approached the $ceiling ceiling")
        assert(delays.min < ceiling / 2, s"jitter never approached zero below $ceiling")
      }
    }).unsafeRunSync()
  }

  test("acquisition gives up exactly when the monotonic deadline is reached") {
    (for {
      now <- Ref.of[IO, FiniteDuration](0.seconds)
      clock = new Clock[IO] {
        override val applicative: cats.Applicative[IO] = summon[cats.Applicative[IO]]
        override def monotonic: IO[FiniteDuration] = now.get
        override def realTime: IO[FiniteDuration] = IO.pure(0.seconds)
      }
      random <- Random.scalaUtilRandomSeedInt[IO](11)
      policy = MongoAnalyticsLakehouseLock.contentionPolicy[IO](clock, 2.minutes, random)
      status = RetryStatus(0, 0.seconds, None)
      before <- now.set(2.minutes - 1.nano) *> policy.decideNextRetry((), status)
      atDeadline <- now.set(2.minutes) *> policy.decideNextRetry((), status)
      after <- now.set(3.minutes) *> policy.decideNextRetry((), status)
    } yield {
      assert(before.isInstanceOf[PolicyDecision.DelayAndRetry])
      assertEquals(atDeadline, PolicyDecision.GiveUp)
      assertEquals(after, PolicyDecision.GiveUp)
    }).unsafeRunSync()
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
