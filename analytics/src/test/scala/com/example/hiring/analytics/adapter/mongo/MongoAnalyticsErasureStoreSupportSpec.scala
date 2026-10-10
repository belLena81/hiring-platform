package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import mongo4cats.errors.MongoJsonParsingException
import munit.FunSuite

final class MongoAnalyticsErasureStoreSupportSpec extends FunSuite {
  private val parseFailure = MongoJsonParsingException("bad document", None)

  private object Support extends MongoAnalyticsErasureStoreSupport[IO](null) {
    def claim(work: IO[Int]): IO[Int] = mongo(work)
    def read(work: IO[Int]): IO[Int] = mongoStorage(work)
  }

  test("claim paths treat a driver parse failure as a malformed marker, repair reads as transient storage") {
    assertEquals(
      Support.claim(IO.raiseError(parseFailure)).attempt.unsafeRunSync(),
      Left(AnalyticsError.MalformedMarker)
    )
    Support.read(IO.raiseError(parseFailure)).attempt.unsafeRunSync() match {
      case Left(_: AnalyticsError.MarkerStorageFailure) => ()
      case other                                        => fail(s"expected MarkerStorageFailure, got $other")
    }
  }
}
