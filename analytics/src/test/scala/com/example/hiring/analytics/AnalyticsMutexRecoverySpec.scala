package com.example.hiring.analytics

import cats.effect.{IO, Ref, Deferred, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import com.example.hiring.analytics.errors.AnalyticsError
import scala.concurrent.duration.*

class AnalyticsMutexRecoverySpec extends munit.CatsEffectSuite {
  test("ambiguous insertion accepts only original owner after transient absence and read failure") {
    TestControl.executeEmbed(for {
      reads <- Ref.of[IO, Int](0)
      owner <- MongoAnalyticsLakehouseLock.reconcileOwnership[IO](
        "original",
        _ =>
          reads.getAndUpdate(_ + 1).flatMap {
            case 0 => IO.pure(None)
            case 1 => IO.raiseError(new RuntimeException("unavailable"))
            case _ => IO.pure(Some("original"))
          }
      )
    } yield assertEquals(owner, "original"))
  }
  test("another owner is never adopted") {
    MongoAnalyticsLakehouseLock
      .reconcileOwnership[IO]("original", _ => IO.pure(Some("other")))
      .attempt
      .map(result => assertEquals(result, Left(AnalyticsError.LakehouseLockTimeout)))
  }
  test("absent or unavailable ownership fails closed at thirty seconds") {
    List[IO[Option[String]]](IO.pure(None), IO.never).traverse_ { read =>
      TestControl.executeEmbed(for {
        started <- IO.monotonic
        result <- Resource
          .makeFull[IO, String](_ => MongoAnalyticsLakehouseLock.reconcileOwnership[IO]("original", _ => read))(_ =>
            IO.unit
          )
          .use(IO.pure)
          .attempt
        ended <- IO.monotonic
      } yield {
        assert(result.isLeft)
        assertEquals(ended - started, 30.seconds)
      })
    }
  }
  test("cancellation during reconciliation releases a subsequently confirmed owner") {
    for {
      reading <- Deferred[IO, Unit]
      acknowledged <- Deferred[IO, Option[String]]
      released <- Deferred[IO, String]
      acquisition = Resource.makeFull[IO, String](_ =>
        MongoAnalyticsLakehouseLock
          .reconcileOwnership[IO]("original", _ => reading.complete(()).void *> acknowledged.get)
      )(owner => released.complete(owner).void)
      fiber <- acquisition.use(_ => IO.never).start
      _ <- reading.get
      cancelling <- fiber.cancel.start
      _ <- IO.cede
      _ <- acknowledged.complete(Some("original"))
      _ <- cancelling.joinWithNever
      owner <- released.get
    } yield assertEquals(owner, "original")
  }

  test("initial duplicate ownership read and subsequent reconciliation share one deadline") {
    TestControl.executeEmbed(for {
      started <- IO.monotonic
      _ <- IO.sleep(20.seconds)
      result <- Resource
        .makeFull[IO, String](_ =>
          MongoAnalyticsLakehouseLock.reconcileOwnership[IO]("original", _ => IO.never, Some(started))
        )(_ => IO.unit)
        .use(IO.pure)
        .attempt
      ended <- IO.monotonic
    } yield { assert(result.isLeft); assertEquals(ended - started, 30.seconds) })
  }

}
