package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.RepositoryError
import munit.CatsEffectSuite

import java.time.Instant

final class MongoLeaseRequestSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-10-20T10:00:00Z")

  test("AC-08.2 a request needs a worker of bounded length and a lease that ends after it starts") {
    assertEquals(LeaseRequest.validate("worker", now, now.plusSeconds(1)), Right(()))
    assertEquals(LeaseRequest.validate("", now, now.plusSeconds(1)), Left(LeaseRequestError.EmptyWorker))
    assertEquals(
      LeaseRequest.validate("w" * 129, now, now.plusSeconds(1)),
      Left(LeaseRequestError.WorkerTooLong)
    )
    assertEquals(LeaseRequest.validate("w" * 128, now, now.plusSeconds(1)), Right(()))
    assertEquals(LeaseRequest.validate("worker", now, now), Left(LeaseRequestError.WindowNotAfterNow))
    assertEquals(LeaseRequest.validate("worker", now, now.minusMillis(1)), Left(LeaseRequestError.WindowNotAfterNow))
  }

  test("AC-08.2 a renewal names no worker and checks the window only") {
    assertEquals(LeaseRequest.validateWindow(now, now.plusMillis(1)), Right(()))
    assertEquals(LeaseRequest.validateWindow(now, now), Left(LeaseRequestError.WindowNotAfterNow))
  }

  test("AC-08.2 every queue reports an invalid request the same way and never runs the guarded work") {
    for {
      ran <- cats.effect.Ref.of[cats.effect.IO, Boolean](false)
      guarded = com.example.graphQL.cats.service.port.RepositoryF.lift(ran.set(true))
      worker <- LeaseRequest.whenValid("", now, now.plusSeconds(1))(guarded).value
      window <- LeaseRequest.whenWindowValid(now, now)(guarded).value
      accepted <- LeaseRequest.whenValid("worker", now, now.plusSeconds(1))(guarded).value
      executed <- ran.get
    } yield {
      assertEquals(worker, Left(RepositoryError.InvalidEvent))
      assertEquals(window, Left(RepositoryError.InvalidEvent))
      assertEquals(accepted, Right(()))
      assert(executed)
    }
  }
}
