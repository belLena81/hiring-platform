package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.RepositoryError
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class MongoLeaseRequestSpec extends CatsEffectSuite {

  test("AC-08.2 DDR-01 a request needs a worker of bounded length and a positive lease duration within the maximum") {
    assertEquals(LeaseRequest.validate("worker", 1.second), Right(()))
    assertEquals(LeaseRequest.validate("", 1.second), Left(LeaseRequestError.EmptyWorker))
    assertEquals(LeaseRequest.validate("w" * 129, 1.second), Left(LeaseRequestError.WorkerTooLong))
    assertEquals(LeaseRequest.validate("w" * 128, 1.second), Right(()))
    assertEquals(LeaseRequest.validate("worker", 0.millis), Left(LeaseRequestError.DurationNotPositive))
    assertEquals(LeaseRequest.validate("worker", (-1).millis), Left(LeaseRequestError.DurationNotPositive))
    assertEquals(LeaseRequest.validate("worker", 1.millis), Right(()))
    assertEquals(LeaseRequest.validate("worker", LeaseRequest.MaxLeaseDuration), Right(()))
    assertEquals(
      LeaseRequest.validate("worker", LeaseRequest.MaxLeaseDuration + 1.millis),
      Left(LeaseRequestError.DurationTooLong)
    )
  }

  test("AC-08.2 DDR-01 a renewal names no worker and checks the duration only") {
    assertEquals(LeaseRequest.validateDuration(1.milli), Right(()))
    assertEquals(LeaseRequest.validateDuration(0.millis), Left(LeaseRequestError.DurationNotPositive))
    assertEquals(
      LeaseRequest.validateDuration(LeaseRequest.MaxLeaseDuration + 1.second),
      Left(LeaseRequestError.DurationTooLong)
    )
  }

  test("AC-08.2 every queue reports an invalid request the same way and never runs the guarded work") {
    for {
      ran <- cats.effect.Ref.of[cats.effect.IO, Boolean](false)
      guarded = com.example.graphQL.cats.service.port.RepositoryF.lift(ran.set(true))
      worker <- LeaseRequest.whenValid("", 1.second)(guarded).value
      duration <- LeaseRequest.whenDurationValid(0.millis)(guarded).value
      untouched <- ran.get
      accepted <- LeaseRequest.whenValid("worker", 1.second)(guarded).value
      executed <- ran.get
    } yield {
      assertEquals(worker, Left(RepositoryError.InvalidEvent))
      assertEquals(duration, Left(RepositoryError.InvalidEvent))
      assert(!untouched)
      assertEquals(accepted, Right(()))
      assert(executed)
    }
  }
}
