package com.example.graphQL.cats.service.resilience

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class LeaseHeartbeatSpec extends CatsEffectSuite {
  private val lease = 30.seconds

  private def renewals(outcomes: List[Either[LeaseLost, Unit]]): IO[(IO[Either[LeaseLost, Unit]], IO[Int])] =
    Ref.of[IO, Int](0).map { calls =>
      val renew = calls.getAndUpdate(_ + 1).map(index => outcomes.lift(index).getOrElse(outcomes.last))
      (renew, calls.get)
    }

  test("AC-08.1 finished work answers its result and stops renewing") {
    TestControl.executeEmbed(for {
      (renew, calls) <- renewals(List(Right(())))
      result <- LeaseHeartbeat.guard[IO, String](lease)(renew)(IO.sleep(25.seconds).as("done"))
      renewed <- calls
    } yield {
      assertEquals(result, Right("done"))
      // Once before the work and then every ten seconds while it ran: 10s and 20s.
      assertEquals(renewed, 3)
    })
  }

  test("AC-08.1 without the initial renewal the first renewal happens one third of the lease in") {
    TestControl.executeEmbed(for {
      (renew, calls) <- renewals(List(Right(())))
      result <- LeaseHeartbeat.guard[IO, Int](lease, renewFirst = false)(renew)(IO.sleep(5.seconds).as(1))
      renewed <- calls
    } yield {
      assertEquals(result, Right(1))
      assertEquals(renewed, 0)
    })
  }

  test("AC-08.1 a lost lease cancels the work and is reported as lost") {
    TestControl.executeEmbed(for {
      (renew, calls) <- renewals(List(Right(()), Right(()), Left(LeaseLost.Expired)))
      started <- Ref.of[IO, Boolean](false)
      cancelled <- Ref.of[IO, Boolean](false)
      work = started.set(true) *> IO.never[Int].onCancel(cancelled.set(true))
      result <- LeaseHeartbeat.guard[IO, Int](lease)(renew)(work)
      wasStarted <- started.get
      wasCancelled <- cancelled.get
      renewed <- calls
    } yield {
      assertEquals(result, Left(LeaseLost.Expired))
      assert(wasStarted)
      assert(wasCancelled)
      assertEquals(renewed, 3)
    })
  }

  test("AC-08.1 an initial renewal that fails never starts the work") {
    TestControl.executeEmbed(for {
      (renew, _) <- renewals(List(Left(LeaseLost.Unavailable)))
      started <- Ref.of[IO, Boolean](false)
      result <- LeaseHeartbeat.guard[IO, Int](lease)(renew)(started.set(true).as(1))
      wasStarted <- started.get
    } yield {
      assertEquals(result, Left(LeaseLost.Unavailable))
      assert(!wasStarted)
    })
  }

  test("AC-08.1 a failure of the work propagates and a failure of a renewal propagates") {
    TestControl.executeEmbed(for {
      (renew, _) <- renewals(List(Right(())))
      failedWork <- LeaseHeartbeat
        .guard[IO, Int](lease)(renew)(IO.raiseError(new IllegalStateException("work")))
        .attempt
      failedRenewal <- LeaseHeartbeat
        .guard[IO, Int](lease)(IO.raiseError[Either[LeaseLost, Unit]](new IllegalStateException("renew")))(IO.never)
        .attempt
    } yield {
      assertEquals(failedWork.left.map(_.getMessage), Left("work"))
      assertEquals(failedRenewal.left.map(_.getMessage), Left("renew"))
    })
  }
}
