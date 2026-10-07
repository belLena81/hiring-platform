package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class DiscoveryQueryPolicySpec extends CatsEffectSuite {
  test("cancellation releases shared query admission and the next operation can complete") {
    for {
      policy <- DiscoveryQueryPolicy.create(2.seconds, 1)
      entered <- Deferred[IO, Unit]
      running <- policy.run(entered.complete(()).void *> IO.never[Unit]).start
      _ <- entered.get
      available <- policy.permits.available
      _ = assertEquals(available, 0L)
      _ <- running.cancel
      result <- policy.run(IO.pure("completed")).timeout(1.second)
      _ = assertEquals(result, "completed")
      released <- policy.permits.available
      _ = assertEquals(released, 1L)
    } yield ()
  }
}
