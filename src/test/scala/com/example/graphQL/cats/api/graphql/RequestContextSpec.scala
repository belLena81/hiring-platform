package com.example.graphQL.cats.api.graphql

import cats.effect.{Deferred, IO, Ref}
import com.example.graphQL.cats.application.ProbeResult
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class RequestContextSpec extends CatsEffectSuite {
  test("request release cancels and joins resolver work and rejects late submissions") {
    for {
      entered <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      allocated <- RequestContext.resource((entered.complete(()) *> IO.never[ProbeResult])
        .onCancel(released.complete(()).void)).allocated
      (context, release) = allocated
      _ <- IO(context.readiness)
      _ <- entered.get.timeout(2.seconds)
      _ <- release
      _ <- released.get.timeout(1.second)
      late <- IO(context.readiness).attempt
    } yield assert(late.isLeft)
  }

  test("memoization executes the request probe once across concurrent aliases") {
    for {
      count <- Ref.of[IO, Int](0)
      results <- RequestContext.resource(count.update(_ + 1).as(ProbeResult.Ready)).use { context =>
        for {
          first <- IO(context.readiness)
          second <- IO(context.readiness)
          firstResult <- IO.fromFuture(IO.pure(first))
          secondResult <- IO.fromFuture(IO.pure(second))
        } yield (firstResult, secondResult)
      }
      calls <- count.get
    } yield {
      assertEquals(calls, 1)
      assertEquals(results, (ProbeResult.Ready, ProbeResult.Ready))
    }
  }
}
