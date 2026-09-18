package com.example.graphQL.cats.api.graphql

import cats.effect.{Deferred, IO, Ref, Resource}
import com.example.graphQL.cats.api.graphql.RequestContext
import com.example.graphQL.cats.service.ProbeResult
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class RequestContextSpec extends CatsEffectSuite {
  test("closing rejects submissions while resolver cancellation finalizers are still running") {
    for {
      entered <- Deferred[IO, Unit]
      finalizing <- Deferred[IO, Unit]
      finishFinalizer <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      _ <- Resource.make(RequestContext.resource((entered.complete(()) *> IO.never[ProbeResult])
        .onCancel(finalizing.complete(()) *> finishFinalizer.get)).allocated) {
        case (_, release) => finishFinalizer.complete(()).void *> release
      }.use { case (context, release) =>
        for {
          _ <- IO(context.readiness)
          _ <- entered.get.timeout(2.seconds)
          _ <- (release *> released.complete(()).void).background.use { _ =>
            (for {
              _ <- finalizing.get.timeout(2.seconds)
              late <- IO(context.readiness).attempt
              completed <- released.tryGet
              _ <- IO {
                assert(late.isLeft)
                assertEquals(completed, None)
              }
              _ <- finishFinalizer.complete(())
              _ <- released.get.timeout(2.seconds)
            } yield ()).guarantee(finishFinalizer.complete(()).void)
          }
        } yield ()
      }
    } yield ()
  }

  test("request release cancels and joins resolver work and rejects late submissions") {
    for {
      entered <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      _ <- Resource.make(RequestContext.resource((entered.complete(()) *> IO.never[ProbeResult])
        .onCancel(released.complete(()).void)).allocated)(_._2).use { case (context, release) =>
        for {
          _ <- IO(context.readiness)
          _ <- entered.get.timeout(2.seconds)
          _ <- release
          _ <- released.get.timeout(1.second)
          late <- IO(context.readiness).attempt
        } yield assert(late.isLeft)
      }
    } yield ()
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
