package com.example.graphQL.cats.api.graphql

import cats.effect.{Deferred, IO, Ref}
import com.example.graphQL.cats.service.ProbeResult
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class RequestContextSpec extends CatsEffectSuite {
  test("request release cancels in-flight resolver work and rejects late submissions") {
    for {
      entered <- Deferred[IO, Unit]
      cancelled <- Deferred[IO, Unit]
      allocated <- TestGraphQLSupport.context((entered.complete(()) *> IO.never[ProbeResult])
        .onCancel(cancelled.complete(()) *> IO.unit)).allocated
      (context, release) = allocated
      _ <- IO(context.unsafeToFuture(context.readiness))
      _ <- entered.get.timeout(2.seconds)
      _ <- release
      _ <- cancelled.get.timeout(2.seconds)
      after <- IO(context.unsafeToFuture(context.readiness)).attempt
    } yield assert(after.isLeft)
  }

  test("request contexts have independent cancellation signals") {
    for {
      firstEntered <- Deferred[IO, Unit]
      firstCancelled <- Deferred[IO, Unit]
      secondEntered <- Deferred[IO, Unit]
      secondReady <- Deferred[IO, ProbeResult]
      first <- TestGraphQLSupport.context((firstEntered.complete(()) *> IO.never[ProbeResult])
        .onCancel(firstCancelled.complete(()) *> IO.unit)).allocated
      second <- TestGraphQLSupport.context((secondEntered.complete(()) *> secondReady.get)
        .onCancel(IO.unit)).allocated
      _ <- IO(first._1.unsafeToFuture(first._1.readiness))
      _ <- IO(second._1.unsafeToFuture(second._1.readiness))
      _ <- firstEntered.get.timeout(2.seconds)
      _ <- secondEntered.get.timeout(2.seconds)
      _ <- first._2
      _ <- firstCancelled.get.timeout(2.seconds)
      _ <- secondReady.complete(ProbeResult.Ready)
      secondResult <- IO(second._1.unsafeToFuture(second._1.readiness)).flatMap(future => IO.fromFuture(IO.pure(future))).attempt
      _ <- second._2
    } yield assert(secondResult.isRight)
  }

  test("memoization executes the request probe once across concurrent aliases") {
    for {
      count <- Ref.of[IO, Int](0)
      results <- TestGraphQLSupport.context(count.update(_ + 1).as(ProbeResult.Ready)).use { context =>
        for {
          first <- IO(context.unsafeToFuture(context.readiness))
          second <- IO(context.unsafeToFuture(context.readiness))
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

  test("anonymous nested email visibility remains masked without resolving a viewer") {
    TestGraphQLSupport.context(IO.pure(ProbeResult.Ready)).use { context =>
      context.visibleEmailUsers(Nil).map(result => assertEquals(result, Nil))
    }
  }
}
