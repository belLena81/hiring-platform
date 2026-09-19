package com.example.graphQL.cats.api.graphql

import cats.effect.{Deferred, IO, IOLocal, Ref, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.service.{ProbeResult, TraceContext}
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class RequestContextSpec extends CatsEffectSuite {
  test("dispatcher trace binding restores its previous value after success, failure, and cancellation") {
    Dispatcher.sequential[IO].use { dispatcher =>
      for {
        local <- IOLocal[Option[TraceContext]](None)
        ambient <- TraceContext.root("00000000-0000-0000-0000-000000000001")
        requestTrace <- TraceContext.root("00000000-0000-0000-0000-000000000002")
        _ <- IO.fromFuture(IO(dispatcher.unsafeToFuture(local.set(Some(ambient)))))
        hiring = TestGraphQLSupport.emptyServices.copy(traceLocal = Some(local))
        contextResource = RequestContext.withDispatcher(dispatcher, IO.pure(ProbeResult.Ready), None, hiring,
          IO.pure(ProbeResult.Ready), Some(requestTrace))
        _ <- contextResource.use { context =>
          for {
            observed <- IO.fromFuture(IO(context.unsafeToFuture(local.get)))
            _ <- IO(assertEquals(observed, Some(requestTrace)))
            failure <- IO.fromFuture(IO(context.unsafeToFuture(IO.raiseError[Unit](new RuntimeException("expected"))))).attempt
            _ <- IO(assert(failure.isLeft))
            restored <- IO.fromFuture(IO(dispatcher.unsafeToFuture(local.get)))
            _ <- IO(assertEquals(restored, Some(ambient)))
          } yield ()
        }
        cancelled <- Deferred[IO, Unit]
        _ <- contextResource.use(context => IO(context.unsafeToFuture(IO.canceled.onCancel(cancelled.complete(()).void))))
        _ <- cancelled.get.timeout(1.second)
        restored <- IO.fromFuture(IO(dispatcher.unsafeToFuture(local.get)))
      } yield assertEquals(restored, Some(ambient))
    }
  }

  test("closing rejects submissions while resolver cancellation finalizers are still running") {
    for {
      entered <- Deferred[IO, Unit]
      finalizing <- Deferred[IO, Unit]
      finishFinalizer <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      _ <- Resource.make(TestGraphQLSupport.context((entered.complete(()) *> IO.never[ProbeResult])
        .onCancel(finalizing.complete(()) *> finishFinalizer.get)).allocated) {
        case (_, release) => finishFinalizer.complete(()).void *> release
      }.use { case (context, release) =>
        for {
          _ <- IO(context.readiness)
          _ <- entered.get.timeout(2.seconds)
          _ <- (release *> released.complete(()).void).background.use { _ =>
            (for {
              _ <- finalizing.get.timeout(2.seconds)
              _ <- IO(context.readiness).attempt
              completed <- released.tryGet
              _ <- IO {
                assertEquals(completed, None)
              }
              _ <- finishFinalizer.complete(())
              _ <- released.get.timeout(2.seconds)
              after <- IO(context.readiness).attempt
              _ <- IO(assert(after.isLeft))
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
      _ <- Resource.make(TestGraphQLSupport.context((entered.complete(()) *> IO.never[ProbeResult])
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
      results <- TestGraphQLSupport.context(count.update(_ + 1).as(ProbeResult.Ready)).use { context =>
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
