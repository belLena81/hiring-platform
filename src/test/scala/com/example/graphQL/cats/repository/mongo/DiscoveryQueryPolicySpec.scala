package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.RepositoryF
import com.mongodb.{
  MongoTimeoutException,
  MongoCommandException,
  MongoExecutionTimeoutException,
  MongoSocketOpenException,
  ServerAddress
}
import org.bson.{BsonDocument, BsonInt32, BsonString}
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

  private val settings = CircuitBreakerPolicy.Settings(2, 10.seconds, 100.millis, 1.second, 1)
  private def withBreaker(permits: Int = 1) =
    for {
      breaker <- CircuitBreaker.create[IO](settings, _ => IO.unit)
      policy <- DiscoveryQueryPolicy.create(2.seconds, permits, breaker)
    } yield (breaker, policy)

  test("CBR-04.1 repeated network and server-selection failures open the breaker and discovery fails before a permit") {
    val deadline = new MongoTimeoutException("no suitable server")
    val network = new MongoSocketOpenException("connect failed", new ServerAddress(), new RuntimeException())
    for {
      pair <- withBreaker()
      (breaker, policy) = pair
      _ <- policy.run(IO.raiseError[Unit](deadline)).attempt
      _ <- policy.run(IO.raiseError[Unit](network)).attempt
      // Hold the only permit: a rejected call must not wait for it.
      held <- Deferred[IO, Unit]
      holder <- policy.permits.permit.use(_ => held.get).start
      _ <- IO.sleep(10.millis)
      evaluated <- Ref.of[IO, Boolean](false)
      rejected <- policy.run(evaluated.set(true)).attempt.timeout(1.second)
      ran <- evaluated.get
      state <- breaker.current
      _ <- held.complete(()) *> holder.joinWithNever
    } yield {
      assertEquals(rejected, Left(DiscoveryQueryPolicy.Rejected))
      assert(!ran)
      assert(state.isInstanceOf[CircuitBreakerPolicy.State.Open], clue(state))
    }
  }

  test("CBR-04.3 maxTime expiry is the caller's query, not unavailability, and never opens the breaker") {
    for {
      pair <- withBreaker()
      (breaker, policy) = pair
      _ <- List
        .fill(10)(policy.run(IO.raiseError[Unit](new MongoExecutionTimeoutException(50, "time limit"))).attempt)
        .sequence
      state <- breaker.current
    } yield assertEquals(state, CircuitBreakerPolicy.initial)
  }

  test("CBR-04.4 the discovery guard surfaces unreachable deployments to the policy and maps other failures in place") {
    for {
      unreachable <- MongoRepositorySupport
        .discoveryGuard(Diagnostics.noop, "test")(RepositoryF.lift(IO.raiseError[Int](new MongoTimeoutException("x"))))
        .value
        .attempt
      deadline <- MongoRepositorySupport
        .discoveryGuard(Diagnostics.noop, "test")(
          RepositoryF.lift(IO.raiseError[Int](new MongoExecutionTimeoutException(50, "time limit")))
        )
        .value
    } yield {
      assert(unreachable.left.exists(_.isInstanceOf[MongoTimeoutException]), clue(unreachable))
      assertEquals(deadline, Left(RepositoryError.Unavailable))
    }
  }

  test("CBR-04.2 a query-shape or validation error is not counted") {
    val invalid = new MongoCommandException(
      new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(9)).append("errmsg", new BsonString("bad")),
      new ServerAddress()
    )
    for {
      pair <- withBreaker()
      (breaker, policy) = pair
      _ <- List.fill(5)(policy.run(IO.raiseError[Unit](invalid)).attempt).sequence
      _ <- List.fill(5)(policy.run(IO.raiseError[Unit](new IllegalArgumentException("shape"))).attempt).sequence
      state <- breaker.current
    } yield assertEquals(state, CircuitBreakerPolicy.initial)
  }
}
