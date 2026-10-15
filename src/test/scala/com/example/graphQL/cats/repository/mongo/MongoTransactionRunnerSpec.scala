package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.State
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import cats.effect.Outcome
import cats.effect.testkit.TestControl
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.Completion
import com.mongodb.{
  MongoCommandException,
  MongoException,
  MongoNotPrimaryException,
  MongoSocketReadTimeoutException,
  MongoTimeoutException,
  ServerAddress
}
import org.bson.{BsonDocument, BsonInt32, BsonString}
import scala.concurrent.duration.*
import com.mongodb.reactivestreams.client.{ClientSession as JClientSession, MongoClient as JMongoClient}
import mongo4cats.bson.Document
import mongo4cats.client.{ClientSession, GenericMongoClient, MongoClient}
import mongo4cats.database.GenericMongoDatabase
import mongo4cats.models.client.{ClientSessionOptions, TransactionOptions}
import munit.CatsEffectSuite

import scala.concurrent.duration.Duration

/** Drives the real runner against scripted sessions: commit and transaction retry classification and abort ownership.
  */
final class MongoTransactionRunnerSpec extends CatsEffectSuite {
  private type Streams[A] = fs2.Stream[IO, A]
  private type Resources[A] = Resource[IO, A]

  private val policy = MongoTransactionRunner.RetryPolicy(
    maxTransactionAttempts = 3,
    maxCommitAttempts = 3,
    initialDelay = Duration.Zero,
    maxDelay = Duration.Zero
  )

  private def unknownCommit: MongoException =
    new MongoException("unknown").tap(_.addLabel(MongoErrors.UnknownTransactionCommitResultLabel))
  private def transient: MongoException =
    new MongoException("transient").tap(_.addLabel(MongoErrors.TransientTransactionErrorLabel))
  private def both: MongoException = unknownCommit.tap(_.addLabel(MongoErrors.TransientTransactionErrorLabel))

  private final case class Counters(
      sessions: Ref[IO, Int],
      starts: Ref[IO, Int],
      commits: Ref[IO, Int],
      aborts: Ref[IO, Int],
      operations: Ref[IO, Int]
  ) {
    def snapshot: IO[(Int, Int, Int, Int, Int)] =
      (sessions.get, starts.get, commits.get, aborts.get, operations.get).tupled
  }

  /** `commitOutcomes` is consumed one per commit call; an exhausted script repeats its last outcome. */
  private def run(
      commitOutcomes: List[Option[Throwable]],
      startSessionFailure: Option[Throwable] = None,
      startTransactionFailure: Option[Throwable] = None,
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO],
      outcome: Either[RepositoryError, String] = Right("done"),
      hangAfterOperations: Option[Int] = None
  ): IO[(Either[RepositoryError, String], (Int, Int, Int, Int, Int))] =
    for {
      counters <- (Ref.of[IO, Int](0), Ref.of[IO, Int](0), Ref.of[IO, Int](0), Ref.of[IO, Int](0), Ref.of[IO, Int](0))
        .mapN(Counters.apply)
      script <- Ref.of[IO, List[Option[Throwable]]](commitOutcomes)
      client = scriptedClient(counters, script, startSessionFailure, startTransactionFailure)
      runner = MongoTransactionRunner.sessions(
        client,
        duplicateKeyError = RepositoryError.Conflict,
        retryPolicy = policy,
        diagnostics = Diagnostics.noop,
        transientExhaustionError = RepositoryError.Conflict,
        breaker = breaker
      )
      result <- runner
        .run(_ =>
          RepositoryF
            .lift(
              counters.operations
                .updateAndGet(_ + 1)
                .flatMap(count => if (hangAfterOperations.exists(count > _)) IO.never[Unit] else IO.unit)
            )
            .flatMap(_ => RepositoryF.at[IO].fromEither(outcome))
        )
        .value
      observed <- counters.snapshot
    } yield (result, observed)

  private val breakerSettings = CircuitBreakerPolicy.Settings(2, 10.seconds, 100.millis, 1.second, 1)
  private def newBreaker = CircuitBreaker.create[IO](breakerSettings, _ => IO.unit)

  test("CBR-06.2 repeated server-selection failures open the breaker and the next write starts no session") {
    val timeout = new MongoTimeoutException("no server")
    for {
      breaker <- newBreaker
      first <- run(Nil, startSessionFailure = Some(timeout), breaker = breaker)
      second <- run(Nil, startSessionFailure = Some(timeout), breaker = breaker)
      third <- run(Nil, breaker = breaker)
      state <- breaker.current
    } yield {
      assertEquals(first._1, Left(RepositoryError.Unavailable))
      assertEquals(second._1, Left(RepositoryError.Unavailable))
      assertEquals(third, (Left(RepositoryError.Unavailable), (0, 0, 0, 0, 0)))
      assert(state.isInstanceOf[State.Open], clue(state))
    }
  }

  private def labelled[E <: MongoException](error: E): E =
    error.tap(_.addLabel(MongoErrors.TransientTransactionErrorLabel))
  private val server = new ServerAddress()
  private def writeConflict: MongoCommandException =
    new MongoCommandException(
      new BsonDocument("ok", new BsonInt32(0))
        .append("code", new BsonInt32(112))
        .append("errmsg", new BsonString("wc")),
      server
    )

  List[(String, () => Throwable)](
    "a labelled server-selection timeout" -> (() => labelled(new MongoTimeoutException("no server"))),
    "a labelled socket read timeout" -> (() =>
      labelled(new MongoSocketReadTimeoutException("read", server, new RuntimeException()))
    ),
    "a labelled not-primary error" -> (() =>
      labelled(new MongoNotPrimaryException(new BsonDocument("code", new BsonInt32(10107)), server))
    )
  ).foreach { case (name, error) =>
    test(s"CBR-06.5 $name that exhausts its retries is Unavailable and opens the breaker") {
      for {
        breaker <- newBreaker
        first <- run(List(Some(error())), breaker = breaker)
        second <- run(List(Some(error())), breaker = breaker)
        state <- breaker.current
      } yield {
        assertEquals(first._1, Left(RepositoryError.Unavailable))
        assertEquals(second._1, Left(RepositoryError.Unavailable))
        assertEquals(first._2, (3, 3, 3, 3, 3))
        assert(state.isInstanceOf[State.Open], clue(state))
      }
    }
  }

  test("CBR-06.5 a write conflict that exhausts its retries stays Conflict and is not counted") {
    for {
      breaker <- newBreaker
      results <- List.fill(5)(run(List(Some(writeConflict)), breaker = breaker)).sequence
      state <- breaker.current
    } yield {
      assert(results.forall(_._1 == Left(RepositoryError.Conflict)), clue(results))
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  test(
    "CBR-06.6 a call cancelled after an unreachable attempt counts, one cancelled without such an attempt does not"
  ) {
    TestControl.executeEmbed(
      for {
        seen <- newBreaker
        _ <- List
          .fill(2)(
            run(
              List(Some(labelled(new MongoTimeoutException("no server")))),
              breaker = seen,
              hangAfterOperations = Some(1)
            )
              .timeout(1.second)
              .attempt
          )
          .sequence
        seenState <- seen.current
        unseen <- newBreaker
        _ <- List
          .fill(5)(run(Nil, breaker = unseen, hangAfterOperations = Some(0)).timeout(1.second).attempt)
          .sequence
        unseenState <- unseen.current
      } yield {
        assert(seenState.isInstanceOf[State.Open], clue(seenState))
        assertEquals(unseenState, CircuitBreakerPolicy.initial)
      }
    )
  }

  test("CBR-06.7 only a committed result is a success; a typed rejection or cancellation is not") {
    val unreachable = IO.pure(true)
    val clean = IO.pure(false)
    for {
      committed <- MongoTransactionRunner.classify(clean)(Outcome.succeeded(IO.pure(Right("done"))))
      rejected <- MongoTransactionRunner.classify(clean)(
        Outcome.succeeded(IO.pure(Left(RepositoryError.DuplicateApplication)))
      )
      cancelledClean <- MongoTransactionRunner.classify(clean)(Outcome.canceled)
      cancelledSeen <- MongoTransactionRunner.classify(unreachable)(Outcome.canceled)
      failed <- MongoTransactionRunner.classify(clean)(Outcome.errored(new MongoTimeoutException("x")))
    } yield {
      assertEquals(committed, Completion.Success)
      assertEquals(rejected, Completion.IgnoredFailure)
      assertEquals(cancelledClean, Completion.IgnoredFailure)
      assertEquals(cancelledSeen, Completion.CountedFailure)
      assertEquals(failed, Completion.CountedFailure)
    }
  }

  test("CBR-06.7 a typed rejection while half-open does not close the breaker") {
    val timeout = new MongoTimeoutException("no server")
    for {
      breaker <- newBreaker
      _ <- run(Nil, startSessionFailure = Some(timeout), breaker = breaker)
      _ <- run(Nil, startSessionFailure = Some(timeout), breaker = breaker)
      _ <- IO.sleep(200.millis)
      probe <- run(Nil, breaker = breaker, outcome = Left(RepositoryError.DuplicateApplication))
      state <- breaker.current
    } yield {
      assertEquals(probe._1, Left(RepositoryError.DuplicateApplication))
      assert(state.isInstanceOf[State.HalfOpen], clue(state))
    }
  }

  test("CBR-06.1 write conflicts and transient exhaustion never open the breaker") {
    for {
      breaker <- newBreaker
      results <- List.fill(5)(run(List(Some(transient)), breaker = breaker)).sequence
      state <- breaker.current
    } yield {
      assert(results.forall(_._1 == Left(RepositoryError.Conflict)))
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  test("CBR-06.1 typed business rejections never open the breaker") {
    for {
      breaker <- newBreaker
      results <- List
        .fill(5)(run(Nil, breaker = breaker, outcome = Left(RepositoryError.DuplicateApplication)))
        .sequence
      state <- breaker.current
    } yield {
      assert(results.forall(_._1 == Left(RepositoryError.DuplicateApplication)))
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  test("CBR-06.8 unavailability counted only after the runner's own retries are exhausted") {
    // A transient label that resolves within the retry budget is a success for the breaker.
    for {
      breaker <- newBreaker
      result <- run(List(Some(transient), None), breaker = breaker)
      state <- breaker.current
    } yield {
      assertEquals(result._1, Right("done"))
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  test("CBR-06.3 a write is accepted again once the open period ends and a probe succeeds") {
    val timeout = new MongoTimeoutException("no server")
    for {
      breaker <- newBreaker
      _ <- run(Nil, startSessionFailure = Some(timeout), breaker = breaker)
      _ <- run(Nil, startSessionFailure = Some(timeout), breaker = breaker)
      _ <- IO.sleep(200.millis)
      recovered <- run(Nil, breaker = breaker)
      state <- breaker.current
    } yield {
      assertEquals(recovered, (Right("done"), (1, 1, 1, 0, 1)))
      assertEquals(state, CircuitBreakerPolicy.initial)
    }
  }

  // (sessions, starts, commits, aborts, operations)
  test("an unknown commit result is retried on the same transaction until it commits") {
    run(List(Some(unknownCommit), Some(unknownCommit), None)).map { case (result, counts) =>
      assertEquals(result, Right("done"))
      assertEquals(counts, (1, 1, 3, 0, 1))
    }
  }

  test("an unknown commit result that never resolves is Unavailable after bounded commit retries and one abort") {
    run(List(Some(unknownCommit))).map { case (result, counts) =>
      assertEquals(result, Left(RepositoryError.Unavailable))
      assertEquals(counts, (1, 1, 3, 1, 1))
    }
  }

  test("a transient label at commit retries the whole transaction") {
    run(List(Some(transient), None)).map { case (result, counts) =>
      assertEquals(result, Right("done"))
      assertEquals(counts, (2, 2, 2, 1, 2))
    }
  }

  test("a commit error with both labels retries the whole transaction without retrying the commit") {
    run(List(Some(both), None)).map { case (result, counts) =>
      assertEquals(result, Right("done"))
      assertEquals(counts, (2, 2, 2, 1, 2))
    }
  }

  test("a persistently transient commit is a conflict after bounded transaction attempts, one abort each") {
    run(List(Some(transient))).map { case (result, counts) =>
      assertEquals(result, Left(RepositoryError.Conflict))
      assertEquals(counts, (3, 3, 3, 3, 3))
    }
  }

  test("a session start failure is Unavailable without running the operation") {
    run(Nil, startSessionFailure = Some(new MongoException("no session"))).map { case (result, counts) =>
      assertEquals(result, Left(RepositoryError.Unavailable))
      assertEquals(counts, (1, 0, 0, 0, 0))
    }
  }

  test("a transaction start failure is Unavailable without running the operation") {
    run(Nil, startTransactionFailure = Some(new MongoException("no transaction"))).map { case (result, counts) =>
      assertEquals(result, Left(RepositoryError.Unavailable))
      assertEquals(counts, (1, 1, 0, 0, 0))
    }
  }

  private def scriptedClient(
      counters: Counters,
      script: Ref[IO, List[Option[Throwable]]],
      startSessionFailure: Option[Throwable],
      startTransactionFailure: Option[Throwable]
  ): MongoClient[IO] =
    new GenericMongoClient[IO, Streams, Resources] {
      override def underlying: JMongoClient = unsupported
      override def getDatabase(name: String): IO[GenericMongoDatabase[IO, Streams]] = unsupported
      override def listDatabaseNames: IO[Iterable[String]] = unsupported
      override def listDatabases: IO[Iterable[Document]] = unsupported
      override def listDatabases(session: ClientSession[IO]): IO[Iterable[Document]] = unsupported

      override def startSession(options: ClientSessionOptions): Resource[IO, ClientSession[IO]] =
        Resource.eval(counters.sessions.update(_ + 1) *> startSessionFailure.traverse_(IO.raiseError)).as(session)

      private def session: ClientSession[IO] = new ClientSession[IO] {
        @volatile private var active = false
        override def underlying: JClientSession = unsupported
        override def hasActiveTransaction: Boolean = active
        override def startTransaction(options: TransactionOptions): IO[Unit] =
          counters.starts.update(_ + 1) *> startTransactionFailure.traverse_(IO.raiseError) *> IO.delay {
            active = true
          }
        override def abortTransaction: IO[Unit] = counters.aborts.update(_ + 1) *> IO.delay { active = false }
        override def commitTransaction: IO[Unit] =
          counters.commits.update(_ + 1) *> script
            .modify {
              case head :: Nil  => (head :: Nil, head)
              case head :: tail => (tail, head)
              case Nil          => (Nil, None)
            }
            .flatMap {
              case Some(error) => IO.raiseError[Unit](error)
              case None        => IO.delay { active = false }
            }
      }
    }

  private def unsupported[A]: A = throw new UnsupportedOperationException("not used by the transaction runner")

  extension [A](value: A) private def tap(f: A => Unit): A = { f(value); value }
}
