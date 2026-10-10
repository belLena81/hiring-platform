package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import com.mongodb.MongoException
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
      startTransactionFailure: Option[Throwable] = None
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
        transientExhaustionError = RepositoryError.Conflict
      )
      result <- runner.run(_ => RepositoryIO.lift(counters.operations.update(_ + 1)).as("done")).value
      observed <- counters.snapshot
    } yield (result, observed)

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
