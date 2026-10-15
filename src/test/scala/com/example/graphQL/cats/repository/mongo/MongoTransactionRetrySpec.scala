package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{MutationWriteContext, RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import mongo4cats.client.ClientSession
import munit.CatsEffectSuite

final class MongoTransactionRetrySpec extends CatsEffectSuite {
  test("noop context opens a repository transaction only for an atomic multi-write") {
    for {
      runs <- Ref.of[IO, Int](0)
      operations <- Ref.of[IO, Int](0)
      runner = recordingRunner(runs)
      atomic <- MongoMutationWriteContext
        .run(MutationWriteContext.directWrite, runner, transactionRequired = true) { _ =>
          RepositoryF.lift(operations.update(_ + 1)).as("atomic")
        }
        .value
      direct <- MongoMutationWriteContext
        .run(MutationWriteContext.directWrite, runner, transactionRequired = false) { _ =>
          RepositoryF.lift(operations.update(_ + 1)).as("direct")
        }
        .value
      runCount <- runs.get
      operationCount <- operations.get
    } yield {
      assertEquals(atomic, Right("atomic"))
      assertEquals(direct, Right("direct"))
      assertEquals(runCount, 1)
      assertEquals(operationCount, 2)
    }
  }

  test("Mongo receipt context joins its supplied session without nesting a transaction") {
    for {
      runs <- Ref.of[IO, Int](0)
      runner = recordingRunner(runs)
      result <- MongoMutationWriteContext
        .run(MongoMutationWriteContext(None), runner, transactionRequired = true) { session =>
          RepositoryF.at[IO].pure(session)
        }
        .value
      runCount <- runs.get
    } yield {
      assertEquals(result, Right(None))
      assertEquals(runCount, 0)
    }
  }

  test("foreign adapter contexts fail as effects for session extraction and execution") {
    val foreign = new MutationWriteContext {}
    for {
      runs <- Ref.of[IO, Int](0)
      sessionFailure <- MongoMutationWriteContext.session(foreign).attempt
      runFailure <- MongoMutationWriteContext
        .run(foreign, recordingRunner(runs), transactionRequired = false)(_ =>
          RepositoryF.at[IO].fromEither(Right("unused"): Either[RepositoryError, String])
        )
        .value
        .attempt
      runCount <- runs.get
    } yield {
      assert(sessionFailure.swap.exists(_.isInstanceOf[MongoSetupError]))
      assert(runFailure.swap.exists(_.isInstanceOf[MongoSetupError]))
      assertEquals(runCount, 0)
    }
  }

  private def recordingRunner(runs: Ref[IO, Int]): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](
          operation: Option[ClientSession[IO]] => RepositoryF[IO, A]
      ): RepositoryF[IO, A] = RepositoryF.lift(runs.update(_ + 1)) *> operation(None)
    }
}
