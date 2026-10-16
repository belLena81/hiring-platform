package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.RepositoryF
import com.mongodb.{
  MongoCommandException,
  MongoExecutionTimeoutException,
  MongoOperationTimeoutException,
  MongoTimeoutException,
  ServerAddress
}
import munit.CatsEffectSuite
import org.bson.{BsonDocument, BsonInt32}

final class MongoSearchTimeBudgetSpec extends CatsEffectSuite {
  private val repository = new MongoSemanticSearchRepository(
    null,
    "jobs_vector",
    "candidates_vector",
    "jobs_lexical",
    "candidates_lexical",
    numCandidates = 60,
    diagnostics = Diagnostics.noop
  )

  private def guarded(failure: Throwable): IO[Either[Throwable, Either[RepositoryError, Unit]]] =
    repository.searchGuard("test")(RepositoryF.lift[IO, Unit](IO.raiseError(failure))).value.attempt

  test("AIR-05.4 a maxTime expiry is a spent time budget, also when wrapped") {
    val expired = new MongoExecutionTimeoutException(50, "operation exceeded time limit")
    assert(MongoSemanticSearchRepository.timeBudgetExpired(expired))
    assert(MongoSemanticSearchRepository.timeBudgetExpired(new RuntimeException("wrapped", expired)))
  }

  test("AIR-05.4 an unreachable deployment is not a spent time budget, so the fallback may answer") {
    assert(!MongoSemanticSearchRepository.timeBudgetExpired(new MongoTimeoutException("no server selected")))
    assert(!MongoSemanticSearchRepository.timeBudgetExpired(new IllegalStateException("other")))
  }

  test("AIR-05.4 a driver operation timeout is Overloaded, not a breaker failure, so no fallback answers") {
    // MongoOperationTimeoutException extends MongoTimeoutException; the time budget must be checked first.
    assert(new MongoOperationTimeoutException("operation timed out").isInstanceOf[MongoTimeoutException])
    assertIO(
      guarded(new MongoOperationTimeoutException("operation timed out")),
      Right(Left(RepositoryError.Overloaded))
    )
  }

  test("AIR-05.4 a server maxTimeMS expiry is Overloaded, not a breaker failure") {
    assertIO(
      guarded(new MongoExecutionTimeoutException(50, "operation exceeded time limit")),
      Right(Left(RepositoryError.Overloaded))
    )
    val command = new MongoCommandException(new BsonDocument("code", new BsonInt32(50)), new ServerAddress())
    assertIO(guarded(command), Right(Left(RepositoryError.Overloaded)))
  }

  test("AIR-05.4 an unreachable deployment still counts against the breaker") {
    val unreachable = new MongoTimeoutException("no server selected")
    assertIO(guarded(unreachable), Left(unreachable))
  }
}
