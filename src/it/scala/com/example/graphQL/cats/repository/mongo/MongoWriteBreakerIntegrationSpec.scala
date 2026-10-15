package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import com.example.hiring.testing.LocalTestServices
import org.bson.Document
import scala.concurrent.duration.*

/** The Mongo-writes breaker around the real transaction runner. Business conflicts must never open it. */
class MongoWriteBreakerIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val settings = CircuitBreakerPolicy.Settings(2, 30.seconds, 5.seconds, 30.seconds, 1)

  test("CBR-06.1 parallel duplicate submissions leave one write and keep the Mongo-writes breaker closed") {
    MongoDatabaseProbe.clientResource(mongoEndpoint.uri).use { client =>
      LocalTestServices.database(client).use { database =>
        for {
          breaker <- CircuitBreaker.create[IO](settings, _ => IO.unit)
          _ <- database.createCollection("breaker_duplicates")
          documents <- Mongo4catsCollections.documents(database, "breaker_duplicates")
          runner = MongoTransactionRunner.sessions(
            client,
            RepositoryError.DuplicateApplication,
            diagnostics = Diagnostics.noop[IO],
            breaker = breaker
          )
          submit = runner.run { session =>
            RepositoryF
              .lift(MongoSessionOperations.insertOne(IO.pure(documents), session, new Document("_id", "candidate-job")))
              .as("accepted")
          }.value
          results <- List.fill(12)(submit).parSequence
          stored <- documents.count
          state <- breaker.current
        } yield {
          assertEquals(results.count(_ == Right("accepted")), 1)
          assert(
            results.forall(result =>
              result == Right("accepted") ||
                result == Left(RepositoryError.DuplicateApplication) ||
                result == Left(RepositoryError.Conflict)
            ),
            clue(results)
          )
          assertEquals(stored, 1L)
          assertEquals(state, CircuitBreakerPolicy.initial)
        }
      }
    }
  }
}
