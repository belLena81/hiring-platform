package com.example.graphQL.cats.service

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.RepositoryReporting.orReport
import com.example.graphQL.cats.service.events.OperationalEventContractError
import com.example.graphQL.cats.service.events.OperationalEventFailure.asUseCaseError
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.search.DegradedSearch
import com.example.graphQL.cats.service.search.SearchFailure.{asSearchFailure, degradation}
import munit.CatsEffectSuite

/** AC-06: one reading of each repository or contract failure, applied by every layer instead of re-matching. */
final class RepositoryFailureReadingSpec extends CatsEffectSuite {

  test("AC-06.5 every operational event contract violation is an internal InvalidEvent fault, never a client error") {
    OperationalEventContractError.values.foreach { violation =>
      assertEquals(violation.asUseCaseError, UseCaseError.Repository(RepositoryError.InvalidEvent))
    }
  }

  test("AC-06.2 only a provider outage or a lone vector failure is degradable") {
    assertEquals(
      UseCaseError.Search(SearchError.ProviderUnavailable).degradation,
      Some(DegradedSearch(SearchError.ProviderUnavailable))
    )
    assertEquals(
      UseCaseError.Search(SearchError.VectorSearchUnavailable).degradation,
      Some(DegradedSearch(SearchError.VectorSearchUnavailable))
    )
    List(
      UseCaseError.Repository(RepositoryError.Unavailable),
      UseCaseError.Repository(RepositoryError.Overloaded),
      UseCaseError.Repository(RepositoryError.InvalidStoredData),
      UseCaseError.Search(SearchError.FacetsUnavailable)
    ).foreach(error => assertEquals(error.degradation, None))
  }

  test(
    "AC-06.2 a search keeps revoked authority and conflicts and sanitizes other store faults to an unavailable search"
  ) {
    List(
      RepositoryError.Unavailable,
      RepositoryError.Overloaded,
      RepositoryError.InvalidStoredData,
      RepositoryError.InvalidEvent,
      RepositoryError.MissingStoredResult
    ).foreach { fault =>
      assertEquals(
        UseCaseError.Repository(fault).asSearchFailure,
        UseCaseError.Search(SearchError.VectorSearchUnavailable)
      )
    }
    List(
      UseCaseError.Repository(RepositoryError.Conflict),
      UseCaseError.Domain(DomainError.Forbidden),
      UseCaseError.Search(SearchError.ProviderUnavailable)
    ).foreach(error => assertEquals(error.asSearchFailure, error))
  }

  test("AC-06.4 orReport answers the default and reports the safe error code once for a failed step") {
    for {
      seen <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          seen.update(_ :+ (event -> fields))
      }
      failed <- RepositoryF
        .at[IO]
        .raise[Boolean](RepositoryError.InvalidStoredData)
        .orReport(diagnostics, LogEvent.EmbeddingProcessingFailed)(false)
      succeeded <- RepositoryF.at[IO].pure(true).orReport(diagnostics, LogEvent.EmbeddingProcessingFailed)(false)
      events <- seen.get
    } yield {
      assertEquals(failed, false)
      assertEquals(succeeded, true)
      assertEquals(
        events,
        List(LogEvent.EmbeddingProcessingFailed -> Map(LogField.ErrorCode -> "INVALID_STORED_DATA"))
      )
    }
  }

  test("AC-06.4 every repository failure has a public error code") {
    RepositoryError.values.foreach { error =>
      val reported = LogFields.repositoryFailure(error)
      assert(reported.forall((field, value) => LogFields.validPublic(field, value)), clue((error, reported)))
    }
  }
}
