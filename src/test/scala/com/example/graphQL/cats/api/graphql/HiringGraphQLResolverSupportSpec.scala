package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.DomainError
import com.example.graphQL.cats.domain.error.{DomainError as DomainFailure}
import com.example.graphQL.cats.repository.protocol.{MutationEntityReference, MutationReceiptExecution, MutationReceiptFingerprint, MutationReceiptRepository, MutationReceiptWrite, MutationWriteContext, RepositoryError}
import com.example.graphQL.cats.service.{AccountError, AuthenticationError, AvailabilityError, SearchError, UseCaseError}
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

final class HiringGraphQLResolverSupportSpec extends CatsEffectSuite {
  test("uses one failure mapping to classify expected and exceptional errors") {
    val expected = List(
      UseCaseError.Account(AccountError.NameTaken),
      UseCaseError.Domain(DomainFailure.NotFound("job")),
      UseCaseError.Repository(RepositoryError.Conflict),
      UseCaseError.ValidationFailed(cats.data.NonEmptyList.one(com.example.graphQL.cats.domain.error.DomainValidationError.BlankField("name")))
    )
    val exceptional = List(
      UseCaseError.Authentication(AuthenticationError.Unauthorized),
      UseCaseError.Availability(AvailabilityError.ServiceNotReady),
      UseCaseError.Domain(DomainFailure.Forbidden),
      UseCaseError.Repository(RepositoryError.Unavailable),
      UseCaseError.Search(SearchError.ProviderUnavailable)
    )

    IO {
      assert(expected.forall(error => !HiringGraphQLResolverSupport.toGraphQLFailure(error).exceptional))
      assert(exceptional.forall(error => HiringGraphQLResolverSupport.toGraphQLFailure(error).exceptional))
    }
  }

  test("turns expected mutation failures into typed domain errors") {
    for {
      account <- HiringGraphQLResolverSupport.mutationResult(
        Left(UseCaseError.Account(AccountError.NameTaken)): Either[UseCaseError, String])
      repository <- HiringGraphQLResolverSupport.mutationResult(
        Left(UseCaseError.Repository(RepositoryError.Conflict)): Either[UseCaseError, String])
    } yield {
      assertEquals(account, DomainError("REGISTRATION_FAILED", "Registration failed"))
      assertEquals(repository, DomainError("CONFLICT", "Conflict"))
    }
  }

  test("raises exceptional mutation failures as read failures") {
    val error = UseCaseError.Repository(RepositoryError.Unavailable)
    HiringGraphQLResolverSupport.mutationResult(Left(error): Either[UseCaseError, String]).attempt.map {
      case Left(RequestContext.ReadFailure(actual)) => assertEquals(actual, error)
      case result => fail(s"Expected a read failure, received $result")
    }
  }

  test("passes the receipt write context to the mutation callback") {
    val expected = new MutationWriteContext {}
    val receipts = new MutationReceiptRepository {
      override def execute[A, E](
          key: com.example.graphQL.cats.repository.protocol.MutationReceiptKey,
          fingerprint: MutationReceiptFingerprint,
          now: Instant,
          expiresAt: Instant
      )(write: MutationWriteContext => IO[Either[RepositoryError, Either[E, MutationReceiptWrite[A]]]]):
          IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] =
        val _ = (key, fingerprint, now, expiresAt)
        write(expected).map(_.map(_.fold(MutationReceiptExecution.Rejected(_), value => MutationReceiptExecution.Applied(value.value, value.entity))))
    }
    val services = TestGraphQLSupport.emptyServices.copy(mutationReceipts = receipts)
    HiringGraphQLResolverSupport.executeMutation[String](
      services,
      "createJob",
      "actor-1",
      UUID.fromString("00000000-0000-0000-0000-000000000010"),
      Json.fromString("input"),
      value => MutationEntityReference("job", value),
      reference => IO.pure(Right(reference.entityId))
    )(context => IO.pure(Right(if (context eq expected) "same-context" else "different-context"))).map { result =>
      assertEquals(result, Right("same-context"))
    }
  }
}
