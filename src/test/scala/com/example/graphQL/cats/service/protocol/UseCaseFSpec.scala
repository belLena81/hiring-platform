package com.example.graphQL.cats.service.protocol

import cats.Id
import cats.effect.IO
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.{RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.port.{RepositoryF}
import munit.CatsEffectSuite

final class UseCaseFSpec extends CatsEffectSuite {

  test("EPS-03.1 ensure succeeds when the condition holds and fails with the given error otherwise") {
    assertEquals(UseCaseF.ensure[Id](true, UseCaseError.Domain(DomainError.Forbidden)).value, Right(()))
    assertEquals(
      UseCaseF.ensure[Id](false, UseCaseError.Domain(DomainError.Forbidden)).value,
      Left(UseCaseError.Domain(DomainError.Forbidden))
    )
  }

  test("EPS-03.1 found maps absence to NotFound for the entity and presence to the value") {
    assertEquals(UseCaseF.found(RepositoryF.pure[Id, Option[Int]](Some(7)), "job").value, Right(7))
    assertEquals(
      UseCaseF.found(RepositoryF.pure[Id, Option[Int]](None), "job").value,
      Left(UseCaseError.Domain(DomainError.NotFound("job")))
    )
  }

  test("EPS-03.1 repository maps AuthorityRevoked to Forbidden and keeps every other repository error") {
    assertEquals(
      UseCaseF.repository(RepositoryF.raise[Id, Int](RepositoryError.AuthorityRevoked)).value,
      Left(UseCaseError.Domain(DomainError.Forbidden))
    )
    assertEquals(
      UseCaseF.repository(RepositoryF.raise[Id, Int](RepositoryError.Conflict)).value,
      Left(UseCaseError.Repository(RepositoryError.Conflict))
    )
    assertEquals(UseCaseF.repositoryFromFEither[Id, Int](Right(2)).value, Right(2))
  }

  test("EPS-03.1 the helpers keep the same mapping at IO") {
    for {
      revoked <- UseCaseF.repository(RepositoryF.at[IO].raise[Int](RepositoryError.AuthorityRevoked)).value
      missing <- UseCaseF.found(RepositoryF.at[IO].pure(Option.empty[Int]), "application").value
      fromIO <- UseCaseF
        .repositoryFromFEither(IO.pure(Left(RepositoryError.Conflict): Either[RepositoryError, Int]))
        .value
      ensured <- UseCaseF.ensure[IO](false, UseCaseError.Domain(DomainError.Forbidden)).value
    } yield {
      assertEquals(revoked, Left(UseCaseError.Domain(DomainError.Forbidden)))
      assertEquals(missing, Left(UseCaseError.Domain(DomainError.NotFound("application"))))
      assertEquals(fromIO, Left(UseCaseError.Repository(RepositoryError.Conflict)))
      assertEquals(ensured, Left(UseCaseError.Domain(DomainError.Forbidden)))
    }
  }
}
