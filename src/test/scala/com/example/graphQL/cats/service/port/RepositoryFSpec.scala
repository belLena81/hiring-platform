package com.example.graphQL.cats.service.port

import cats.Id
import cats.effect.IO
import com.example.graphQL.cats.service.RepositoryError
import munit.CatsEffectSuite

final class RepositoryFSpec extends CatsEffectSuite {

  test("EPS-03.1 repository helpers build the same results for an effect that is not IO") {
    assertEquals(RepositoryF.pure[Id, Int](3).value, Right(3))
    assertEquals(RepositoryF.raise[Id, Int](RepositoryError.Conflict).value, Left(RepositoryError.Conflict))
    assertEquals(
      RepositoryF.fromEither[Id, Int](Left(RepositoryError.Unavailable)).value,
      Left(RepositoryError.Unavailable)
    )
    assertEquals(RepositoryF.lift[Id, Int](4).value, Right(4))
    assertEquals(RepositoryF.fromFEither[Id, Int](Right(5)).value, Right(5))
  }

  test("EPS-03.1 the helpers behave the same at IO as in any other effect") {
    for {
      pure <- RepositoryF.at[IO].pure(1).value
      raised <- RepositoryF.at[IO].raise[Int](RepositoryError.Conflict).value
      fromEither <- RepositoryF.at[IO].fromEither[Int](Left(RepositoryError.Unavailable)).value
      lifted <- RepositoryF.lift(IO.pure(2)).value
      fromIO <- RepositoryF.fromFEither(IO.pure(Right(3))).value
    } yield {
      assertEquals(pure, Right(1))
      assertEquals(raised, Left(RepositoryError.Conflict))
      assertEquals(fromEither, Left(RepositoryError.Unavailable))
      assertEquals(lifted, Right(2))
      assertEquals(fromIO, Right(3))
    }
  }
}
