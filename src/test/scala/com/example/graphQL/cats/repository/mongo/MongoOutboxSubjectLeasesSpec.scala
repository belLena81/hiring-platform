package com.example.graphQL.cats.repository.mongo

import cats.effect.IO

import cats.effect.Ref
import com.example.graphQL.cats.service.port.{RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import munit.CatsEffectSuite

final class MongoOutboxSubjectLeasesSpec extends CatsEffectSuite {
  test("acquisition sorts subjects and stops at the first typed conflict") {
    for {
      acquired <- Ref.of[cats.effect.IO, List[String]](Nil)
      result <- MongoOutboxSubjectLeases
        .acquire(List("c", "b", "a")) { subject =>
          RepositoryF.lift(acquired.update(_ :+ subject)).flatMap { _ =>
            RepositoryF.at[IO].fromEither(if (subject == "b") Left(RepositoryError.Conflict) else Right(()))
          }
        }
        .value
      seen <- acquired.get
    } yield {
      assertEquals(result, Left(RepositoryError.Conflict))
      assertEquals(seen, List("a", "b"))
    }
  }

  test("empty subjects fail without issuing a lease write") {
    for {
      acquired <- Ref.of[cats.effect.IO, List[String]](Nil)
      result <- MongoOutboxSubjectLeases.acquire(Nil)(subject => RepositoryF.lift(acquired.update(_ :+ subject))).value
      seen <- acquired.get
    } yield {
      assertEquals(result, Left(RepositoryError.InvalidStoredData))
      assertEquals(seen, Nil)
    }
  }

  test("successful acquisition preserves sorted sequential writes") {
    for {
      acquired <- Ref.of[cats.effect.IO, List[String]](Nil)
      result <- MongoOutboxSubjectLeases
        .acquire(List("c", "a", "b"))(subject => RepositoryF.lift(acquired.update(_ :+ subject)))
        .value
      seen <- acquired.get
    } yield {
      assertEquals(result, Right(()))
      assertEquals(seen, List("a", "b", "c"))
    }
  }
}
