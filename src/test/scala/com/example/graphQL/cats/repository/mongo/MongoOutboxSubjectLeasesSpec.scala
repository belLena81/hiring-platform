package com.example.graphQL.cats.repository.mongo

import cats.effect.Ref
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import munit.CatsEffectSuite

final class MongoOutboxSubjectLeasesSpec extends CatsEffectSuite {
  test("acquisition sorts subjects and stops at the first typed conflict") {
    for {
      acquired <- Ref.of[cats.effect.IO, List[String]](Nil)
      result <- MongoOutboxSubjectLeases
        .acquire(List("c", "b", "a")) { subject =>
          RepositoryIO.lift(acquired.update(_ :+ subject)).flatMap { _ =>
            RepositoryIO.fromEither(if (subject == "b") Left(RepositoryError.Conflict) else Right(()))
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
      result <- MongoOutboxSubjectLeases.acquire(Nil)(subject => RepositoryIO.lift(acquired.update(_ :+ subject))).value
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
        .acquire(List("c", "a", "b"))(subject => RepositoryIO.lift(acquired.update(_ :+ subject)))
        .value
      seen <- acquired.get
    } yield {
      assertEquals(result, Right(()))
      assertEquals(seen, List("a", "b", "c"))
    }
  }
}
