package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite
import org.bson.Document

final class MongoBatchScanSpec extends CatsEffectSuite {
  private def rows(count: Int): List[Document] = (1 to count).toList.map(i => new Document("_id", f"$i%02d"))
  private def page(all: List[Document], size: Int)(after: Option[String]): IO[List[Document]] =
    IO.pure(all.filter(row => after.forall(row.getString("_id") > _)).take(size))
  private def key(row: Document): IO[String] = IO.pure(row.getString("_id"))

  private def scan(all: List[Document], size: Int) =
    for {
      seen <- Ref.of[IO, List[List[String]]](Nil)
      fetched <- Ref.of[IO, List[Option[String]]](Nil)
      _ <- MongoBatchScan.byId(size, Option.empty[String])(after =>
        fetched.update(_ :+ after) *> page(all, size)(after)
      )(
        key
      )(rows => seen.update(_ :+ rows.map(_.getString("_id"))))
      pages <- seen.get
      cursors <- fetched.get
    } yield (pages, cursors)

  test("pages continue from the last key until a short page ends the scan") {
    scan(rows(7), 3).map { case (pages, cursors) =>
      assertEquals(pages, List(List("01", "02", "03"), List("04", "05", "06"), List("07")))
      assertEquals(cursors, List(None, Some("03"), Some("06")))
    }
  }

  test("a full last page needs one more fetch to find the end") {
    scan(rows(6), 3).map { case (pages, cursors) =>
      assertEquals(pages.last, Nil)
      assertEquals(cursors, List(None, Some("03"), Some("06")))
    }
  }

  test("an empty collection still gives the handler one empty page") {
    scan(Nil, 3).map(result => assertEquals(result, (List(Nil), List(None))))
  }

  test("a failing handler stops the scan before the next page is fetched") {
    for {
      fetched <- Ref.of[IO, Int](0)
      outcome <- MongoBatchScan
        .byId(3, Option.empty[String])(after => fetched.update(_ + 1) *> page(rows(9), 3)(after))(key)(_ =>
          IO.raiseError(new IllegalStateException("rejected"))
        )
        .attempt
      count <- fetched.get
    } yield {
      assert(outcome.isLeft)
      assertEquals(count, 1)
    }
  }
}
