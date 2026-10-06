package com.example.graphQL.cats.service.search

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import scala.concurrent.duration.*

class SearchEvaluationCaptureSpec extends munit.CatsEffectSuite {
  private val corpus = SearchEvaluationFixtures.corpus
  private def adapter(
      f: SearchEvaluationFixtureQuery => IO[Either[SearchEvaluationFailure, List[String]]]
  ): SearchEvaluationCapture.Retrieval =
    new SearchEvaluationCapture.Retrieval {
      def retrieve(query: SearchEvaluationFixtureQuery, strategy: SearchEvaluationStrategy) = f(query)
    }

  test("supported captures preserve corpus order and skip unsupported recommendation filters and modes") {
    for {
      calls <- Ref.of[IO, List[String]](Nil)
      retrieval = adapter(query => calls.update(_ :+ query.queryId).as(Right(query.eligibleIds.take(7))))
      result <- SearchEvaluationCapture.capture(
        corpus,
        SearchEvaluationStrategy.Vector,
        1,
        1.second,
        Resource.pure(retrieval)
      )
      observed <- calls.get
    } yield {
      val values = result.fold(error => fail(error.toString), identity)
      assertEquals(values.map(_.queryId), corpus.queries.map(_.queryId))
      assertEquals(observed.size, 10)
      assertEquals(values.count(_.ranking.isInstanceOf[SearchEvaluationRanking.Unavailable]), 2)
      assert(!observed.contains("Recommendations-1"))
      assert(!observed.contains("Recommendations-3"))
    }
  }

  test("invalid identities and label subsets reject before resource acquisition") {
    for {
      acquired <- Ref.of[IO, Int](0)
      resource = Resource.eval(acquired.update(_ + 1).as(adapter(_ => IO.pure(Right(Nil)))))
      invalid = List(
        corpus.copy(queries = Nil),
        corpus.copy(entityIds = corpus.entityIds :+ corpus.entityIds.head),
        corpus.copy(queries = corpus.queries :+ corpus.queries.head),
        corpus.copy(queries = corpus.queries.map(_.copy(relevantIds = Set("outside-corpus"))))
      )
      results <- invalid.traverse(
        SearchEvaluationCapture.capture(_, SearchEvaluationStrategy.Vector, 1, 1.second, resource)
      )
      count <- acquired.get
    } yield { assert(results.forall(_.isLeft)); assertEquals(count, 0) }
  }

  test("unexpected retrieval details and oversized/ineligible rankings become sanitized typed failures") {
    val one = corpus.copy(queries = List(corpus.queries.head))
    List(
      adapter(_ => IO.raiseError(new RuntimeException("mongodb://secret:password@private"))),
      adapter(_ => IO.pure(Right(List("outside-corpus")))),
      adapter(query => IO.pure(Right(query.eligibleIds.take(2)))),
      adapter(query => IO.pure(Right(List(query.eligibleIds.head, query.eligibleIds.head))))
    ).traverse(resource =>
      SearchEvaluationCapture
        .capture(one, SearchEvaluationStrategy.Vector, 1, 1.second, Resource.pure(resource), maximumResults = 1)
    ).map { results =>
      results.foreach { result =>
        val rankings = result.fold(error => fail(error.toString), identity).map(_.ranking)
        assert(rankings.forall {
          case SearchEvaluationRanking.Failed(SearchEvaluationFailure.RetrievalFailed, Some(_)) => true
          case _                                                                                => false
        })
        assert(!rankings.toString.contains("password"))
      }
    }
  }

  test("timeout cancels callback and releases adapter") {
    for {
      canceled <- Ref.of[IO, Boolean](false)
      released <- Ref.of[IO, Boolean](false)
      resource = Resource.make(IO.pure(adapter(_ => IO.never.onCancel(canceled.set(true)))))(_ => released.set(true))
      result <- SearchEvaluationCapture.capture(
        corpus.copy(queries = corpus.queries.take(1)),
        SearchEvaluationStrategy.Vector,
        1,
        10.millis,
        resource
      )
      cancel <- canceled.get
      release <- released.get
    } yield {
      assert(cancel && release)
      assert(result.toOption.toList.flatten.forall(_.ranking match {
        case SearchEvaluationRanking.Failed(SearchEvaluationFailure.TimedOut, Some(_)) => true
        case _                                                                         => false
      }))
    }
  }

  test("caller cancellation awaits resource release") {
    for {
      started <- Deferred[IO, Unit]
      released <- Deferred[IO, Unit]
      resource = Resource.make(IO.pure(adapter(_ => started.complete(()).void *> IO.never)))(_ =>
        released.complete(()).void
      )
      fiber <- SearchEvaluationCapture.capture(corpus, SearchEvaluationStrategy.Vector, 1, 1.second, resource).start
      _ <- started.get
      _ <- fiber.cancel
      _ <- released.get
    } yield ()
  }

  test("concurrency eight runs one bounded batch with deterministic result order") {
    val eight = corpus.copy(queries = corpus.queries.filter(_.useCase != SearchEvaluationUseCase.Recommendations))
    for {
      active <- Ref.of[IO, Int](0)
      maximum <- Ref.of[IO, Int](0)
      allEntered <- Deferred[IO, Unit]
      retrieval = adapter(query =>
        Resource
          .make(
            active
              .updateAndGet(_ + 1)
              .flatTap(count => maximum.update(_.max(count)))
              .flatTap(count => if (count == 8) allEntered.complete(()).void else IO.unit)
          )(_ => active.update(_ - 1))
          .use(_ => allEntered.get.as(Right(query.eligibleIds.take(7))))
      )
      result <- SearchEvaluationCapture.capture(
        eight,
        SearchEvaluationStrategy.Vector,
        8,
        2.seconds,
        Resource.pure(retrieval)
      )
      peak <- maximum.get
      remaining <- active.get
    } yield {
      assertEquals(peak, 8)
      assertEquals(remaining, 0)
      assertEquals(result.toOption.toList.flatten.map(_.queryId), eight.queries.map(_.queryId))
    }
  }
}
