package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.search.{CandidateRetrievalHit, JobRetrievalHit}
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class MongoSearchFusionSpec extends CatsEffectSuite {
  test("job fusion starts both retrieval branches concurrently") {
    val fusion = for {
      vectorStarted <- Deferred[IO, Unit]
      lexicalStarted <- Deferred[IO, Unit]
      vector = RepositoryF.lift(vectorStarted.complete(()) *> lexicalStarted.get).as(List.empty[JobRetrievalHit])
      lexical = RepositoryF.lift(lexicalStarted.complete(()) *> vectorStarted.get).as(List.empty[JobRetrievalHit])
      result <- MongoSemanticSearchResult.fuseJobs(vector, lexical, 10).value
    } yield assertEquals(result, Right(Nil))
    fusion.timeout(5.seconds)
  }

  test("candidate fusion starts all three retrieval branches concurrently") {
    val fusion = for {
      jobStarted <- Deferred[IO, Unit]
      queryStarted <- Deferred[IO, Unit]
      lexicalStarted <- Deferred[IO, Unit]
      job = RepositoryF
        .lift(jobStarted.complete(()) *> queryStarted.get *> lexicalStarted.get)
        .as(List.empty[CandidateRetrievalHit])
      query = RepositoryF
        .lift(queryStarted.complete(()) *> jobStarted.get *> lexicalStarted.get)
        .as(List.empty[CandidateRetrievalHit])
      lexical = RepositoryF
        .lift(lexicalStarted.complete(()) *> jobStarted.get *> queryStarted.get)
        .as(List.empty[CandidateRetrievalHit])
      result <- MongoSemanticSearchResult.fuseCandidates(job, query, lexical, 10).value
    } yield assertEquals(result, Right(Nil))
    fusion.timeout(5.seconds)
  }

  test("job fusion normalizes each branch's typed error to unavailable") {
    val successful = RepositoryF.at[IO].pure(List.empty[JobRetrievalHit])
    val invalid: RepositoryF[IO, List[JobRetrievalHit]] = RepositoryF.at[IO].raise(RepositoryError.InvalidStoredData)
    for {
      vectorFailure <- MongoSemanticSearchResult.fuseJobs(invalid, successful, 10).value
      lexicalFailure <- MongoSemanticSearchResult.fuseJobs(successful, invalid, 10).value
    } yield {
      assertEquals(vectorFailure, Left(RepositoryError.Unavailable))
      assertEquals(lexicalFailure, Left(RepositoryError.Unavailable))
    }
  }

  test("candidate fusion normalizes each branch's typed error to unavailable") {
    val successful = RepositoryF.at[IO].pure(List.empty[CandidateRetrievalHit])
    val invalid: RepositoryF[IO, List[CandidateRetrievalHit]] =
      RepositoryF.at[IO].raise(RepositoryError.InvalidStoredData)
    List(
      MongoSemanticSearchResult.fuseCandidates(invalid, successful, successful, 10),
      MongoSemanticSearchResult.fuseCandidates(successful, invalid, successful, 10),
      MongoSemanticSearchResult.fuseCandidates(successful, successful, invalid, 10)
    ).traverse(_.value).map { results =>
      results.foreach(result => assertEquals(result, Left(RepositoryError.Unavailable)))
    }
  }
}
