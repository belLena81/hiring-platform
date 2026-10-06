package com.example.graphQL.cats.service.search

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.graphQL.cats.infrastructure.search.{SearchEvaluationArtifacts, SearchEvaluationReportJson}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.UUID

/** Explicit Test/runMain entrypoint. It replays authored branches and never opens a network client. */
object SearchEvaluationFixtureReplay extends IOApp {
  def captureAuthoredRun(
      strategy: SearchEvaluationStrategy,
      revision: String,
      fingerprint: String,
      timestamp: Instant,
      concurrency: Int
  ): IO[SearchEvaluationRun] =
    IO.parTraverseN(concurrency)(SearchEvaluationFixtures.queries)(fixture =>
      IO.delay(SearchEvaluationFixtures.observation(fixture, strategy))
    ).map(observations =>
      SearchEvaluationFixtures
        .emptyRun(strategy, revision, fingerprint, timestamp)
        .copy(concurrency = concurrency, queries = observations)
    )

  override def run(args: List[String]): IO[ExitCode] = args match {
    case List("--source-revision", revision) if revision.trim.nonEmpty => execute(revision, 1)
    case List("--source-revision", revision, "--concurrency", concurrency)
        if revision.trim.nonEmpty && Set("1", "8").contains(concurrency) =>
      execute(revision, if (concurrency == "8") 8 else 1)
    case _ =>
      IO.println(
        "Supply --source-revision and optional --concurrency 1 or 8; the Scala fingerprint is captured separately."
      ).as(ExitCode.Error)
  }

  private def execute(revision: String, concurrency: Int): IO[ExitCode] =
    for {
      fingerprint <- SearchEvaluationArtifacts.sourceFingerprint
      timestamp <- IO.realTimeInstant
      reports <- List(
        SearchEvaluationStrategy.Lexical,
        SearchEvaluationStrategy.Vector,
        SearchEvaluationStrategy.ApplicationRrf
      )
        .traverse(strategy =>
          captureAuthoredRun(strategy, revision, fingerprint, timestamp, concurrency).flatMap(run =>
            IO.fromEither(
              SearchEvaluationHarness
                .report(
                  SearchEvaluationFixtures.corpus,
                  run,
                  SearchEvaluationFixtures.K
                )
                .left
                .map(_ => new IllegalArgumentException("Invalid authored evaluation fixtures"))
            )
          )
        )
      _ <- reports
        .drop(1)
        .traverse_(report =>
          IO.fromEither(
            SearchEvaluationHarness
              .compare(reports.headOption.getOrElse(report), report)
              .left
              .map(_ => new IllegalArgumentException("Inconsistent fixture comparisons"))
          )
        )
      output <- IO.blocking {
        val output = Path
          .of(".local/data/search-evaluation")
          .resolve(s"fixture-${timestamp.toEpochMilli}-${fingerprint.take(12)}-${concurrency}-${UUID.randomUUID()}")
        Files.createDirectories(output)
        reports.foreach(report => {
          val path = output.resolve(s"fixture-${report.run.strategy.toString}.json")
          val _ = Files.writeString(path, SearchEvaluationReportJson.render(report).spaces2, StandardCharsets.UTF_8)
        })
        output
      }
      _ <- IO.println(
        s"Authored hiring fixtures replayed at concurrency $concurrency into $output; judgments remain provisional and adoption deferred."
      )
    } yield ExitCode.Success
}
