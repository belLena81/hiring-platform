package com.example.graphQL.cats.service.search

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import io.circe.Json
import io.circe.parser.parse
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

/** Explicit Test/runMain entrypoint: tabulates recorded evaluation JSON without opening any client. */
object SearchEvaluationRecordTable extends IOApp {
  final case class Row(
      file: String,
      strategy: String,
      deployment: String,
      version: String,
      ok: Long,
      failed: Long,
      unavailable: Long,
      recall: Option[Double],
      ndcg: Option[Double],
      recallVsExact: Option[Double],
      p50: Option[Double],
      p95: Option[Double],
      p99: Option[Double]
  )

  /** The runners label loopback runs `local-container`; anything else is reported as Atlas-labelled evidence. */
  def deployment(identity: String): String =
    if (identity.contains("local-container")) "local-container" else "atlas-labelled"

  def row(file: String, json: Json): Either[String, Row] = {
    val root = json.hcursor
    val measurements = root.downField("measurements")
    def count(name: String): Either[String, Long] =
      measurements.get[Long](name).leftMap(_ => s"$file: missing measurements.$name")
    def number(cursor: io.circe.ACursor, name: String): Option[Double] =
      cursor.get[Option[Double]](name).toOption.flatten
    val latency = measurements.downField("successLatencyMillis")
    for {
      strategy <- root.get[String]("strategy").leftMap(_ => s"$file: missing strategy")
      identity <- root
        .downField("environment")
        .get[String]("identity")
        .leftMap(_ => s"$file: missing environment.identity")
      ok <- count("successfulQueries")
      failed <- count("failedQueries")
      unavailable <- count("unavailableQueries")
    } yield Row(
      file,
      strategy,
      deployment(identity),
      root.downField("environment").get[Option[String]]("atlasVersion").toOption.flatten.getOrElse("unknown"),
      ok,
      failed,
      unavailable,
      number(measurements.downField("successfulOnlyMeanRelevance"), "recallAtK"),
      number(measurements.downField("successfulOnlyMeanRelevance"), "ndcgAtK"),
      number(measurements, "meanRecallAtKAgainstExact"),
      number(latency, "p50"),
      number(latency, "p95"),
      number(latency, "p99")
    )
  }

  private def cell(value: Option[Double]): String = value.fold("n/a")(number => f"$number%.3f")

  def render(rows: List[Row]): String = {
    val header =
      "| file | strategy | deployment | version | ok | failed | unavailable | recall@K | NDCG@K | recall vs exact | p50 ms | p95 ms | p99 ms |\n" +
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|\n"
    val body = rows
      .sortBy(row => (row.strategy, row.file))
      .map(r =>
        s"| ${r.file} | ${r.strategy} | ${r.deployment} | ${r.version} | ${r.ok} | ${r.failed} | ${r.unavailable} | " +
          s"${cell(r.recall)} | ${cell(r.ndcg)} | ${cell(r.recallVsExact)} | ${cell(r.p50)} | ${cell(r.p95)} | ${cell(r.p99)} |"
      )
      .mkString("\n")
    val note =
      "\n\nlocal-container rows are correctness evidence only; they are not Atlas latency, recall or index-size measurements."
    header + body + note
  }

  def summarize(inputs: List[(String, String)]): Either[String, String] =
    inputs
      .traverse { case (file, text) => parse(text).leftMap(_ => s"$file: invalid JSON").flatMap(row(file, _)) }
      .map(render)

  override def run(args: List[String]): IO[ExitCode] = args match {
    case List("--input", directory) if Path.of(directory).normalize().startsWith(Path.of(".local/data")) =>
      IO.blocking {
        val stream = Files.list(Path.of(directory).normalize())
        try
          stream.iterator().asScala.toList.filter(path => Files.isRegularFile(path)).sortBy(_.getFileName.toString)
        finally stream.close()
      }.flatMap { files =>
        val evaluation = files.filter { path =>
          val name = path.getFileName.toString
          (name.startsWith("curated-") || name.startsWith("search-evaluation-")) && name.endsWith(".json")
        }
        evaluation
          .traverse(path =>
            IO.blocking(Files.readString(path, StandardCharsets.UTF_8)).map(path.getFileName.toString -> _)
          )
          .map(summarize)
          .flatMap {
            case Right(table) => IO.println(table).as(ExitCode.Success)
            case Left(error)  => IO.println(error).as(ExitCode.Error)
          }
      }
    case _ => IO.println("Use --input .local/data/search-evaluation").as(ExitCode.Error)
  }
}
