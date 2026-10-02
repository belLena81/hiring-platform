package com.example.hiring.analytics.cli

import java.net.URI
import java.nio.file.{Path, Paths}
import scala.util.Try
import scala.jdk.CollectionConverters.*

/** Pure identity checks shared by test activation and workload entrypoints. */
private[analytics] object StreamingProofIsolation {
  def independentCoverage(
      scope: Vector[String],
      covered: Vector[String],
      excluded: Vector[String],
      authored: Vector[String]
  ): Boolean =
    independentCoverageUnion(scope, Vector((covered, excluded, authored)))

  def independentCoverageUnion(
      scope: Vector[String],
      reviews: Vector[(Vector[String], Vector[String], Vector[String])]
  ): Boolean =
    scope.nonEmpty && reviews.nonEmpty && reviews.forall { case (covered, excluded, authored) =>
      covered.nonEmpty && !covered.exists(authored.contains) && !covered.exists(excluded.contains)
    } && scope.forall(reviews.flatMap(_._1).contains)

  def validate(
      database: String,
      topic: String,
      lakehouse: String,
      checkpoint: String,
      spill: String
  ): Either[String, Path] = {
    def local(value: String): Option[Path] = Try {
      val uri = URI.create(value)
      if (uri.getScheme == "file" && Option(uri.getHost).forall(_.isEmpty)) Paths.get(uri).toAbsolutePath.normalize()
      else if (uri.getScheme == null) Paths.get(value).toAbsolutePath.normalize()
      else throw new IllegalArgumentException("local file path required")
    }.toOption
    for {
      _ <- Either.cond(database.matches("hiring_streaming_proof_[a-f0-9]{16}"), (), "isolated database required")
      nonce = database.stripPrefix("hiring_streaming_proof_")
      _ <- Either.cond(topic == "hiring.streaming.proof." + nonce, (), "nonce-bound topic required")
      root <- local(lakehouse).toRight("local lakehouse required")
      cp <- local(checkpoint).toRight("local checkpoint required")
      scratch <- local(spill).toRight("local spill required")
      namespace = root.getParent
      _ <- Either.cond(
        root.getFileName.toString == "lakehouse" && namespace != null &&
          namespace.getFileName.toString == "hiring-streaming-proof-" + nonce,
        (),
        "nonce-bound lakehouse required"
      )
      _ <- Either.cond(
        owned(cp, "checkpoints", nonce) && owned(scratch, "spark-temp", nonce),
        (),
        "checkpoint and spill must use nonce scopes under their owned runtime roots"
      )
    } yield namespace
  }

  private def owned(path: Path, category: String, nonce: String): Boolean = {
    val parts = path.iterator().asScala.map(_.toString).toVector
    val suffix = Vector(category, "hiring-streaming-proof-" + nonce)
    val local = parts.sliding(5).exists(_ == (Vector(".local", "data", "analytics") ++ suffix))
    val docker = parts.sliding(4).exists(_ == Vector("var", "lib", "hiring-analytics", category)) &&
      path.startsWith(Paths.get("/var/lib/hiring-analytics", category, "hiring-streaming-proof-" + nonce))
    local || docker
  }
}
