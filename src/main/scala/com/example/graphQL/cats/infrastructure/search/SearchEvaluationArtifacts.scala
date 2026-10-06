package com.example.graphQL.cats.infrastructure.search

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.shared.crypto.SourceHash
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

/** Local artifact adapter records the actual Scala working tree, including replay fixtures. */
object SearchEvaluationArtifacts {
  def sourceFingerprint: IO[String] =
    Resource.fromAutoCloseable(IO.blocking(Files.walk(Path.of("src")))).use { files =>
      IO.blocking {
        val paths = files
          .iterator()
          .asScala
          .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".scala"))
          .toList
          .sortBy(_.toString)
        SourceHash.sha256(
          paths.map(path => s"${path.toString}\n${Files.readString(path, StandardCharsets.UTF_8)}").mkString("\n")
        )
      }
    }
}
