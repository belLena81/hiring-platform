package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.example.graphQL.cats.config.{PersistedQueryConfig, PersistedQueryMode}
import sangria.ast
import sangria.parser.QueryParser

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.HexFormat
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Admission of GraphQL documents by identity. Full documents only: the key is the lowercase hex SHA-256 over the exact
  * UTF-8 bytes of the `query` string, with no normalization. Introspection-only documents are exempt and remain
  * governed by `graphql.schema-exposure`.
  */
sealed trait PersistedQueryPolicy {
  def admits(query: String): Boolean
}

object PersistedQueryPolicy {
  case object Off extends PersistedQueryPolicy {
    def admits(query: String): Boolean = true
  }

  final case class Allowlist(hashes: Set[String]) extends PersistedQueryPolicy {
    def admits(query: String): Boolean = hashes.contains(hash(query)) || introspectionOnly(query)
  }

  enum ManifestError {
    case Unreadable
    case Empty
    case MalformedEntry(line: Int)
  }

  def hash(query: String): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(query.getBytes(StandardCharsets.UTF_8)))

  private val HashPattern = "[0-9a-f]{64}".r
  private val IntrospectionRoots = Set("__schema", "__type", "__typename")

  /** One hash per line; blank lines and `#` comments are ignored. */
  def parseManifest(text: String): Either[ManifestError, Allowlist] = {
    val entries = text.linesIterator.zipWithIndex
      .map { case (line, index) => (line.trim, index + 1) }
      .filterNot { case (line, _) => line.isEmpty || line.startsWith("#") }
      .toList
    entries
      .collectFirst { case (line, number) if !HashPattern.matches(line) => ManifestError.MalformedEntry(number) }
      .toLeft(entries.map(_._1).toSet)
      .flatMap(hashes => Either.cond(hashes.nonEmpty, Allowlist(hashes), ManifestError.Empty))
  }

  /** Reads the manifest once at startup. `off` needs no file. */
  def load(config: PersistedQueryConfig): IO[Either[ManifestError, PersistedQueryPolicy]] =
    config.mode match {
      case PersistedQueryMode.Off       => IO.pure(Right(Off))
      case PersistedQueryMode.Allowlist =>
        config.manifest match {
          case None       => IO.pure(Left(ManifestError.Unreadable))
          case Some(path) =>
            IO.blocking(Files.readAllLines(Path.of(path), StandardCharsets.UTF_8).asScala.mkString("\n")).attempt.map {
              case Left(_)     => Left(ManifestError.Unreadable)
              case Right(text) => parseManifest(text)
            }
        }
    }

  /** A document whose every definition is an operation (no fragments or other definitions) with distinct names, and
    * whose every root selection is `__schema`, `__type` or `__typename`. Walking `definitions` keeps duplicate
    * operation names from collapsing into one. The cheap `__` test keeps ordinary unlisted documents from being parsed
    * at all. Any parse failure, including stack exhaustion on hostile nesting, means "not exempt".
    */
  private def introspectionOnly(query: String): Boolean =
    query.contains("__") && (
      try QueryParser.parse(query).toOption.exists(introspectionOnly)
      catch {
        case _: StackOverflowError => false
        case NonFatal(_)           => false
      }
    )

  private def introspectionOnly(document: ast.Document): Boolean = {
    val operations = document.definitions.collect { case operation: ast.OperationDefinition => operation }
    operations.nonEmpty && operations.size == document.definitions.size &&
    operations.map(_.name).distinct.size == operations.size &&
    operations.forall(operation =>
      operation.selections.nonEmpty && operation.selections.forall {
        case field: ast.Field => IntrospectionRoots.contains(field.name)
        case _                => false
      }
    )
  }
}
