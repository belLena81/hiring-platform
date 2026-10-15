package com.example.graphQL.cats.domain.workflow.characterization

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import scala.compiletime.constValueTuple
import scala.deriving.Mirror

/** One decision table: every row is a deterministic rendering of an input and the output of the pure function. */
final case class GoldenSection(title: String, rows: Vector[(String, String)])

object GoldenSection {
  def of[A](title: String, inputs: Iterable[A])(label: A => String)(result: A => String): GoldenSection =
    GoldenSection(title, inputs.iterator.map(input => (label(input), result(input))).toVector)
}

object GoldenCases {

  /** Case names of an enum in ordinal order, resolved at compile time so a new case cannot be overlooked. */
  inline def labels[E](using m: Mirror.SumOf[E]): Vector[String] =
    constValueTuple[m.MirroredElemLabels].toList.map(_.toString).toVector
}

/** Regenerate-and-compare support for characterization tables stored under `src/test/resources/saga-golden`.
  *
  * Normal runs only compare. Regeneration is an explicit opt-in: set `SAGA_GOLDEN_REGENERATE=true` (the forked test JVM
  * inherits the environment) or the `saga.golden.regenerate=true` system property.
  */
abstract class GoldenTableSuite extends munit.FunSuite {

  private val regenerate: Boolean =
    sys.props.get("saga.golden.regenerate").orElse(sys.env.get("SAGA_GOLDEN_REGENERATE")).contains("true")

  private val directory: Path = Paths.get(sys.props("user.dir"), "src", "test", "resources", "saga-golden")

  /** Fails when the generator produced no input for some case of the enum `E`. */
  protected def assertEveryCaseGenerated(what: String, labels: Vector[String], generated: Iterable[scala.reflect.Enum])(
      using munit.Location
  ): Unit = {
    val seen = generated.iterator.map(_.ordinal).toSet
    val missing = labels.indices.filterNot(seen).map(labels)
    assert(missing.isEmpty, s"$what: the generator does not cover enum case(s) ${missing.mkString(", ")}")
  }

  protected def assertGolden(fileName: String, sections: Vector[GoldenSection])(using munit.Location): Unit = {
    sections.foreach { section =>
      val inputs = section.rows.map(_._1)
      assertEquals(inputs.distinct.size, inputs.size, s"duplicate input rows in ${section.title}")
      assert(inputs.nonEmpty, s"empty table ${section.title}")
    }
    val actual = render(sections)
    val path = directory.resolve(fileName)
    if (regenerate) {
      Files.createDirectories(directory)
      Files.writeString(path, actual, UTF_8)
      ()
    } else if (!Files.exists(path))
      fail(
        s"Golden table $path is missing. Characterization tables are committed before any engine code exists. To " +
          "create it deliberately, run once with SAGA_GOLDEN_REGENERATE=true (or -Dsaga.golden.regenerate=true) and " +
          "review the file."
      )
    else {
      val expected = Files.readString(path, UTF_8)
      if (expected != actual) {
        val expectedLines = expected.linesIterator.toVector
        val actualLines = actual.linesIterator.toVector
        val differing = expectedLines.zipAll(actualLines, "<absent>", "<absent>").zipWithIndex.collect {
          case ((want, got), index) if want != got => s"line ${index + 1}\n  golden: $want\n  actual: $got"
        }
        fail(
          s"Golden table $fileName differs in ${differing.size} line(s) (golden ${expectedLines.size}, actual " +
            s"${actualLines.size}). First differences:\n${differing.take(10).mkString("\n")}"
        )
      }
    }
  }

  private def render(sections: Vector[GoldenSection]): String = {
    val header =
      Vector(
        "# Characterization golden table for the hiring saga orchestration (docs/specs/hiring-saga-orchestration.md).",
        "# One line per input: `input => output`. Do not edit by hand; later slices must reproduce it byte for byte.",
        ""
      )
    val body = sections.flatMap { section =>
      Vector(s"## ${section.title} (${section.rows.size} inputs)") ++
        section.rows.map { case (input, output) => s"$input => $output" } :+ ""
    }
    (header ++ body).mkString("\n") + "\n"
  }
}
