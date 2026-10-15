package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import io.circe.Json
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Security F4: fragment-heavy documents must be refused or finish quickly, before any resolver runs. The cost of
  * validation and the query reducers is measured through the real executor.
  */
final class HiringGraphQLDocumentExpansionSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 60.seconds

  private val Bound = 3.seconds

  private def fragmentName(level: Int) = s"Level$level"

  /** `levels` fragments on Query, each with `aliases` aliased selections, spreading either its successor or every later
    * fragment (a DAG with exponentially many spread paths).
    */
  private def document(
      levels: Int,
      aliases: Int,
      spreadsAllLater: Boolean,
      sharedResponseNames: Boolean = false
  ): String = {
    val fragments = (0 until levels).map { level =>
      val fields = (0 until aliases)
        .map(alias => s"${if (sharedResponseNames) "" else s"l$level"}a$alias: me { id }")
        .mkString(" ")
      val spreads =
        if (spreadsAllLater) ((level + 1) until levels).map(next => s"...${fragmentName(next)}")
        else if (level + 1 < levels) Seq(s"...${fragmentName(level + 1)}")
        else Nil
      s"fragment ${fragmentName(level)} on Query { $fields ${spreads.mkString(" ")} }"
    }
    s"{ ...${fragmentName(0)} }\n${fragments.mkString("\n")}"
  }

  private def timedExecution(query: String): IO[(FiniteDuration, Either[HiringGraphQLSchema.Failure, Json])] =
    TestGraphQLSupport
      .context(schemaExposure = false)
      .use(context =>
        TestGraphQLSupport.parseAndExecute(GraphQLRequest(query, Json.obj(), None), context).timed.map {
          case (elapsed, result) => (elapsed, result)
        }
      )

  // The first execution in a JVM pays class loading and JIT; warm up so the bound measures the document, not startup.
  private def assertBounded(label: String, query: String): IO[Unit] =
    (timedExecution("{ __typename }") *> timedExecution(query)).map { case (elapsed, result) =>
      println(
        s"F4 probe $label: ${query.length} bytes, ${elapsed.toMillis} ms, ${result.fold(_.toString, _ => "executed")}"
      )
      assert(elapsed < Bound, s"$label took $elapsed")
    }

  test("F4 a 16-level fragment chain with 3 aliases per level is refused or finishes within the bound") {
    assertBounded("chain 16x3", document(levels = 16, aliases = 3, spreadsAllLater = false))
  }

  test("F4 a 16-level fragment DAG spreading every later fragment is refused or finishes within the bound") {
    assertBounded("dag 16x3", document(levels = 16, aliases = 3, spreadsAllLater = true))
  }

  test("F4 a deeper and wider fragment DAG within the request size limit is refused or finishes within the bound") {
    val query = document(levels = 60, aliases = 8, spreadsAllLater = true)
    assert(query.length < 64 * 1024, s"probe is ${query.length} bytes")
    assertBounded("dag 60x8", query)
  }

  test("F4 thousands of aliases for one field, and thousands of repeats of one response name, stay within the bound") {
    val distinct = (0 until 1500).map(index => s"a$index: me { id }").mkString("{ ", " ", " }")
    val repeated = (0 until 1500).map(_ => "a: me { id }").mkString("{ ", " ", " }")
    assert(distinct.length < 64 * 1024 && repeated.length < 64 * 1024)
    assertBounded("1500 distinct aliases", distinct) *> assertBounded("1500 repeated response names", repeated)
  }

  test("F4 the same response names reused across every level of a fragment DAG stay within the bound") {
    val query = document(levels = 60, aliases = 8, spreadsAllLater = true, sharedResponseNames = true)
    assert(query.length < 64 * 1024, s"probe is ${query.length} bytes")
    assertBounded("dag 60x8 shared response names", query)
  }
}
