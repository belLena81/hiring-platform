package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{GraphQLOutcomeRecorder, GraphQLRootFieldOutcome}
import org.typelevel.otel4s.{Attribute, Attributes}
import org.typelevel.otel4s.metrics.MeterProvider

/** The GraphQL outcome counter.
  *
  * Instrument `hiring.graphql.root_field.executions` (`{execution}`, counter). Attributes, all from closed sets:
  *   - `graphql.operation.type`: `query`, `mutation` or `_unknown`.
  *   - `hiring.graphql.root_field`: a Query or Mutation field name of the schema, or `_introspection`, `_unknown`,
  *     `_invalid`.
  *   - `hiring.graphql.outcome`: `success`, `field_error`, `denied`, `unauthenticated`, `rejected`, `internal`.
  *
  * Counting rule: see `GraphQLOperationOutcomes`.
  */
object GraphQLOutcomeMetrics {
  private val MeterName = "hiring-platform"
  val InstrumentName = "hiring.graphql.root_field.executions"

  private[telemetry] def attributes(outcome: GraphQLRootFieldOutcome): Attributes =
    Attributes(
      Attribute("graphql.operation.type", outcome.kind.label),
      Attribute("hiring.graphql.root_field", outcome.rootField),
      Attribute("hiring.graphql.outcome", outcome.outcome.label)
    )

  def create(meterProvider: MeterProvider[IO]): IO[GraphQLOutcomeRecorder] =
    meterProvider
      .get(MeterName)
      .flatMap(
        _.counter[Long](InstrumentName)
          .withUnit("{execution}")
          .withDescription(
            "GraphQL root field executions by schema field name and bounded outcome class. Field errors return HTTP 200."
          )
          .create
      )
      .map { counter =>
        new GraphQLOutcomeRecorder {
          // A metrics failure must never replace the GraphQL response.
          override def record(outcomes: List[GraphQLRootFieldOutcome]): IO[Unit] =
            outcomes.traverse_(outcome => counter.inc(attributes(outcome))).handleError(_ => ())
        }
      }
}
