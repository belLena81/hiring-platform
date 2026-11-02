package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import io.circe.{ACursor, Json}
import sangria.ast
import sangria.schema.Schema

/** The bounded result of one GraphQL root field. HTTP status is no help here: field errors return HTTP 200.
  *
  * Classes are derived from the sanitized error codes in [[GraphQLFailureCatalog]], never from message text. Their
  * severity order (highest first) decides the class of a root field that carries several errors.
  */
enum GraphQLOutcome(val label: String, val severity: Int) {

  /** The root field completed without an error entry. A typed business payload is not an error entry. */
  case Success extends GraphQLOutcome("success", 0)

  /** An expected business or input refusal (validation, not found, conflict, invalid transition, bad cursor...). */
  case FieldError extends GraphQLOutcome("field_error", 1)

  /** A role or ownership refusal. */
  case Denied extends GraphQLOutcome("denied", 2)

  /** Missing, invalid or revoked credentials. */
  case Unauthenticated extends GraphQLOutcome("unauthenticated", 3)

  /** Refused before or instead of execution by admission: rate limit, cost budget, invalid request. */
  case Rejected extends GraphQLOutcome("rejected", 4)

  /** A dependency or capability failure, an unrecognised error code, or an unexpected execution failure. */
  case Internal extends GraphQLOutcome("internal", 5)
}

/** `graphql.operation.type`: only the types the schema defines, collapsing everything else. */
enum GraphQLOperationKind(val label: String) {
  case Query extends GraphQLOperationKind("query")
  case Mutation extends GraphQLOperationKind("mutation")
  case Unknown extends GraphQLOperationKind("_unknown")
}

/** One counted root field execution. `rootField` is a schema field name or one of the collapse values below. */
final case class GraphQLRootFieldOutcome(kind: GraphQLOperationKind, rootField: String, outcome: GraphQLOutcome)

/** The port the HTTP layer reports root field outcomes to. An implementation must not fail the request. */
trait GraphQLOutcomeRecorder {
  def record(outcomes: List[GraphQLRootFieldOutcome]): IO[Unit]
}

object GraphQLOutcomeRecorder {
  val noop: GraphQLOutcomeRecorder = _ => IO.unit
}

/** Counting rule for GraphQL root fields.
  *
  *   - The unit is a root field of the selected operation (`operationName` picks it), counted once per distinct
  *     response key (the alias, or the field name when there is none), because the executor merges equal keys and runs
  *     them once. Two aliases of one field are two executions and are two counts. The label is the schema field name,
  *     never the alias, the caller's operation name or a document hash.
  *   - Fields reached through root level fragments are included. A field under `@skip` or `@include` counts only when
  *     its response key appears in `data` or in an error path (or when an error has no path), because the condition is
  *     not evaluated here.
  *   - The outcome of a field is the most severe class among the errors whose `path` starts with its response key plus
  *     the errors without a path (a request level refusal applies to every root field); with no such error it is
  *     [[GraphQLOutcome.Success]].
  *   - Collapsed names: `__schema`, `__type` and `__typename` are `_introspection`; a field the schema does not define
  *     is `_unknown`; a request that never reached execution as a valid document (parse failure, invalid document,
  *     persisted query miss, missing operation) is one `_invalid` count with [[GraphQLOutcome.Rejected]].
  */
object GraphQLOperationOutcomes {
  val Introspection = "_introspection"
  val UnknownField = "_unknown"
  val InvalidRequestField = "_invalid"

  /** One count for a request that did not reach execution as a valid document. */
  val invalidRequest: List[GraphQLRootFieldOutcome] =
    List(GraphQLRootFieldOutcome(GraphQLOperationKind.Unknown, InvalidRequestField, GraphQLOutcome.Rejected))

  private final case class RootSelection(name: String, responseKey: String, conditional: Boolean)

  private val ConditionalDirectives = Set("skip", "include")

  /** Classifies an executed response (the JSON the client receives). */
  def classify(
      document: ast.Document,
      operationName: Option[String],
      response: Json,
      schema: Schema[?, ?] = HiringGraphQLSchema.schema
  ): List[GraphQLRootFieldOutcome] =
    selections(document, operationName) match {
      case None                => List(invalidResponse(response))
      case Some((kind, roots)) =>
        val dataKeys = response.hcursor.downField("data").keys.map(_.toSet).getOrElse(Set.empty[String])
        val errors = errorEntries(response.hcursor.downField("errors"))
        val global = errors.collect { case (None, outcome) => outcome }
        val keyed = errors.collect { case (Some(key), outcome) => key -> outcome }.groupMap(_._1)(_._2)
        roots
          .filter(root =>
            !root.conditional || dataKeys(root.responseKey) || keyed.contains(root.responseKey) || global.nonEmpty
          )
          .map { root =>
            val outcome = (keyed.getOrElse(root.responseKey, Nil) ++ global).maxByOption(_.severity)
            GraphQLRootFieldOutcome(
              kind,
              boundedName(kind, root.name, schema),
              outcome.getOrElse(GraphQLOutcome.Success)
            )
          }
    }

  /** Every root field of a valid document refused with one outcome, for example an exhausted cost budget. */
  def refused(
      document: ast.Document,
      operationName: Option[String],
      outcome: GraphQLOutcome,
      schema: Schema[?, ?] = HiringGraphQLSchema.schema
  ): List[GraphQLRootFieldOutcome] =
    selections(document, operationName) match {
      case None                => invalidRequest
      case Some((kind, roots)) =>
        roots.map(root => GraphQLRootFieldOutcome(kind, boundedName(kind, root.name, schema), outcome))
    }

  private def invalidResponse(response: Json): GraphQLRootFieldOutcome = {
    val worst = errorEntries(response.hcursor.downField("errors")).map(_._2).maxByOption(_.severity)
    GraphQLRootFieldOutcome(
      GraphQLOperationKind.Unknown,
      InvalidRequestField,
      worst.getOrElse(GraphQLOutcome.Rejected)
    )
  }

  private def boundedName(kind: GraphQLOperationKind, name: String, schema: Schema[?, ?]): String = {
    val known = kind match {
      case GraphQLOperationKind.Query    => schema.query.fieldsByName.keySet
      case GraphQLOperationKind.Mutation => schema.mutation.fold(Set.empty[String])(_.fieldsByName.keySet)
      case GraphQLOperationKind.Unknown  => Set.empty[String]
    }
    if (name.startsWith("__")) Introspection else if (known.contains(name)) name else UnknownField
  }

  private def errorEntries(errors: ACursor): List[(Option[String], GraphQLOutcome)] =
    errors.values.toList.flatMap(_.toList).map { error =>
      val cursor = error.hcursor
      val key = cursor.downField("path").downN(0).as[String].toOption
      val code = cursor.downField("extensions").downField("code").as[String].toOption
      key -> GraphQLFailureCatalog.outcomeOf(code)
    }

  private def selections(
      document: ast.Document,
      operationName: Option[String]
  ): Option[(GraphQLOperationKind, List[RootSelection])] =
    selectedOperation(document, operationName).map { operation =>
      val kind = operation.operationType match {
        case ast.OperationType.Query        => GraphQLOperationKind.Query
        case ast.OperationType.Mutation     => GraphQLOperationKind.Mutation
        case ast.OperationType.Subscription => GraphQLOperationKind.Unknown
      }
      val flat = flatten(operation.selections, document.fragments)
      kind -> flat.distinctBy(_.responseKey)
    }

  /** Same rule as the executor: a named operation must exist; without a name the document must hold exactly one. */
  private def selectedOperation(
      document: ast.Document,
      operationName: Option[String]
  ): Option[ast.OperationDefinition] =
    operationName match {
      case Some(_) => document.operations.get(operationName)
      case None    => Option.when(document.operations.size == 1)(document.operations.values.head)
    }

  /** Upper bound on collected root selections, so a hostile document cannot make counting expensive. */
  private val MaxRootSelections = 1000

  private def isConditional(directives: Vector[ast.Directive]): Boolean =
    directives.exists(d => ConditionalDirectives.contains(d.name))

  /** Collects root fields like the GraphQL field collection: each named fragment is expanded at most once per document
    * (one visited set threaded through the whole walk), and collection stops at [[MaxRootSelections]]. The work is
    * therefore linear in the document size however the fragments are chained.
    */
  private def flatten(
      selections: Vector[ast.Selection],
      fragments: Map[String, ast.FragmentDefinition]
  ): List[RootSelection] = {
    final case class Walk(visited: Set[String], collected: Vector[RootSelection])

    def walk(state: Walk, selections: Vector[ast.Selection], conditional: Boolean): Walk =
      selections.foldLeft(state) { (current, selection) =>
        if (current.collected.size >= MaxRootSelections) current
        else
          selection match {
            case field: ast.Field =>
              current.copy(collected =
                current.collected :+ RootSelection(
                  field.name,
                  field.outputName,
                  conditional || isConditional(field.directives)
                )
              )
            case inline: ast.InlineFragment =>
              walk(current, inline.selections, conditional || isConditional(inline.directives))
            case spread: ast.FragmentSpread if !current.visited.contains(spread.name) =>
              fragments.get(spread.name).fold(current.copy(visited = current.visited + spread.name)) { fragment =>
                walk(
                  current.copy(visited = current.visited + spread.name),
                  fragment.selections,
                  conditional || isConditional(spread.directives)
                )
              }
            case _: ast.FragmentSpread => current
          }
      }

    walk(Walk(Set.empty, Vector.empty), selections, conditional = false).collected.toList
  }
}
