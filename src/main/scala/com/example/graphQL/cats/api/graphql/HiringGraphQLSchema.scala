package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import io.circe.Json
import sangria.ast
import sangria.execution.Executor
import sangria.marshalling.circe.*
import sangria.renderer.{SchemaFilter, SchemaRenderer}
import sangria.schema.Schema
import sangria.validation.QueryValidator

object HiringGraphQLSchema {
  lazy val schema: Schema[RequestContext, Unit] = HiringGraphQLSchemaAssembly.schema
  // The default filter hides every Sangria built-in scalar by name, including the declared `Long`; only the GraphQL
  // built-ins are hidden here, so the SDL declares `scalar Long`.
  // The renderer treats `@oneOf` as built in and omits its declaration, but the SDL uses it; declare it so that strict
  // SDL consumers accept the document.
  lazy val sdl: String =
    "directive @oneOf on INPUT_OBJECT\n\n" +
      SchemaRenderer.renderSchema(schema, SchemaFilter.withoutGraphQLBuiltIn) + "\n"

  enum Failure {
    case InvalidQuery, Internal
    case CostBudgetExhausted(retryAfterSeconds: Long, cost: Option[CostReceipt])
  }

  /** The refusal body for an exhausted query cost budget; the HTTP layer pairs it with status 429. */
  private[api] def costBudgetResponse(retryAfterSeconds: Long, cost: Option[CostReceipt]): Json = {
    val failure = GraphQLFailureCatalog.costBudgetExhausted
    val body = Json.obj(
      "data" -> Json.Null,
      "errors" -> Json.arr(
        Json.obj(
          "message" -> Json.fromString(failure.message),
          "extensions" -> Json.obj(
            "code" -> Json.fromString(failure.code),
            "retryAfter" -> Json.fromLong(retryAfterSeconds)
          )
        )
      )
    )
    withCost(cost)(body)
  }

  /** Sets `extensions.cost` only when the caller has a receipt (none on a shared counter), keeping every other
    * `extensions` key; an `extensions` value that is not an object is left untouched.
    */
  private[api] def withCost(cost: Option[CostReceipt])(response: Json): Json =
    cost.fold(response)(receipt =>
      response.asObject.fold(response) { fields =>
        val costJson = Json.obj(
          "requested" -> Json.fromInt(receipt.requested),
          "limit" -> Json.fromInt(receipt.limit),
          "remaining" -> Json.fromLong(receipt.remaining),
          "resetsInSeconds" -> Json.fromLong(receipt.resetsInSeconds)
        )
        fields("extensions") match {
          case None           => Json.fromJsonObject(fields.add("extensions", Json.obj("cost" -> costJson)))
          case Some(existing) =>
            existing.asObject.fold(response)(extensions =>
              Json.fromJsonObject(fields.add("extensions", Json.fromJsonObject(extensions.add("cost", costJson))))
            )
        }
      }
    )

  private def rejectedResponse(failure: HiringGraphQLModel.GraphQLFailure): Json =
    Json.obj(
      "data" -> Json.Null,
      "errors" -> Json.arr(
        Json.obj(
          "message" -> Json.fromString(failure.message),
          "extensions" -> Json.obj(
            ("code" -> Json.fromString(failure.code)) +:
              failure.reason.toList.map(reason => "reason" -> Json.fromString(reason))*
          )
        )
      )
    )

  private val IntrospectionFields = Set("__schema", "__type")

  /** True when any operation or fragment selects `__schema` or `__type`; `__typename` is not introspection. */
  private[graphql] def selectsIntrospection(document: ast.Document): Boolean = {
    def hits(selections: Vector[ast.Selection]): Boolean =
      selections.exists {
        case field: ast.Field           => IntrospectionFields.contains(field.name) || hits(field.selections)
        case inline: ast.InlineFragment => hits(inline.selections)
        case _: ast.FragmentSpread      => false
      }
    document.operations.values.exists(operation => hits(operation.selections)) ||
    document.fragments.values.exists(fragment => hits(fragment.selections))
  }

  private[api] def executeInContext(
      request: GraphQLRequest,
      document: GraphQLDocument,
      context: RequestContext
  ): IO[Either[Failure, Json]] =
    if (!context.schemaExposure && selectsIntrospection(document.document))
      // Refused before validation or execution, so a disabled schema reveals nothing about itself.
      IO.pure(Right(rejectedResponse(GraphQLFailureCatalog.schemaExposureDisabled)))
    else execute(request, document, context)

  private def execute(
      request: GraphQLRequest,
      document: GraphQLDocument,
      context: RequestContext
  ): IO[Either[Failure, Json]] =
    context.effectAdapter
      .execute(context, rejectedResponse) { exceptionHandler =>
        IO.executionContext.flatMap { implicit executionContext =>
          context.effectAdapter.fromFuture(
            IO(
              Executor.execute(
                schema = schema,
                queryAst = document.document,
                userContext = context,
                variables = request.variables,
                operationName = request.operationName,
                queryValidator = QueryValidator.default,
                exceptionHandler = exceptionHandler,
                queryReducers = HiringGraphQLSchemaAssembly.queryReducers,
                deferredResolver = HiringGraphQLSchemaAssembly.deferredResolver,
                errorsLimit = Some(20)
              )
            )
          )
        }
      }
      .map(_.map(withCost(context.costReceipt)))
}
