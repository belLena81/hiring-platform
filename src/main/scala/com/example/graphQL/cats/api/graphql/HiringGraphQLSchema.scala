package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import io.circe.Json
import sangria.execution.Executor
import sangria.marshalling.circe.*
import sangria.renderer.SchemaRenderer
import sangria.schema.Schema

object HiringGraphQLSchema {
  lazy val schema: Schema[RequestContext, Unit] = HiringGraphQLSchemaAssembly.schema
  lazy val sdl: String = SchemaRenderer.renderSchema(schema) + "\n"

  enum Failure {
    case InvalidQuery, Internal
  }

  private[api] def executeInContext(
      request: GraphQLRequest,
      document: GraphQLDocument,
      context: RequestContext
  ): IO[Either[Failure, Json]] =
    context.effectAdapter.execute(context) { exceptionHandler =>
      IO.executionContext.flatMap { implicit executionContext =>
        context.effectAdapter.fromFuture(
          IO(
            Executor.execute(
              schema = schema,
              queryAst = document.document,
              userContext = context,
              variables = request.variables,
              operationName = request.operationName,
              queryValidator = document.queryValidator,
              exceptionHandler = exceptionHandler,
              queryReducers = HiringGraphQLSchemaAssembly.queryReducers,
              deferredResolver = HiringGraphQLSchemaAssembly.deferredResolver,
              errorsLimit = Some(20)
            )
          )
        )
      }
    }
}
