package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLSchemaAssembly.QueryComplexityExceeded
import io.circe.Json
import sangria.execution.{ExceptionHandler, Executor, HandledException, QueryAnalysisError}
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
    IO.executionContext.flatMap { implicit executionContext =>
      IO.fromFuture(IO(Executor.execute(
        schema = schema,
        queryAst = document.document,
        userContext = context,
        variables = request.variables,
        operationName = request.operationName,
        queryValidator = document.queryValidator,
        exceptionHandler = ExceptionHandler {
          case (marshaller, RequestContext.ReadFailure(error)) =>
            val failure = HiringGraphQLResolverSupport.toGraphQLFailure(error)
            HandledException(failure.message, Map("code" -> marshaller.scalarNode(failure.code, "String", Set.empty)))
          case (marshaller, RequestContext.FieldFailure(code, message)) =>
            HandledException(message, Map("code" -> marshaller.scalarNode(code, "String", Set.empty)))
          case (marshaller, RequestContext.RateLimited(retryAfterSeconds)) =>
            HandledException("Too many authentication attempts", Map(
              "code" -> marshaller.scalarNode("RATE_LIMITED", "String", Set.empty),
              "retryAfter" -> marshaller.scalarNode(retryAfterSeconds, "Int", Set.empty)
            ))
          case (_, RequestContext.RequestClosed) =>
            HandledException("Execution failed")
          case (_, error) =>
            context.reportExecutionFailure(error)
            HandledException("Execution failed")
        },
        queryReducers = HiringGraphQLSchemaAssembly.queryReducers,
        deferredResolver = HiringGraphQLSchemaAssembly.deferredResolver,
        errorsLimit = Some(20)
      ))).map(Right(_)).handleError {
        case _: QueryAnalysisError => Left(Failure.InvalidQuery)
        case _: QueryComplexityExceeded => Left(Failure.InvalidQuery)
        case _ => Left(Failure.Internal)
      }
    }
}
