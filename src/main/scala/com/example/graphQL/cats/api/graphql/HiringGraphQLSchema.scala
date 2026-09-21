package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.api.graphql.HiringGraphQLSchemaAssembly.QueryComplexityExceeded
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.UseCaseError
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

  def execute(
      request: GraphQLRequest,
      context: Resource[IO, RequestContext]
  ): IO[Either[Failure, Json]] =
    context.use(executeInContext(request, _))

  private[api] def executeInContext(request: GraphQLRequest, context: RequestContext): IO[Either[Failure, Json]] =
    IO.executionContext.flatMap { implicit executionContext =>
      IO.fromFuture(IO(Executor.execute(
        schema = schema,
        queryAst = request.document,
        userContext = context,
        variables = request.variables,
        operationName = request.operationName,
        exceptionHandler = ExceptionHandler {
          case (_, error: QueryAnalysisError) => throw error
          case (_, error: QueryComplexityExceeded) => throw error
          case (_, RequestContext.ReadFailure(UseCaseError.Repository(RepositoryError.Unavailable))) =>
            HandledException("Repository unavailable")
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
