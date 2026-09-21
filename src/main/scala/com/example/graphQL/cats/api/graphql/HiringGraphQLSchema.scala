package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLSchemaAssembly.QueryComplexityExceeded
import com.example.graphQL.cats.service.{ActorContext, HealthService, ProbeResult}
import com.example.graphQL.cats.service.{RepositoryError, UseCaseError}
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer
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
      service: HealthService,
      requestId: String,
      actor: Option[ActorContext],
      hiring: HiringGraphQLServices,
      ensureHiringReady: IO[ProbeResult],
      contextFactory: RequestContextFactory,
      tracer: Tracer[IO] = Tracer.noop[IO]
  ): IO[Either[Failure, Json]] =
    contextFactory.resource(service.readiness(Some(requestId)), actor, hiring, ensureHiringReady, tracer).use { context =>
      executeInContext(request, context)
    }

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
          case (_, _) => HandledException("Execution failed")
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
