package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.application.{HealthService, ProbeResult}
import io.circe.Json
import sangria.execution.{ExceptionHandler, Executor, HandledException, QueryAnalysisError}
import sangria.marshalling.circe.*
import sangria.renderer.SchemaRenderer
import sangria.schema.*

object FoundationSchema {
  private val healthStatus = EnumType("HealthStatus", values = List(EnumValue("UP", value = "UP")))
  private val readinessStatus = EnumType("ReadinessStatus", values = List(
    EnumValue("READY", value = "READY"), EnumValue("NOT_READY", value = "NOT_READY")))
  private val healthType = ObjectType("Health", fields[RequestContext, Unit](
    Field("status", healthStatus, resolve = _ => "UP")))
  private val readinessType = ObjectType("Readiness", fields[RequestContext, ProbeResult](
    Field("status", readinessStatus, resolve = context =>
      if (context.value == ProbeResult.Ready) "READY" else "NOT_READY")))

  val schema: Schema[RequestContext, Unit] = Schema(ObjectType("Query", fields[RequestContext, Unit](
    Field("health", healthType, resolve = _ => ()),
    Field("readiness", readinessType, resolve = context => context.ctx.readiness))))

  val sdl: String = SchemaRenderer.renderSchema(schema) + "\n"

  enum Failure {
    case InvalidQuery, Internal
  }

  def execute(request: GraphQLRequest, service: HealthService, requestId: String): IO[Either[Failure, Json]] =
    RequestContext.resource(service.readiness(Some(requestId))).use { context =>
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
        exceptionHandler = ExceptionHandler { case (_, _) => HandledException("Execution failed") },
        errorsLimit = Some(1)
      ))).map { result =>
        Right(if (result.hcursor.downField("errors").succeeded)
          result.mapObject(_.add("errors", Json.arr(Json.obj("message" -> Json.fromString("Execution failed")))))
        else result)
      }.handleError {
        case _: QueryAnalysisError => Left(Failure.InvalidQuery)
        case _                     => Left(Failure.Internal)
      }
    }
}
