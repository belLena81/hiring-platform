package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import sangria.schema.Context
import java.util.UUID
import scala.util.Try

private[graphql] object HiringGraphQLInteractionResolvers {
  def recordJobView(context: Context[RequestContext, Unit]): IO[InteractionPayload] =
    authenticatedPayload(InteractionPayload(false, _))(context) { case (actor, hiring) =>
      val input = context.arg(recordJobViewInputArgument)
      parseUuid(input.eventId).flatMap(eventId => input.searchId.traverse(parseUuid).map(eventId -> _)) match {
        case Left(error) => IO.pure(InteractionPayload(false, List(error)))
        case Right((eventId, searchId)) =>
          IO.realTimeInstant.flatMap(now =>
            hiring.interactionService.recordJobView(actor, eventId, input.jobId, searchId, now)
              .map(_.fold(error => InteractionPayload(false, List(toGraphQLError(error))), _ => InteractionPayload(true, Nil)))
          )
      }
    }

  def recordSearchResultClick(context: Context[RequestContext, Unit]): IO[InteractionPayload] =
    authenticatedPayload(InteractionPayload(false, _))(context) { case (actor, hiring) =>
      val input = context.arg(recordSearchResultClickInputArgument)
      (parseUuid(input.eventId), parseUuid(input.searchId)).mapN(_ -> _) match {
        case Left(error) => IO.pure(InteractionPayload(false, List(error)))
        case Right((eventId, searchId)) =>
          IO.realTimeInstant.flatMap(now =>
            hiring.interactionService.recordSearchResultClick(actor, eventId, searchId, input.resultId, now)
              .map(_.fold(error => InteractionPayload(false, List(toGraphQLError(error))), _ => InteractionPayload(true, Nil)))
          )
      }
    }

  private def parseUuid(value: String): Either[GraphQLError, UUID] =
    Try(UUID.fromString(value)).toEither.left.map(_ => GraphQLError("INVALID_ID", "Invalid UUID"))
}
