package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import io.circe.Json
import sangria.schema.Context

private[graphql] object HiringGraphQLInteractionResolvers {
  def recordJobView(context: Context[RequestContext, Unit]): IO[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordJobViewInputArgument)
      mutationResult(
        hiring.interactionService
          .recordJobView(idempotencyRequest(input.idempotencyKey, Json.fromString(input.toString)), actor, input.eventId, input.jobId, input.searchId)
          .map(_ => InteractionSuccess(true))
      )
    }

  def recordSearchResultClick(context: Context[RequestContext, Unit]): IO[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordSearchResultClickInputArgument)
      mutationResult(
        hiring.interactionService
          .recordSearchResultClick(idempotencyRequest(input.idempotencyKey, Json.fromString(input.toString)), actor, input.eventId, input.searchId, input.resultId.toString)
          .map(_ => InteractionSuccess(true))
      )
    }
}
