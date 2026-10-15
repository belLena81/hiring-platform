package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import sangria.schema.Context

private[graphql] object HiringGraphQLInteractionResolvers {
  def recordJobView(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordJobViewInputArgument)
      mutationResult(
        hiring.interactionService
          .recordJobView(
            idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
            actor,
            input.eventId,
            input.jobId,
            input.searchId.map(_.value)
          )
          .map(_ => InteractionSuccess(input.eventId))
      )
    }

  def recordSearchResultClick(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordSearchResultClickInputArgument)
      mutationResult(
        hiring.interactionService
          .recordSearchResultClick(
            idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
            actor,
            input.eventId,
            input.searchId.value,
            input.resultId.toString
          )
          .map(_ => InteractionSuccess(input.eventId))
      )
    }
}
