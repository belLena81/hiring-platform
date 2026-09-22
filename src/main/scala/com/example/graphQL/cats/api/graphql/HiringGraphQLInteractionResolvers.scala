package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import sangria.schema.Context

private[graphql] object HiringGraphQLInteractionResolvers {
  def recordJobView(context: Context[RequestContext, Unit]): IO[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordJobViewInputArgument)
      IO.realTimeInstant.flatMap(now =>
        hiring.interactionService.recordJobView(actor, input.eventId, input.jobId, input.searchId, now)
          .map(_.map(_ => InteractionSuccess(true))).flatMap(mutationResult)
      )
    }

  def recordSearchResultClick(context: Context[RequestContext, Unit]): IO[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordSearchResultClickInputArgument)
      IO.realTimeInstant.flatMap(now =>
        hiring.interactionService.recordSearchResultClick(actor, input.eventId, input.searchId, input.resultId.toString, now)
          .map(_.map(_ => InteractionSuccess(true))).flatMap(mutationResult)
      )
    }
}
