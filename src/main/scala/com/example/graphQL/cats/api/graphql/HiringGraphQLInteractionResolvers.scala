package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import sangria.schema.Context

private[graphql] object HiringGraphQLInteractionResolvers {
  def recordJobView(context: Context[RequestContext, Unit]): IO[InteractionPayload] =
    authenticatedPayload(InteractionPayload(false, _))(context) { case (actor, hiring) =>
      val input = context.arg(recordJobViewInputArgument)
      hiring.interactionService.fold(IO.pure(InteractionPayload(true, Nil))) { interaction =>
        IO.realTimeInstant.flatMap(now =>
          interaction.recordJobView(actor, input.eventId, input.jobId, input.searchId, now)
            .map(_.fold(error => InteractionPayload(false, List(toGraphQLError(error))), _ => InteractionPayload(true, Nil)))
        )
      }
    }

  def recordSearchResultClick(context: Context[RequestContext, Unit]): IO[InteractionPayload] =
    authenticatedPayload(InteractionPayload(false, _))(context) { case (actor, hiring) =>
      val input = context.arg(recordSearchResultClickInputArgument)
      hiring.interactionService.fold(IO.pure(InteractionPayload(true, Nil))) { interaction =>
        IO.realTimeInstant.flatMap(now =>
          interaction.recordSearchResultClick(actor, input.eventId, input.searchId, input.resultId.toString, now)
            .map(_.fold(error => InteractionPayload(false, List(toGraphQLError(error))), _ => InteractionPayload(true, Nil)))
        )
      }
    }
}
