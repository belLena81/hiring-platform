package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.repository.protocol.MutationEntityReference
import io.circe.Json
import sangria.schema.Context

private[graphql] object HiringGraphQLInteractionResolvers {
  def recordJobView(context: Context[RequestContext, Unit]): IO[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordJobViewInputArgument)
      executeMutation(
        hiring,
        "recordJobView",
        actorScope(actor),
        input.idempotencyKey,
        Json.fromString(input.toString),
        _ => MutationEntityReference("interaction", input.eventId.toString),
        _ => IO.pure(Right(()))
      ) { context =>
        IO.realTimeInstant.flatMap(now => hiring.interactionService.recordJobView(actor, input.eventId, input.jobId, input.searchId, now, context))
      }.map(_.map(_ => InteractionSuccess(true))).flatMap(mutationResult)
    }

  def recordSearchResultClick(context: Context[RequestContext, Unit]): IO[MutationOutcome[InteractionSuccess]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(recordSearchResultClickInputArgument)
      executeMutation(
        hiring,
        "recordSearchResultClick",
        actorScope(actor),
        input.idempotencyKey,
        Json.fromString(input.toString),
        _ => MutationEntityReference("interaction", input.eventId.toString),
        _ => IO.pure(Right(()))
      ) { context =>
        IO.realTimeInstant.flatMap(now => hiring.interactionService.recordSearchResultClick(actor, input.eventId, input.searchId, input.resultId.toString, now, context))
      }.map(_.map(_ => InteractionSuccess(true))).flatMap(mutationResult)
    }
}
