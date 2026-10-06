package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.workflow.{InterviewWorkflow, InterviewWorkflowId}
import com.example.graphQL.cats.service.{AvailabilityError, UseCaseError}
import com.example.graphQL.cats.service.application.InterviewSchedulingService
import sangria.schema.Context

private[graphql] object HiringGraphQLInterviewResolvers {
  private def available(hiring: HiringGraphQLServices): HiringGraphQLResult[InterviewSchedulingService] =
    EitherT.fromEither[IO](
      hiring.interviewScheduling.toRight(
        HiringGraphQLFailure.UseCase(UseCaseError.Availability(AvailabilityError.ServiceNotReady))
      )
    )

  def scheduleInterview(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(scheduleInterviewInputArgument)
      available(hiring).flatMap(service =>
        mutationResult(
          service.schedule(
            actor,
            input.applicationId,
            input.startsAt,
            input.endsAt,
            input.idempotencyKey
          )
        )
      )
    }

  def interviewWorkflow(context: Context[RequestContext, Unit]): HiringGraphQLResult[InterviewWorkflow] =
    authenticated(context) { case (actor, hiring) =>
      available(hiring).flatMap(service =>
        raiseOnUseCaseError(
          service.inspect(
            actor,
            InterviewWorkflowId(context.arg(workflowIdArgument))
          )
        )
      )
    }

  def repairInterviewWorkflow(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(repairInterviewInputArgument)
      available(hiring).flatMap(service =>
        mutationResult(
          service.repair(
            actor,
            InterviewWorkflowId(input.workflowId),
            input.expectedRevision,
            input.idempotencyKey
          )
        )
      )
    }
}
