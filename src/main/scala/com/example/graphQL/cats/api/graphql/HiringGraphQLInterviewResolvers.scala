package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.workflow.{InterviewWorkflow, InterviewWorkflowId}
import com.example.graphQL.cats.service.{ActorContext, AvailabilityError, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.application.{InterviewActionError, InterviewActionF, InterviewSchedulingService}
import sangria.schema.Context

private[graphql] object HiringGraphQLInterviewResolvers {
  private def available(hiring: HiringGraphQLServices): HiringGraphQLResult[InterviewSchedulingService[IO]] =
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

  /** A workflow that does not exist and one the actor may not see are both `None`, so existence is not revealed. */
  def interviewWorkflow(context: Context[RequestContext, Unit]): HiringGraphQLResult[Option[InterviewWorkflow]] =
    authenticated(context) { case (actor, hiring) =>
      available(hiring).flatMap(service =>
        EitherT(service.inspect(actor, context.arg(workflowIdArgument)).value.map {
          case Right(workflow) => Right(Some(workflow))
          case Left(UseCaseError.Domain(com.example.graphQL.cats.domain.error.DomainError.NotFound(_))) => Right(None)
          case Left(error) => Left(HiringGraphQLFailure.UseCase(error))
        })
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
            input.workflowId,
            input.expectedRevision,
            input.idempotencyKey
          )
        )
      )
    }

  // --- Cancel and reschedule actions ------------------------------------------------------------------------------

  def cancelInterview(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    interviewAction(context)((service, actor, input) =>
      service.cancel(actor, workflowOf(input), input.expectedRevision, input.idempotencyKey)
    )

  def requestInterviewReschedule(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    interviewAction(context)((service, actor, input) =>
      service.requestReschedule(actor, workflowOf(input), input.expectedRevision, input.idempotencyKey)
    )

  def dismissInterviewRescheduleRequest(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    interviewAction(context)((service, actor, input) =>
      service.dismissRescheduleRequest(actor, workflowOf(input), input.expectedRevision, input.idempotencyKey)
    )

  def withdrawInterviewReschedule(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    interviewAction(context)((service, actor, input) =>
      service.withdrawReschedule(actor, workflowOf(input), input.expectedRevision, input.idempotencyKey)
    )

  def acceptInterviewReschedule(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    interviewAction(context)((service, actor, input) =>
      service.acceptReschedule(actor, workflowOf(input), input.expectedRevision, input.idempotencyKey)
    )

  def declineInterviewReschedule(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    interviewAction(context)((service, actor, input) =>
      service.declineReschedule(actor, workflowOf(input), input.expectedRevision, input.idempotencyKey)
    )

  def proposeInterviewReschedule(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] = {
    val input = context.arg(proposeInterviewRescheduleInputArgument)
    interviewActionOutcome(context)((service, actor) =>
      service.proposeReschedule(
        actor,
        input.workflowId,
        input.expectedRevision,
        input.startsAt,
        input.endsAt,
        input.idempotencyKey
      )
    )
  }

  private def workflowOf(input: InterviewActionGraphQLInput): InterviewWorkflowId =
    input.workflowId

  private def interviewAction(context: Context[RequestContext, Unit])(
      run: (InterviewSchedulingService[IO], ActorContext, InterviewActionGraphQLInput) => InterviewActionF[
        IO,
        InterviewWorkflow
      ]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] = {
    val input = context.arg(interviewActionInputArgument)
    interviewActionOutcome(context)((service, actor) => run(service, actor, input))
  }

  /** The one path of every interview action. The per-actor allowance is taken first, from the verified token claims, so
    * a refused call does no storage work at all (not even the viewer read); each action of a batched request takes its
    * own unit. The actor handed to the service is the authenticated viewer, never anything from the input.
    */
  private def interviewActionOutcome(context: Context[RequestContext, Unit])(
      run: (InterviewSchedulingService[IO], ActorContext) => InterviewActionF[IO, InterviewWorkflow]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    context.ctx.takeInterviewActionAllowance.flatMap {
      case Left(limited) =>
        val failure = GraphQLFailureCatalog.interviewActionRateLimited
        EitherT.rightT[IO, HiringGraphQLFailure](
          rateLimitedRefusal(failure, limited.retryAfterSeconds): MutationOutcome[InterviewWorkflow]
        )
      case Right(_) =>
        authenticated(context) { case (actor, hiring) =>
          available(hiring).flatMap(service => outcome(run(service, actor)))
        }
    }

  private def outcome(
      value: InterviewActionF[IO, InterviewWorkflow]
  ): HiringGraphQLResult[MutationOutcome[InterviewWorkflow]] =
    EitherT(value.value.flatMap {
      case Right(workflow) => IO.pure(Right(workflow))
      case Left(InterviewActionError.UseCase(error)) if GraphQLFailureCatalog.isAuthorizationRefusal(error) =>
        val failure = GraphQLFailureCatalog.classify(error)
        IO.pure(Right(DomainError(failure.code, failure.message)))
      case Left(InterviewActionError.UseCase(error))  => mutationResult(EitherT.leftT(error)).value
      case Left(InterviewActionError.Workflow(error)) =>
        val failure = GraphQLFailureCatalog.classifyInterviewRule(error)
        IO.pure(
          if (failure.exceptional)
            Left(HiringGraphQLFailure.UseCase(UseCaseError.Repository(RepositoryError.Unavailable)))
          else Right(DomainError(failure.code, failure.message))
        )
    })
}
