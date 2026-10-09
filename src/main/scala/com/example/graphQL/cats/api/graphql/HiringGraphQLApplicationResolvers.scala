package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.domain.pagination.{
  ApplicationCursor,
  ApplicationEventCursor,
  ApplicationEventPageRequest,
  ApplicationPageRequest
}
import sangria.schema.Context

private[graphql] object HiringGraphQLApplicationResolvers {
  def myApplications(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[Application]] =
    authenticated(context) { case (actor, hiring) =>
      applicationConnection(context, hiring)(request =>
        raiseOnUseCaseError(hiring.applicationService.myApplications(actor, request))
      )
    }

  def jobApplications(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[Application]] =
    authenticated(context) { case (actor, hiring) =>
      applicationConnection(context, hiring)(request =>
        raiseOnUseCaseError(hiring.applicationService.jobApplications(actor, context.arg(jobIdArgument), request))
      )
    }

  def applicationHistory(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[ApplicationEvent]] =
    authenticated(context) { case (actor, hiring) =>
      paged[ApplicationEventCursor, ApplicationEventPageRequest, ApplicationEvent](
        hiring,
        context.arg(firstArgument),
        context.arg(afterArgument)
      )(ApplicationEventPageRequest.apply)(request =>
        raiseOnUseCaseError(hiring.readModel.applicationHistory(actor, context.arg(applicationIdArgument), request))
      )(event => ApplicationEventCursor(event.occurredAt, event.id))
    }

  def submitApplication(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[Application]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(submitApplicationInputArgument)
      mutationResult(
        hiring.applicationService.submitApplication(
          idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
          actor,
          input.jobId
        )
      )
    }

  def applicationStatusAction(
      context: Context[RequestContext, Unit],
      status: ApplicationStatus
  ): HiringGraphQLResult[MutationOutcome[Application]] = {
    val input = context.arg(applicationActionInputArgument)
    changeApplicationStatus(context, input.idempotencyKey, input.applicationId, status, None, None)
  }

  def rejectApplication(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[Application]] = {
    val input = context.arg(rejectApplicationInputArgument)
    changeApplicationStatus(
      context,
      input.idempotencyKey,
      input.applicationId,
      ApplicationStatus.Rejected,
      input.feedback,
      None
    )
  }

  def declineApplication(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[Application]] = {
    val input = context.arg(declineApplicationInputArgument)
    changeApplicationStatus(
      context,
      input.idempotencyKey,
      input.applicationId,
      ApplicationStatus.Declined,
      None,
      input.reason
    )
  }

  private def changeApplicationStatus(
      context: Context[RequestContext, Unit],
      idempotencyKey: java.util.UUID,
      applicationId: ApplicationId,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): HiringGraphQLResult[MutationOutcome[Application]] =
    authenticated(context) { case (actor, hiring) =>
      mutationResult(
        hiring.applicationService.changeStatus(
          idempotencyRequest(
            idempotencyKey,
            applicationStatusFingerprintInput(applicationId, status, feedback, reason)
          ),
          actor,
          applicationId,
          status,
          feedback,
          reason
        )
      )
    }

  private def applicationConnection(context: Context[RequestContext, Unit], hiring: HiringGraphQLServices)(
      fetch: ApplicationPageRequest => HiringGraphQLResult[List[Application]]
  ): HiringGraphQLResult[Connection[Application]] =
    paged[ApplicationCursor, ApplicationPageRequest, Application](
      hiring,
      context.arg(firstArgument),
      context.arg(afterArgument)
    )((cursor, size) => ApplicationPageRequest(context.arg(applicationStatusArgument), cursor, size))(fetch)(
      application => ApplicationCursor(application.createdAt, application.id)
    )
}
