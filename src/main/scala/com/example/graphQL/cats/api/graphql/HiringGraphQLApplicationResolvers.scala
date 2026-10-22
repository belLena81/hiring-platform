package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
import com.example.graphQL.cats.domain.workflow.InterviewWorkflow
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.domain.pagination.{
  ApplicationCursor,
  ApplicationEventCursor,
  ApplicationEventPageRequest,
  ApplicationPageRequest
}
import sangria.schema.Context

private[graphql] object HiringGraphQLApplicationResolvers {
  def myApplications(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[Application]] =
    ownApplications(context)

  def jobApplications(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[Application]] =
    applicationsToJob(context, context.arg(jobIdArgument))

  def applicationHistory(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[ApplicationEvent]] =
    historyOf(context, context.arg(applicationIdArgument))

  /** `Job.applications`: the same use case and authorization as `jobApplications`, for the job being resolved. */
  def jobApplicationsOf(context: Context[RequestContext, Job]): HiringGraphQLResult[Connection[Application]] =
    applicationsToJob(context, context.value.id)

  /** `Application.events`: the same use case and authorization as `applicationHistory`. */
  def applicationEventsOf(
      context: Context[RequestContext, Application]
  ): HiringGraphQLResult[Connection[ApplicationEvent]] =
    historyOf(context, context.value.id)

  /** `User.applications`: only the viewer's own account has applications to list, through `myApplications`. Any other
    * user resolves to null without a read, so a candidate or recruiter reached through a relation exposes nothing.
    */
  def userApplications(context: Context[RequestContext, User]): HiringGraphQLResult[Option[Connection[Application]]] =
    authenticated(context) { case (actor, _) =>
      if (actor.userId == context.value.id) ownApplications(context).map(Some(_))
      else EitherT.pure[IO, HiringGraphQLFailure](None)
    }

  /** `InterviewWorkflow.application`: visible exactly when the caller may view the application, otherwise null (a
    * workflow the caller can see never reveals an application the caller cannot).
    */
  def workflowApplication(
      context: Context[RequestContext, InterviewWorkflow]
  ): HiringGraphQLResult[Option[Application]] =
    authenticated(context) { case (actor, hiring) =>
      val applicationId = context.value.applicationId
      raiseOnUseCaseError(hiring.readModel.canViewApplication(actor, applicationId))
        .flatMap(_ => raiseOnUseCaseError(hiring.readModel.application(applicationId)))
        .recover {
          case HiringGraphQLFailure.UseCase(UseCaseError.Domain(DomainError.Forbidden | DomainError.NotFound(_))) =>
            None
        }
    }

  private def ownApplications(context: Context[RequestContext, ?]): HiringGraphQLResult[Connection[Application]] =
    authenticated(context) { case (actor, hiring) =>
      applicationConnection(hiring, context, "my-applications")(request =>
        raiseOnUseCaseError(hiring.applicationService.myApplications(actor, request))
      )
    }

  private def applicationsToJob(
      context: Context[RequestContext, ?],
      jobId: JobId
  ): HiringGraphQLResult[Connection[Application]] =
    authenticated(context) { case (actor, hiring) =>
      applicationConnection(hiring, context, s"job-applications:${jobId.value}")(request =>
        raiseOnUseCaseError(hiring.applicationService.jobApplications(actor, jobId, request))
      )
    }

  private def historyOf(
      context: Context[RequestContext, ?],
      applicationId: ApplicationId
  ): HiringGraphQLResult[Connection[ApplicationEvent]] =
    authenticated(context) { case (actor, hiring) =>
      paged[ApplicationEventCursor, ApplicationEventPageRequest, ApplicationEvent](
        hiring,
        context.arg(firstArgument),
        context.arg(afterArgument),
        s"application-events:${applicationId.value}"
      )(ApplicationEventPageRequest.apply)(request =>
        raiseOnUseCaseError(hiring.readModel.applicationHistory(actor, applicationId, request))
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
        ),
        status match {
          case ApplicationStatus.Rejected => InputFieldPaths.rejectApplication
          case ApplicationStatus.Declined => InputFieldPaths.declineApplication
          case _                          => InputFieldPaths.none
        }
      )
    }

  private def applicationConnection(hiring: HiringGraphQLServices, context: Context[RequestContext, ?], list: String)(
      fetch: ApplicationPageRequest => HiringGraphQLResult[List[Application]]
  ): HiringGraphQLResult[Connection[Application]] =
    paged[ApplicationCursor, ApplicationPageRequest, Application](
      hiring,
      context.arg(firstArgument),
      context.arg(afterArgument),
      s"$list:${context.arg(applicationStatusArgument).fold("any")(_.toString)}"
    )((cursor, size) => ApplicationPageRequest(context.arg(applicationStatusArgument), cursor, size))(fetch)(
      application => ApplicationCursor(application.createdAt, application.id)
    )
}
