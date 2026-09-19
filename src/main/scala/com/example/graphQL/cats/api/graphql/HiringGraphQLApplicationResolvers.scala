package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId}
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor}
import sangria.schema.Context

private[graphql] object HiringGraphQLApplicationResolvers {
  def myApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.applicationCursorCodec
      for {
        (pageRequest, requested) <- EitherT(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), cursorCodec))
        values                   <- liftUseCase(hiring.applicationService.myApplications(actor, pageRequest))
      } yield applicationConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Application])

  def jobApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.applicationCursorCodec
      val jobId = context.arg(jobIdArgument)
      for {
        (pageRequest, requested) <- EitherT(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), cursorCodec))
        values                   <- liftUseCase(hiring.applicationService.jobApplications(actor, jobId, pageRequest))
      } yield applicationConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Application])

  def applicationHistory(context: Context[RequestContext, Unit]): IO[Connection[ApplicationEvent]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.eventCursorCodec
      val applicationId = context.arg(applicationIdArgument)
      for {
        (pageRequest, requested) <- EitherT(pageEvent(context.arg(firstArgument), context.arg(afterArgument), cursorCodec))
        _                        <- liftUseCase(hiring.readModel.canViewApplication(actor, applicationId))
        values                   <- liftUseCase(hiring.readModel.applicationHistory(applicationId, pageRequest))
      } yield eventConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[ApplicationEvent])

  def submitApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      val jobId = context.arg(submitApplicationInputArgument).jobId
      timestamped { (now, applicationId) =>
        IO.randomUUID.flatMap { eventId =>
          hiring.applicationService.submitApplication(
            actor,
            jobId,
            ApplicationId(applicationId),
            ApplicationEventId(eventId),
            now
          )
        }
      }.map(applicationPayload)
    }

  def applicationStatusAction(context: Context[RequestContext, Unit], status: ApplicationStatus): IO[ApplicationPayload] = {
    val input = context.arg(applicationActionInputArgument)
    changeApplicationStatus(context, input.applicationId, status, None, None)
  }

  def rejectApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] = {
    val input = context.arg(rejectApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Rejected, input.feedback, None)
  }

  def declineApplication(context: Context[RequestContext, Unit]): IO[ApplicationPayload] = {
    val input = context.arg(declineApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Declined, None, input.reason)
  }

  private def changeApplicationStatus(
      context: Context[RequestContext, Unit],
      applicationId: ApplicationId,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): IO[ApplicationPayload] =
    authenticatedPayload(ApplicationPayload(None, _))(context) { case (actor, hiring) =>
      timestamped { (now, eventId) =>
        hiring.applicationService.changeStatus(actor, applicationId, status, feedback, reason, ApplicationEventId(eventId), now)
      }.map(applicationPayload)
    }

  private def applicationConnection(
      values: List[Application],
      requested: Int,
      cursorCodec: CursorCodec[ApplicationCursor]
  ): Connection[Application] =
    connection(values, requested)(application => cursorCodec.encode(ApplicationCursor(application.createdAt, application.id)))

  private def eventConnection(
      values: List[ApplicationEvent],
      requested: Int,
      cursorCodec: CursorCodec[ApplicationEventCursor]
  ): Connection[ApplicationEvent] =
    connection(values, requested)(event => cursorCodec.encode(ApplicationEventCursor(event.occurredAt, event.id)))

  private def applicationPayload(result: Either[com.example.graphQL.cats.service.UseCaseError, Application]): ApplicationPayload =
    result.fold(applicationErrorPayload, application => ApplicationPayload(Some(application), Nil))

  private def applicationErrorPayload(error: com.example.graphQL.cats.service.UseCaseError): ApplicationPayload =
    ApplicationPayload(None, List(toGraphQLError(error)))
}
