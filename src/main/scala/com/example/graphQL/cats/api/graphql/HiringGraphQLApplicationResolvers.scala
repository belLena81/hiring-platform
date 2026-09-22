package com.example.graphQL.cats.api.graphql

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
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      for {
        (pageRequest, requested) <- inputResult(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), CursorCodec.decode[ApplicationCursor]))
        values                   <- raiseOnUseCaseError(hiring.applicationService.myApplications(actor, pageRequest))
      } yield applicationConnection(values, requested)
    }

  def jobApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val jobId = context.arg(jobIdArgument)
      for {
        (pageRequest, requested) <- inputResult(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), CursorCodec.decode[ApplicationCursor]))
        values                   <- raiseOnUseCaseError(hiring.applicationService.jobApplications(actor, jobId, pageRequest))
      } yield applicationConnection(values, requested)
    }

  def applicationHistory(context: Context[RequestContext, Unit]): IO[Connection[ApplicationEvent]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val applicationId = context.arg(applicationIdArgument)
      for {
        (pageRequest, requested) <- inputResult(pageEvent(context.arg(firstArgument), context.arg(afterArgument), CursorCodec.decode[ApplicationEventCursor]))
        _                        <- raiseOnUseCaseError(hiring.readModel.canViewApplication(actor, applicationId))
        values                   <- raiseOnUseCaseError(hiring.readModel.applicationHistory(applicationId, pageRequest))
      } yield eventConnection(values, requested)
    }

  def submitApplication(context: Context[RequestContext, Unit]): IO[Any] =
    authenticated(context) { case (actor, hiring) =>
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
      }.flatMap(result => mutationResult(IO.pure(result))(identity))
    }

  def applicationStatusAction(context: Context[RequestContext, Unit], status: ApplicationStatus): IO[Any] = {
    val input = context.arg(applicationActionInputArgument)
    changeApplicationStatus(context, input.applicationId, status, None, None)
  }

  def rejectApplication(context: Context[RequestContext, Unit]): IO[Any] = {
    val input = context.arg(rejectApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Rejected, input.feedback, None)
  }

  def declineApplication(context: Context[RequestContext, Unit]): IO[Any] = {
    val input = context.arg(declineApplicationInputArgument)
    changeApplicationStatus(context, input.applicationId, ApplicationStatus.Declined, None, input.reason)
  }

  private def changeApplicationStatus(
      context: Context[RequestContext, Unit],
      applicationId: ApplicationId,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): IO[Any] =
    authenticated(context) { case (actor, hiring) =>
      timestamped { (now, eventId) =>
        hiring.applicationService.changeStatus(actor, applicationId, status, feedback, reason, ApplicationEventId(eventId), now)
      }.flatMap(result => mutationResult(IO.pure(result))(identity))
    }

  private def applicationConnection(
      values: List[Application],
      requested: Int
  )(using CursorCodec.CursorKey
  ): Connection[Application] =
    connection(values, requested)(application => CursorCodec.encode(ApplicationCursor(application.createdAt, application.id)))

  private def eventConnection(
      values: List[ApplicationEvent],
      requested: Int
  )(using CursorCodec.CursorKey
  ): Connection[ApplicationEvent] =
    connection(values, requested)(event => CursorCodec.encode(ApplicationEventCursor(event.occurredAt, event.id)))

}
