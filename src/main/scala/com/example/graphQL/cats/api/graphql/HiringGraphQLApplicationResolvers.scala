package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor}
import io.circe.Json
import sangria.schema.Context
import java.time.Instant

private[graphql] object HiringGraphQLApplicationResolvers {
  def myApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      for {
        now                      <- IO.realTimeInstant
        (pageRequest, requested) <- inputResult(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), cursor => CursorCodec.decode[ApplicationCursor](cursor, now)))
        values                   <- raiseOnUseCaseError(hiring.applicationService.myApplications(actor, pageRequest))
      } yield applicationConnection(values, requested, now)
    }

  def jobApplications(context: Context[RequestContext, Unit]): IO[Connection[Application]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val jobId = context.arg(jobIdArgument)
      for {
        now                      <- IO.realTimeInstant
        (pageRequest, requested) <- inputResult(applicationPage(context.arg(firstArgument), context.arg(afterArgument), context.arg(applicationStatusArgument), cursor => CursorCodec.decode[ApplicationCursor](cursor, now)))
        values                   <- raiseOnUseCaseError(hiring.applicationService.jobApplications(actor, jobId, pageRequest))
      } yield applicationConnection(values, requested, now)
    }

  def applicationHistory(context: Context[RequestContext, Unit]): IO[Connection[ApplicationEvent]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val applicationId = context.arg(applicationIdArgument)
      for {
        now                      <- IO.realTimeInstant
        (pageRequest, requested) <- inputResult(pageEvent(context.arg(firstArgument), context.arg(afterArgument), cursor => CursorCodec.decode[ApplicationEventCursor](cursor, now)))
        _                        <- raiseOnUseCaseError(hiring.readModel.canViewApplication(actor, applicationId))
        values                   <- raiseOnUseCaseError(hiring.readModel.applicationHistory(applicationId, pageRequest))
      } yield eventConnection(values, requested, now)
    }

  def submitApplication(context: Context[RequestContext, Unit]): IO[MutationOutcome[Application]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(submitApplicationInputArgument)
      mutationResult(hiring.applicationService.submitApplication(
        idempotencyRequest(input.idempotencyKey, Json.fromString(input.toString)),
        actor,
        input.jobId
      ))
    }

  def applicationStatusAction(context: Context[RequestContext, Unit], status: ApplicationStatus): IO[MutationOutcome[Application]] = {
    val input = context.arg(applicationActionInputArgument)
    changeApplicationStatus(context, input.idempotencyKey, input.applicationId, status, None, None)
  }

  def rejectApplication(context: Context[RequestContext, Unit]): IO[MutationOutcome[Application]] = {
    val input = context.arg(rejectApplicationInputArgument)
    changeApplicationStatus(context, input.idempotencyKey, input.applicationId, ApplicationStatus.Rejected, input.feedback, None)
  }

  def declineApplication(context: Context[RequestContext, Unit]): IO[MutationOutcome[Application]] = {
    val input = context.arg(declineApplicationInputArgument)
    changeApplicationStatus(context, input.idempotencyKey, input.applicationId, ApplicationStatus.Declined, None, input.reason)
  }

  private def changeApplicationStatus(
      context: Context[RequestContext, Unit],
      idempotencyKey: java.util.UUID,
      applicationId: ApplicationId,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): IO[MutationOutcome[Application]] =
    authenticated(context) { case (actor, hiring) =>
      mutationResult(hiring.applicationService.changeStatus(
        idempotencyRequest(idempotencyKey, Json.fromString(s"$applicationId:$status:$feedback:$reason")),
        actor,
        applicationId,
        status,
        feedback,
        reason
      ))
    }

  private def applicationConnection(
      values: List[Application],
      requested: Int,
      now: Instant
  )(using CursorCodec.CursorKey
  ): Connection[Application] =
    connection(values, requested)(application => CursorCodec.encode(ApplicationCursor(application.createdAt, application.id), now))

  private def eventConnection(
      values: List[ApplicationEvent],
      requested: Int,
      now: Instant
  )(using CursorCodec.CursorKey
  ): Connection[ApplicationEvent] =
    connection(values, requested)(event => CursorCodec.encode(ApplicationEventCursor(event.occurredAt, event.id), now))

}
