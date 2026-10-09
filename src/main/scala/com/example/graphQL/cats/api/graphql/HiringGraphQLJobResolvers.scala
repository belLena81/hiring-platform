package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.data.EitherT
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, JobUseCases, UseCaseIO}
import com.example.graphQL.cats.service.search.JobSearchFilter
import com.example.graphQL.cats.domain.pagination.JobCursor
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import sangria.schema.Context

import java.time.Instant

private[graphql] object HiringGraphQLJobResolvers {
  def jobs(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[Job]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val filter = JobSearchFilter(
        context.arg(cityArgument),
        context.arg(skillsArgument).fold(Set.empty[String])(_.toSet),
        context.arg(createdAfterArgument)
      )
      for {
        now <- EitherT.liftF[IO, HiringGraphQLFailure, Instant](IO.realTimeInstant)
        (pageRequest, requested) <- inputResult(
          page(
            context.arg(firstArgument),
            context.arg(afterArgument),
            cursor => CursorCodec.decode[JobCursor](cursor, now)
          )
        )
        searchId <- searchIdFor(hiring, context.arg(searchIdArgument))
        values <- raiseOnUseCaseError(hiring.jobService.searchOpenJobs(actor, filter, pageRequest))
        _ <- recordSearch(hiring, actor.userId, SearchKind.Jobs, searchId, filterJson(filter))(values)(
          _.id.value.toString,
          _ => 0d
        )
      } yield jobConnection(values, requested, now).copy(searchId = Some(searchId.toString))
    }

  def job(context: Context[RequestContext, Unit]): HiringGraphQLResult[Job] =
    authenticated(context) { case (actor, hiring) =>
      raiseOnUseCaseError(hiring.jobService.viewJob(actor, context.arg(idArgument)))
    }

  def myJobs(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[Job]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      for {
        now <- EitherT.liftF[IO, HiringGraphQLFailure, Instant](IO.realTimeInstant)
        (pageRequest, requested) <- inputResult(
          page(
            context.arg(firstArgument),
            context.arg(afterArgument),
            cursor => CursorCodec.decode[JobCursor](cursor, now)
          )
        )
        values <- raiseOnUseCaseError(
          hiring.jobService.myJobs(actor, pageRequest.copy(status = context.arg(jobStatusArgument)))
        )
      } yield jobConnection(values, requested, now)
    }

  def createJob(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val graphQLInput = context.arg(createJobInputArgument)
      mutationResult(
        hiring.jobService.createJob(
          idempotencyRequest(graphQLInput.idempotencyKey, graphQLInput.idempotencyPayload),
          actor,
          createJobInput(graphQLInput, JobStatus.Open)
        )
      )
    }

  def updateJob(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      val patch = updateInput(input.patch)
      mutationResult(
        hiring.jobService.updateJob(
          idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
          actor,
          input.id,
          patch
        )
      )
    }

  def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases => (IdempotencyRequest, com.example.graphQL.cats.service.ActorContext, JobId) => UseCaseIO[
        Job
      ]
  ): HiringGraphQLResult[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(jobActionInputArgument)
      mutationResult(
        method(hiring.jobService)(
          idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
          actor,
          input.jobId
        )
      )
    }

  private def createJobInput(input: CreateJobGraphQLInput, status: JobStatus): CreateJobInput =
    CreateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(
        input.country,
        input.city.getOrElse(""),
        input.remote,
        input.coordinates.map(point => com.example.graphQL.cats.domain.model.GeoPoint(point.latitude, point.longitude))
      ),
      status
    )

  private def updateInput(input: JobGraphQLInput): UpdateJobInput =
    UpdateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(
        input.country,
        input.city.getOrElse(""),
        input.remote,
        input.coordinates.map(point => com.example.graphQL.cats.domain.model.GeoPoint(point.latitude, point.longitude))
      )
    )

  private def jobConnection(values: List[Job], requested: Int, now: Instant)(using
      CursorCodec.CursorKey
  ): Connection[Job] =
    connection(values, requested)(job => CursorCodec.encode(JobCursor(job.createdAt, job.id), now))

}
