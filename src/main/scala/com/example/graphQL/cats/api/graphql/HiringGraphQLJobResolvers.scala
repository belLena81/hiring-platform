package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.JobUseCases
import com.example.graphQL.cats.repository.protocol.{MutationEntityReference, RepositoryError}
import com.example.graphQL.cats.repository.protocol.MutationWriteContext
import io.circe.Json
import com.example.graphQL.cats.shared.search.JobSearchFilter
import com.example.graphQL.cats.shared.pagination.JobCursor
import sangria.schema.Context

import java.time.Instant
import java.util.UUID

private[graphql] object HiringGraphQLJobResolvers {
  def jobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val filter = JobSearchFilter(
        context.arg(cityArgument),
        context.arg(skillsArgument).fold(Set.empty[String])(_.toSet),
        context.arg(createdAfterArgument)
      )
      for {
        now                      <- IO.realTimeInstant
        (pageRequest, requested) <- inputResult(page(context.arg(firstArgument), context.arg(afterArgument), cursor => CursorCodec.decode[JobCursor](cursor, now)))
        searchId                 <- context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        values                   <- raiseOnUseCaseError(hiring.jobService.searchOpenJobs(actor, filter, pageRequest))
        _                        <- saveSearchSession(hiring, actor.userId, "jobs", searchId, filterJson(filter))(values)(
                                      _.id.value.toString,
                                      _ => 0d
                                    )
      } yield jobConnection(values, requested, now).copy(searchId = Some(searchId.toString))
    }

  def job(context: Context[RequestContext, Unit]): IO[Job] =
    authenticated(context) { case (actor, hiring) => raiseOnUseCaseError(hiring.jobService.viewJob(actor, context.arg(idArgument))) }

  def myJobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      for {
        now                      <- IO.realTimeInstant
        (pageRequest, requested) <- inputResult(page(context.arg(firstArgument), context.arg(afterArgument), cursor => CursorCodec.decode[JobCursor](cursor, now)))
        values                   <- raiseOnUseCaseError(hiring.jobService.myJobs(actor, pageRequest.copy(status = context.arg(jobStatusArgument))))
      } yield jobConnection(values, requested, now)
    }

  def createJob(context: Context[RequestContext, Unit]): IO[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val input = createJobInput(context.arg(createJobInputArgument), JobStatus.Open)
      executeMutation[Job](
        hiring,
        "createJob",
        actorScope(actor),
        context.arg(createJobInputArgument).idempotencyKey,
        Json.fromString(context.arg(createJobInputArgument).toString),
        job => MutationEntityReference("job", job.id.value.toString),
        reference => replayJob(hiring, actor, reference)
      ) { context =>
        timestamped { (now, jobId) =>
          hiring.jobService.createJob(actor, input, now, JobId(jobId), context)
        }
      }.flatMap(mutationResult)
    }

  def updateJob(context: Context[RequestContext, Unit]): IO[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      val patch = updateInput(input.patch)
      executeMutation[Job](
        hiring,
        "updateJob",
        actorScope(actor),
        input.idempotencyKey,
        Json.fromString(input.toString),
        job => MutationEntityReference("job", job.id.value.toString),
        reference => replayJob(hiring, actor, reference)
      ) { context =>
        IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, input.id, patch, now, context))
      }.flatMap(mutationResult)
    }

  def changeJob(
      context: Context[RequestContext, Unit],
      operation: String,
      method: JobUseCases => (ActorContext, JobId, Instant, MutationWriteContext) => IO[Either[UseCaseError, Job]]
  ): IO[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val jobId = context.arg(jobActionInputArgument).jobId
      executeMutation[Job](
        hiring,
        operation,
        actorScope(actor),
        context.arg(jobActionInputArgument).idempotencyKey,
        Json.fromString(context.arg(jobActionInputArgument).toString),
        job => MutationEntityReference("job", job.id.value.toString),
        reference => replayJob(hiring, actor, reference)
      ) { writeContext =>
        IO.realTimeInstant.flatMap(now => method(hiring.jobService)(actor, jobId, now, writeContext))
      }.flatMap(mutationResult)
    }

  private def createJobInput(input: CreateJobGraphQLInput, status: JobStatus): CreateJobInput =
    CreateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(input.country, input.city.getOrElse(""), input.remote),
      status
    )

  private def replayJob(
      hiring: HiringGraphQLServices,
      actor: ActorContext,
      reference: MutationEntityReference
  ): IO[Either[UseCaseError, Job]] =
    scala.util.Try(JobId(UUID.fromString(reference.entityId))).toEither.fold(
      _ => IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable))),
      id => hiring.jobService.viewJob(actor, id)
    )

  private def updateInput(input: JobGraphQLInput): UpdateJobInput =
    UpdateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(input.country, input.city.getOrElse(""), input.remote)
    )

  private def jobConnection(values: List[Job], requested: Int, now: Instant)(using CursorCodec.CursorKey): Connection[Job] =
    connection(values, requested)(job => CursorCodec.encode(JobCursor(job.createdAt, job.id), now))

}
