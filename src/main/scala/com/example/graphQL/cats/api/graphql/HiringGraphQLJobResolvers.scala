package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.JobUseCases
import com.example.graphQL.cats.shared.search.JobSearchFilter
import com.example.graphQL.cats.shared.pagination.JobCursor
import sangria.schema.Context

import java.time.Instant

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
        (pageRequest, requested) <- inputResult(page(context.arg(firstArgument), context.arg(afterArgument), CursorCodec.decode[JobCursor]))
        searchId                 <- context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        values                   <- raiseOnUseCaseError(hiring.jobService.searchOpenJobs(actor, filter, pageRequest))
        _                        <- saveSearchSession(hiring, actor.userId, "jobs", searchId, filterJson(filter))(values)(
                                      _.id.value.toString,
                                      _ => 0d
                                    )
      } yield jobConnection(values, requested).copy(searchId = Some(searchId.toString))
    }

  def job(context: Context[RequestContext, Unit]): IO[Job] =
    authenticated(context) { case (actor, hiring) => raiseOnUseCaseError(hiring.jobService.viewJob(actor, context.arg(idArgument))) }

  def myJobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      for {
        (pageRequest, requested) <- inputResult(page(context.arg(firstArgument), context.arg(afterArgument), CursorCodec.decode[JobCursor]))
        values                   <- raiseOnUseCaseError(hiring.jobService.myJobs(actor, pageRequest.copy(status = context.arg(jobStatusArgument))))
      } yield jobConnection(values, requested)
    }

  def createJob(context: Context[RequestContext, Unit]): IO[Any] =
    authenticated(context) { case (actor, hiring) =>
      val input = jobInput(context.arg(createJobInputArgument), JobStatus.Open)
      timestamped { (now, jobId) =>
        hiring.jobService.createJob(actor, input, now, JobId(jobId))
      }.flatMap(result => mutationResult(IO.pure(result))(identity))
    }

  def updateJob(context: Context[RequestContext, Unit]): IO[Any] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      val patch = updateInput(input.patch)
      IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, input.id, patch, now))
        .flatMap(result => mutationResult(IO.pure(result))(identity))
    }

  def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases => (com.example.graphQL.cats.service.ActorContext, JobId, Instant) => IO[Either[UseCaseError, Job]]
  ): IO[Any] =
    authenticated(context) { case (actor, hiring) =>
      val jobId = context.arg(jobActionInputArgument).jobId
      IO.realTimeInstant.flatMap(now => method(hiring.jobService)(actor, jobId, now))
        .flatMap(result => mutationResult(IO.pure(result))(identity))
    }

  private def jobInput(input: JobGraphQLInput, status: JobStatus): CreateJobInput =
    CreateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(input.country, input.city.getOrElse(""), input.remote),
      status
    )

  private def updateInput(input: JobGraphQLInput): UpdateJobInput =
    UpdateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(input.country, input.city.getOrElse(""), input.remote)
    )

  private def jobConnection(values: List[Job], requested: Int)(using CursorCodec.CursorKey): Connection[Job] =
    connection(values, requested)(job => CursorCodec.encode(JobCursor(job.createdAt, job.id)))

}
