package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
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
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.jobCursorCodec
      val filter = JobSearchFilter(
        context.arg(cityArgument),
        context.arg(skillsArgument).fold(Set.empty[String])(_.toSet),
        context.arg(createdAfterArgument)
      )
      for {
        (pageRequest, requested) <- EitherT.fromEither[IO](page(context.arg(firstArgument), context.arg(afterArgument), cursorCodec.decode))
        searchId                 <- EitherT.liftF(context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure))
        values                   <- liftUseCase(hiring.jobService.searchOpenJobs(actor, filter, pageRequest))
        _                        <- EitherT.liftF(saveSearchSession(hiring, actor.userId, "jobs", searchId, filterJson(filter))(values)(
                                      _.id.value.toString,
                                      _ => 0d
                                    ))
      } yield jobConnection(values, requested, cursorCodec).copy(searchId = Some(searchId.toString))
    }, graphQLErrorConnection[Job])

  def job(context: Context[RequestContext, Unit]): IO[JobPayload] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      liftUseCase(hiring.jobService.viewJob(actor, context.arg(idArgument))).map(value => JobPayload(Some(value), Nil))
    }, error => JobPayload(None, List(error)))

  def myJobs(context: Context[RequestContext, Unit]): IO[Connection[Job]] =
    complete(authenticatedStep(context) { case (actor, hiring) =>
      val cursorCodec = context.ctx.hiring.cursorCodec.jobCursorCodec
      for {
        (pageRequest, requested) <- EitherT.fromEither[IO](page(context.arg(firstArgument), context.arg(afterArgument), cursorCodec.decode))
        values                   <- liftUseCase(hiring.jobService.myJobs(actor, pageRequest.copy(status = context.arg(jobStatusArgument))))
      } yield jobConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Job])

  def createJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val input = jobInput(context.arg(createJobInputArgument), JobStatus.Open)
      timestamped { (now, jobId) =>
        hiring.jobService.createJob(actor, input, now, JobId(jobId))
      }.map(jobPayload)
    }

  def updateJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      val patch = updateInput(input.patch)
      IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, input.id, patch, now).map(jobPayload))
    }

  def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases => (com.example.graphQL.cats.service.ActorContext, JobId, Instant) => IO[Either[UseCaseError, Job]]
  ): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val jobId = context.arg(jobActionInputArgument).jobId
      IO.realTimeInstant.flatMap(now => method(hiring.jobService)(actor, jobId, now).map(jobPayload))
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

  private def jobConnection(values: List[Job], requested: Int, cursorCodec: CursorCodec[JobCursor]): Connection[Job] =
    connection(values, requested)(job => cursorCodec.encode(JobCursor(job.createdAt, job.id)))

  private def jobPayload(result: Either[UseCaseError, Job]): JobPayload =
    result.fold(jobErrorPayload, job => JobPayload(Some(job), Nil))

  private def jobErrorPayload(error: UseCaseError): JobPayload =
    JobPayload(None, List(toGraphQLError(error)))
}
