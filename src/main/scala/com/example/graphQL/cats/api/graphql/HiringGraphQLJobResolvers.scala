package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.JobUseCases
import com.example.graphQL.cats.shared.events.{OperationalEvents, SearchSession, SearchSessionResult}
import com.example.graphQL.cats.shared.search.JobSearchFilter
import com.example.graphQL.cats.shared.pagination.JobCursor
import io.circe.Json
import sangria.schema.Context

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.util.Try

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
        (pageRequest, requested) <- EitherT(page(context.arg(firstArgument), context.arg(afterArgument), cursorCodec.decode))
        searchId                 <- EitherT(searchIdValue(context.arg(searchIdArgument)))
        values                   <- liftUseCase(hiring.jobService.searchOpenJobs(actor, filter, pageRequest))
        _                        <- saveJobSearch(hiring, actor.userId, searchId, filter, values)
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
        (pageRequest, requested) <- EitherT(page(context.arg(firstArgument), context.arg(afterArgument), cursorCodec.decode))
        values                   <- liftUseCase(hiring.jobService.myJobs(actor, pageRequest.copy(status = context.arg(jobStatusArgument))))
      } yield jobConnection(values, requested, cursorCodec)
    }, graphQLErrorConnection[Job])

  def createJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      jobInput(context.arg(createJobInputArgument), JobStatus.Open).fold(error => IO.pure(jobErrorPayload(error)), input =>
        timestamped { (now, jobId) =>
          hiring.jobService.createJob(actor, input, now, JobId(jobId))
        }.map(jobPayload)
      )
    }

  def updateJob(context: Context[RequestContext, Unit]): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      updateInput(input.patch).fold(
        error => IO.pure(jobErrorPayload(error)),
        patch => IO.realTimeInstant.flatMap(now => hiring.jobService.updateJob(actor, input.id, patch, now).map(jobPayload))
      )
    }

  def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases[IO] => (com.example.graphQL.cats.service.ActorContext, JobId, Instant) => IO[Either[UseCaseError, Job]]
  ): IO[JobPayload] =
    authenticatedPayload(JobPayload(None, _))(context) { case (actor, hiring) =>
      val jobId = context.arg(jobActionInputArgument).jobId
      IO.realTimeInstant.flatMap(now => method(hiring.jobService)(actor, jobId, now).map(jobPayload))
    }

  private def jobInput(input: JobGraphQLInput, status: JobStatus): Either[UseCaseError, CreateJobInput] =
    Right(CreateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(input.country, input.city.getOrElse(""), input.remote),
      status
    ))

  private def updateInput(input: JobGraphQLInput): Either[UseCaseError, UpdateJobInput] =
    Right(UpdateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(input.country, input.city.getOrElse(""), input.remote)
    ))

  private def jobConnection(values: List[Job], requested: Int, cursorCodec: CursorCodec[JobCursor]): Connection[Job] =
    connection(values, requested)(job => cursorCodec.encode(JobCursor(job.createdAt, job.id)))

  private def searchIdValue(value: Option[String]): IO[Either[GraphQLError, UUID]] =
    value match {
      case Some(raw) => IO.pure(Try(UUID.fromString(raw)).toEither.leftMap(_ => GraphQLError("INVALID_ID", "Invalid UUID")))
      case None => IO.randomUUID.map(Right(_))
    }

  private def saveJobSearch(
      hiring: HiringGraphQLServices,
      actorId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      searchId: UUID,
      filter: JobSearchFilter,
      values: List[Job]
  ): GraphQLStep[Unit] =
    EitherT(IO.realTimeInstant.flatMap { now =>
      val session = SearchSession(
        searchId,
        actorId,
        "jobs",
        None,
        filterJson(filter),
        None,
        None,
        values.zipWithIndex.map { case (value, index) =>
          SearchSessionResult(value.id.value.toString, index + 1, 0d)
        },
        now,
        now.plusSeconds(7.days.toSeconds)
      )
      hiring.searchSessions.save(session, OperationalEvents.searchPerformed(searchEventId(searchId), session))
        .map(_.leftMap(error => toGraphQLError(UseCaseError.repository(error))))
    })

  private def searchEventId(searchId: UUID): UUID =
    UUID.nameUUIDFromBytes(s"search-performed:$searchId".getBytes(StandardCharsets.UTF_8))

  private def filterJson(value: JobSearchFilter): Json =
    Json.obj(
      "city" -> value.city.fold(Json.Null)(Json.fromString),
      "skills" -> Json.arr(value.skills.toList.sorted.map(Json.fromString)*),
      "createdAfter" -> value.createdAfter.fold(Json.Null)(instant => Json.fromString(instant.toString))
    )

  private def jobPayload(result: Either[UseCaseError, Job]): JobPayload =
    result.fold(jobErrorPayload, job => JobPayload(Some(job), Nil))

  private def jobErrorPayload(error: UseCaseError): JobPayload =
    JobPayload(None, List(toGraphQLError(error)))
}
