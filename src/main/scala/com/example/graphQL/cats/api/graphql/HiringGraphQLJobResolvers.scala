package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.{IO, Ref}

import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.domain.model.{Job, Location}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, JobUseCases, UseCaseF}
import com.example.graphQL.cats.domain.pagination.{JobCursor, JobPageRequest}
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.search.RelaxedFilter
import sangria.schema.Context

private[graphql] object HiringGraphQLJobResolvers {
  def jobs(context: Context[RequestContext, Unit]): HiringGraphQLResult[JobSearchConnection] =
    authenticated(context) { case (actor, hiring) =>
      for {
        filter <- inputResult(discoveryFilter(context.arg(jobFilterArgument)))
        searchId <- searchIdFor(hiring, context.arg(searchIdArgument).map(_.value))
        relaxed <- EitherT.liftF[IO, HiringGraphQLFailure, Ref[IO, List[RelaxedFilter]]](Ref.of(List.empty))
        connection <- paged[JobCursor, JobPageRequest, Job](
          hiring,
          context.arg(firstArgument),
          context.arg(afterArgument),
          "open-jobs"
        )((cursor, size) => JobPageRequest(None, cursor, size, HiringGraphQLJobProjection.of(context)))(request =>
          for {
            found <- raiseOnUseCaseError(
              hiring.jobService.searchOpenJobs(
                actor,
                filter,
                request,
                // Each extra search costs what this field was charged at admission, so it is priced like the base one.
                IO(
                  context.ctx
                    .chargeRelaxationCost(
                      context.ctx
                        .rootFieldCost(context.path)
                        .getOrElse(QueryAdmission.relaxedSearchCost(context.arg(firstArgument)))
                    )
                    .isRight
                )
              )
            )
            _ <- EitherT.liftF[IO, HiringGraphQLFailure, Unit](relaxed.set(found.relaxedFilters))
            // The session records what the caller was shown, under the filter as sent.
            _ <- recordSearch(context.ctx, hiring, actor.userId, SearchKind.Jobs, searchId, filterSnapshot(filter))(
              found.jobs
            )(
              _.id.value.toString,
              _ => 0d
            )
          } yield found.jobs
        )(job => JobCursor(job.createdAt, job.id))
        relaxedFilters <- EitherT.liftF[IO, HiringGraphQLFailure, List[RelaxedFilter]](relaxed.get)
      } yield JobSearchConnection(
        connection.edges,
        // A relaxed page is a recovery page: its cursor continues the relaxed query, which the client cannot send.
        if (relaxedFilters.isEmpty) connection.pageInfo else connection.pageInfo.copy(hasNextPage = false),
        SearchId(searchId),
        relaxedFilters
      )
    }

  def job(context: Context[RequestContext, Unit]): HiringGraphQLResult[Job] =
    authenticated(context) { case (actor, hiring) =>
      raiseOnUseCaseError(hiring.jobService.viewJob(actor, context.arg(idArgument)))
    }

  def myJobs(context: Context[RequestContext, Unit]): HiringGraphQLResult[Connection[Job]] =
    authenticated(context) { case (actor, hiring) =>
      paged[JobCursor, JobPageRequest, Job](
        hiring,
        context.arg(firstArgument),
        context.arg(afterArgument),
        s"my-jobs:${context.arg(jobStatusArgument).fold("any")(_.toString)}"
      )((cursor, size) =>
        JobPageRequest(context.arg(jobStatusArgument), cursor, size, HiringGraphQLJobProjection.of(context))
      )(request => raiseOnUseCaseError(hiring.jobService.myJobs(actor, request)))(job =>
        JobCursor(job.createdAt, job.id)
      )
    }

  def createJob(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val graphQLInput = context.arg(createJobInputArgument)
      mutationResult(
        hiring.jobService.createJob(
          idempotencyRequest(graphQLInput.idempotencyKey, graphQLInput.idempotencyPayload),
          actor,
          createJobInput(graphQLInput)
        ),
        InputFieldPaths.createJob
      )
    }

  def updateJob(context: Context[RequestContext, Unit]): HiringGraphQLResult[MutationOutcome[Job]] =
    authenticated(context) { case (actor, hiring) =>
      val input = context.arg(updateJobInputArgument)
      val patch = updateInput(input.job)
      mutationResult(
        hiring.jobService.updateJob(
          idempotencyRequest(input.idempotencyKey, input.idempotencyPayload),
          actor,
          input.id,
          patch
        ),
        InputFieldPaths.updateJob
      )
    }

  def changeJob(
      context: Context[RequestContext, Unit],
      method: JobUseCases[IO] => (
          IdempotencyRequest,
          com.example.graphQL.cats.service.ActorContext,
          JobId
      ) => UseCaseF[IO, Job]
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

  private def createJobInput(input: CreateJobGraphQLInput): CreateJobInput =
    CreateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(
        input.country,
        input.city,
        input.remote,
        input.coordinates.map(point => com.example.graphQL.cats.domain.model.GeoPoint(point.latitude, point.longitude))
      )
    )

  private def updateInput(input: JobGraphQLInput): UpdateJobInput =
    UpdateJobInput(
      input.title,
      input.description,
      input.requirements,
      input.skills.toSet,
      Location(
        input.country,
        input.city,
        input.remote,
        input.coordinates.map(point => com.example.graphQL.cats.domain.model.GeoPoint(point.latitude, point.longitude))
      )
    )
}
