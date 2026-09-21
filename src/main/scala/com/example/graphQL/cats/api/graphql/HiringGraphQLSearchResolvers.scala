package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.shared.search.{RankedCandidate, RankedJob}
import io.circe.Json
import sangria.schema.Context

private[graphql] object HiringGraphQLSearchResolvers {
  def semanticJobSearch(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      val filter = jobFilter(context.arg(jobFilterArgument))
      for {
        size     <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        results  <- liftUseCase(service.semanticJobSearch(actor, context.arg(queryArgument), filter, size, searchId))
        _        <- saveSearchSession(hiring, actor.userId, "semanticJobSearch", searchId, filterJson(filter),
                      results.headOption.map(_.meta.model), results.headOption.map(_.meta.version))(results)(
                      _.job.id.value.toString, _.score)
      } yield rankedJobResults(results)
    }

  def recommendedJobs(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      for {
        size     <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        results  <- liftUseCase(service.recommendedJobs(actor, size, searchId))
        _        <- saveSearchSession(hiring, actor.userId, "recommendedJobs", searchId, Json.obj(),
                      results.headOption.map(_.meta.model), results.headOption.map(_.meta.version))(results)(
                      _.job.id.value.toString, _.score)
      } yield rankedJobResults(results)
    }

  def candidateMatches(context: Context[RequestContext, Unit]): IO[RankedCandidateResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      val jobId = context.arg(jobIdArgument)
      for {
        size     <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        results  <- liftUseCase(service.candidateMatches(actor, jobId, size, searchId))
        _        <- saveSearchSession(hiring, actor.userId, "candidateMatches", searchId,
                      Json.obj("jobId" -> Json.fromString(jobId.value.toString)),
                      results.headOption.map(_.meta.model), results.headOption.map(_.meta.version))(results)(
                      _.candidate.id.value.toString, _.score)
      } yield rankedCandidateResults(results)
    }

  private def rankedJobResults(values: List[RankedJob]): RankedJobResults =
    RankedJobResults(values.map(value => RankedJobPayload(
      value.job, value.score, value.mode, value.meta.model, value.meta.version, value.searchId.toString)))

  private def rankedCandidateResults(values: List[RankedCandidate]): RankedCandidateResults =
    RankedCandidateResults(values.map(value => RankedCandidatePayload(
      CandidateMatchCandidate(
        value.candidate.id.value.toString,
        value.candidate.name,
        value.candidate.candidateProfile.map(profile => CandidateMatchProfile(profile.skills, profile.experienceSummary))
      ), value.score, value.mode, value.meta.model, value.meta.version, value.searchId.toString)))
}
