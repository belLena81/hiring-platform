package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.shared.search.{RankedCandidate, RankedJob}
import io.circe.Json
import sangria.schema.Context


private[graphql] object HiringGraphQLSearchResolvers {
  def semanticJobSearch(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, hiring, service) =>
      val filter = jobFilter(context.arg(jobFilterArgument))
      for {
        size     <- EitherT.fromEither[IO](pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF(context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure))
        results  <- liftUseCase(service.semanticJobSearch(actor, context.arg(queryArgument), filter, size, searchId))
        _        <- EitherT.liftF(saveSearchSession(
                      hiring,
                      actor.userId,
                      "semanticJobSearch",
                      searchId,
                      filterJson(filter),
                      results.headOption.map(_.meta.model),
                      results.headOption.map(_.meta.version)
                    )(results)(_.job.id.value.toString, _.score))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def recommendedJobs(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, hiring, service) =>
      for {
        size     <- EitherT.fromEither[IO](pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF(context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure))
        results  <- liftUseCase(service.recommendedJobs(actor, size, searchId))
        _        <- EitherT.liftF(saveSearchSession(
                      hiring,
                      actor.userId,
                      "recommendedJobs",
                      searchId,
                      Json.obj(),
                      results.headOption.map(_.meta.model),
                      results.headOption.map(_.meta.version)
                    )(results)(_.job.id.value.toString, _.score))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def candidateMatches(context: Context[RequestContext, Unit]): IO[RankedCandidateResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, hiring, service) =>
      val jobId = context.arg(jobIdArgument)
      for {
        size     <- EitherT.fromEither[IO](pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF(context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure))
        results  <- liftUseCase(service.candidateMatches(actor, jobId, size, searchId))
        _        <- EitherT.liftF(saveSearchSession(
                      hiring,
                      actor.userId,
                      "candidateMatches",
                      searchId,
                      Json.obj("jobId" -> Json.fromString(jobId.value.toString)),
                      results.headOption.map(_.meta.model),
                      results.headOption.map(_.meta.version)
                    )(results)(_.candidate.id.value.toString, _.score))
      } yield rankedCandidateResults(results)
    }, error => RankedCandidateResults(Nil, List(error)))

  private def rankedJobResults(values: List[RankedJob]): RankedJobResults =
    RankedJobResults(values.map(value => RankedJobPayload(
      value.job,
      value.score,
      value.mode,
      value.meta.model,
      value.meta.version,
      value.searchId.toString
    )), Nil)

  private def rankedCandidateResults(values: List[RankedCandidate]): RankedCandidateResults =
    RankedCandidateResults(values.map(value => RankedCandidatePayload(
      CandidateMatchCandidate(
        value.candidate.id.value.toString,
        value.candidate.name,
        value.candidate.candidateProfile.map(profile => CandidateMatchProfile(profile.skills, profile.experienceSummary))
      ),
      value.score,
      value.mode,
      value.meta.model,
      value.meta.version,
      value.searchId.toString
    )), Nil)
}
