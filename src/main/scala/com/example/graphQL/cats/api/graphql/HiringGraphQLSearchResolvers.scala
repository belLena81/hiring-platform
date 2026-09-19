package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.either.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import sangria.schema.Context

import java.util.UUID

private[graphql] object HiringGraphQLSearchResolvers {
  def semanticJobSearch(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.semanticJobSearch(actor, context.arg(queryArgument), jobFilter(context.arg(jobFilterArgument)), size, searchId))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def recommendedJobs(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.recommendedJobs(actor, size, searchId))
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def candidateMatches(context: Context[RequestContext, Unit]): IO[RankedCandidateResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, _, service) =>
      val jobId = context.arg(jobIdArgument)
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, GraphQLError, UUID](IO.randomUUID)
        results  <- liftUseCase(service.candidateMatches(actor, jobId, size, searchId))
      } yield rankedCandidateResults(results)
    }, error => RankedCandidateResults(Nil, List(error)))

  private def jobFilter(value: Option[JobFilterGraphQLInput]): JobSearchFilter =
    value match {
      case None => JobSearchFilter(None, Set.empty, None)
      case Some(filter) => JobSearchFilter(filter.city, filter.skills.fold(Set.empty[String])(_.toSet), filter.createdAfter)
    }

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
