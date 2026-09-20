package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.shared.events.{OperationalEvents, SearchSession, SearchSessionResult}
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import io.circe.Json
import sangria.schema.Context

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.*
import scala.util.Try

private[graphql] object HiringGraphQLSearchResolvers {
  def semanticJobSearch(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, hiring, service) =>
      val filter = jobFilter(context.arg(jobFilterArgument))
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT(searchIdValue(context.arg(searchIdArgument)))
        results  <- liftUseCase(service.semanticJobSearch(actor, context.arg(queryArgument), filter, size, searchId))
        _        <- saveRankedJobs(hiring, actor.userId, "semanticJobSearch", searchId, Some(context.arg(queryArgument)), filterJson(filter), results)
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def recommendedJobs(context: Context[RequestContext, Unit]): IO[RankedJobResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, hiring, service) =>
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT(searchIdValue(context.arg(searchIdArgument)))
        results  <- liftUseCase(service.recommendedJobs(actor, size, searchId))
        _        <- saveRankedJobs(hiring, actor.userId, "recommendedJobs", searchId, None, Json.obj(), results)
      } yield rankedJobResults(results)
    }, error => RankedJobResults(Nil, List(error)))

  def candidateMatches(context: Context[RequestContext, Unit]): IO[RankedCandidateResults] =
    complete(EitherT(authenticatedSearch(context).map(_.leftMap(toGraphQLError))).flatMap { case (actor, hiring, service) =>
      val jobId = context.arg(jobIdArgument)
      for {
        size     <- EitherT(pageSize(context.arg(firstArgument)))
        searchId <- EitherT(searchIdValue(context.arg(searchIdArgument)))
        results  <- liftUseCase(service.candidateMatches(actor, jobId, size, searchId))
        _        <- saveRankedCandidates(hiring, actor.userId, "candidateMatches", searchId, None,
                      Json.obj("jobId" -> Json.fromString(jobId.value.toString)), results)
      } yield rankedCandidateResults(results)
    }, error => RankedCandidateResults(Nil, List(error)))

  private def searchIdValue(value: Option[String]): IO[Either[GraphQLError, UUID]] =
    value match {
      case Some(raw) => IO.pure(Try(UUID.fromString(raw)).toEither.leftMap(_ => GraphQLError("INVALID_ID", "Invalid UUID")))
      case None => IO.randomUUID.map(Right(_))
    }

  private def saveRankedJobs(
      hiring: HiringGraphQLServices,
      actorId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      kind: String,
      searchId: UUID,
      query: Option[String],
      filter: Json,
      values: List[RankedJob]
  ): GraphQLStep[Unit] =
    EitherT(IO.realTimeInstant.flatMap { now =>
      val session = SearchSession(
        searchId,
        actorId,
        kind,
        query,
        filter,
        values.headOption.map(_.meta.model),
        values.headOption.map(_.meta.version),
        values.zipWithIndex.map { case (value, index) =>
          SearchSessionResult(value.job.id.value.toString, index + 1, value.score)
        },
        now,
        now.plusSeconds(7.days.toSeconds)
      )
      hiring.searchSessions.save(session, OperationalEvents.searchPerformed(searchEventId(searchId), session))
        .map(_.leftMap(error => toGraphQLError(UseCaseError.repository(error))))
    })

  private def saveRankedCandidates(
      hiring: HiringGraphQLServices,
      actorId: com.example.graphQL.cats.domain.model.Identifiers.UserId,
      kind: String,
      searchId: UUID,
      query: Option[String],
      filter: Json,
      values: List[RankedCandidate]
  ): GraphQLStep[Unit] =
    EitherT(IO.realTimeInstant.flatMap { now =>
      val session = SearchSession(
        searchId,
        actorId,
        kind,
        query,
        filter,
        values.headOption.map(_.meta.model),
        values.headOption.map(_.meta.version),
        values.zipWithIndex.map { case (value, index) =>
          SearchSessionResult(value.candidate.id.value.toString, index + 1, value.score)
        },
        now,
        now.plusSeconds(7.days.toSeconds)
      )
      hiring.searchSessions.save(session, OperationalEvents.searchPerformed(searchEventId(searchId), session))
        .map(_.leftMap(error => toGraphQLError(UseCaseError.repository(error))))
    })

  private def searchEventId(searchId: UUID): UUID =
    UUID.nameUUIDFromBytes(s"search-performed:$searchId".getBytes(StandardCharsets.UTF_8))

  private def jobFilter(value: Option[JobFilterGraphQLInput]): JobSearchFilter =
    value match {
      case None => JobSearchFilter(None, Set.empty, None)
      case Some(filter) => JobSearchFilter(filter.city, filter.skills.fold(Set.empty[String])(_.toSet), filter.createdAfter)
    }

  private def filterJson(value: JobSearchFilter): Json =
    Json.obj(
      "city" -> value.city.fold(Json.Null)(Json.fromString),
      "skills" -> Json.arr(value.skills.toList.sorted.map(Json.fromString)*),
      "createdAfter" -> value.createdAfter.fold(Json.Null)(instant => Json.fromString(instant.toString))
    )

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
