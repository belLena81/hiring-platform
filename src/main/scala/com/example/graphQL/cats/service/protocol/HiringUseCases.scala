package com.example.graphQL.cats.service.protocol

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job}
import com.example.graphQL.cats.domain.pagination.{
  ApplicationEventPageRequest,
  ApplicationPageRequest,
  JobPageRequest,
  PageSize
}
import com.example.graphQL.cats.service.search.{
  JobDiscoveryFacets,
  JobFacetQuery,
  JobSearchFilter,
  NearbyJob,
  NearbyJobsQuery,
  RankedCandidate,
  RankedJob
}
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import java.util.UUID

import com.example.graphQL.cats.service.read.*

trait HiringReadModel[F[_]] {
  def relatedUsers(actor: ActorContext, keys: List[UserRelationKey]): UseCaseF[F, List[RelatedUser]]
  def relatedJobs(actor: ActorContext, keys: List[JobRelationKey]): UseCaseF[F, List[RelatedJob]]
  def viewer(actor: ActorContext): UseCaseF[F, AuthenticatedActor]
  def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): UseCaseF[F, Set[UserId]]
  def application(id: ApplicationId): UseCaseF[F, Option[Application]]
  def canViewApplication(actor: ActorContext, applicationId: ApplicationId): UseCaseF[F, Unit]
  def applicationHistory(
      actor: ActorContext,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): UseCaseF[F, List[ApplicationEvent]]
}

trait UserAuthenticator[F[_]] {
  def actorFor(userId: UserId): RepositoryF[F, Option[ActorContext]]

  /** Resolves a cryptographically verified principal, including a deleted account for idempotent deletion replay.
    * Operations still resolve active ownership through their service authorization boundary.
    */
  def actorForVerifiedToken(userId: UserId): RepositoryF[F, Option[ActorContext]]
}

trait JobUseCases[F[_]] {
  def createJob(request: IdempotencyRequest, actor: ActorContext, input: CreateJobInput): UseCaseF[F, Job]
  def updateJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId, input: UpdateJobInput): UseCaseF[F, Job]
  def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseF[F, Job]
  def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseF[F, Job]
  def viewJob(actor: ActorContext, jobId: JobId): UseCaseF[F, Job]
  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): UseCaseF[F, List[Job]]
  def nearbyJobs(actor: ActorContext, query: NearbyJobsQuery, limit: Int): UseCaseF[F, List[NearbyJob]]
  def jobDiscoveryFacets(actor: ActorContext, query: JobFacetQuery): UseCaseF[F, JobDiscoveryFacets]
  def myJobs(actor: ActorContext, page: JobPageRequest): UseCaseF[F, List[Job]]
}

trait ApplicationUseCases[F[_]] {
  def submitApplication(
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  ): UseCaseF[F, Application]

  def myApplications(actor: ActorContext, page: ApplicationPageRequest): UseCaseF[F, List[Application]]
  def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest): UseCaseF[F, List[Application]]

  def changeStatus(
      request: IdempotencyRequest,
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): UseCaseF[F, Application]
}

trait SearchUseCases[F[_]] {
  def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): UseCaseF[F, List[RankedJob]]

  def recommendedJobs(actor: ActorContext, first: PageSize, searchId: UUID): UseCaseF[F, List[RankedJob]]

  def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      query: Option[String],
      filters: com.example.graphQL.cats.service.search.CandidateMatchFilters,
      first: PageSize,
      searchId: UUID
  ): UseCaseF[F, List[RankedCandidate]]
}

trait InteractionUseCases[F[_]] {
  def recordJobView(
      request: IdempotencyRequest,
      actor: ActorContext,
      eventId: UUID,
      jobId: JobId,
      searchId: Option[UUID]
  ): UseCaseF[F, Unit]
  def recordSearchResultClick(
      request: IdempotencyRequest,
      actor: ActorContext,
      eventId: UUID,
      searchId: UUID,
      resultId: String
  ): UseCaseF[F, Unit]
}
