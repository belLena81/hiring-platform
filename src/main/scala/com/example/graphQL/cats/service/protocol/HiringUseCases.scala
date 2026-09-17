package com.example.graphQL.cats.service.protocol

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, PageSize}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import java.time.Instant
import java.util.UUID

trait HiringReadModel[F[_]] {
  def user(id: com.example.graphQL.cats.domain.model.Identifiers.UserId): F[Option[User]]
  def users(ids: List[com.example.graphQL.cats.domain.model.Identifiers.UserId]): F[List[User]]
  def job(id: JobId): F[Option[Job]]
  def jobs(ids: List[JobId]): F[List[Job]]
  def application(id: ApplicationId): F[Option[Application]]
  def canViewApplication(actor: ActorContext, applicationId: ApplicationId): F[Either[UseCaseError, Unit]]
  def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest): F[List[ApplicationEvent]]
}

trait UserAuthenticator[F[_]] {
  def actorFor(userId: UserId): F[Option[ActorContext]]
}

trait JobUseCases[F[_]] {
  def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, jobId: JobId): F[Either[UseCaseError, Job]]
  def updateJob(actor: ActorContext, jobId: JobId, input: UpdateJobInput, now: Instant): F[Either[UseCaseError, Job]]
  def publishJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]]
  def closeJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]]
  def viewJob(actor: ActorContext, jobId: JobId): F[Either[UseCaseError, Job]]
  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): F[Either[UseCaseError, List[Job]]]
  def myJobs(actor: ActorContext, page: JobPageRequest): F[Either[UseCaseError, List[Job]]]
}

trait ApplicationUseCases[F[_]] {
  def submitApplication(
      actor: ActorContext,
      jobId: JobId,
      applicationId: ApplicationId,
      eventId: ApplicationEventId,
      now: Instant
  ): F[Either[UseCaseError, Application]]

  def myApplications(actor: ActorContext, page: ApplicationPageRequest): F[Either[UseCaseError, List[Application]]]
  def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest): F[Either[UseCaseError, List[Application]]]

  def changeStatus(
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String],
      eventId: ApplicationEventId,
      now: Instant
  ): F[Either[UseCaseError, Application]]
}

trait SearchUseCases[F[_]] {
  def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): F[Either[UseCaseError, List[RankedJob]]]

  def recommendedJobs(actor: ActorContext, first: PageSize, searchId: UUID): F[Either[UseCaseError, List[RankedJob]]]

  def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      first: PageSize,
      searchId: UUID
  ): F[Either[UseCaseError, List[RankedCandidate]]]
}
