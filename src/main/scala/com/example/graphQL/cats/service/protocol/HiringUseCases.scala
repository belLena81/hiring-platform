package com.example.graphQL.cats.service.protocol

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, PageSize}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import java.util.UUID

trait HiringReadModel {
  def user(id: com.example.graphQL.cats.domain.model.Identifiers.UserId): UseCaseIO[Option[User]]
  def viewer(actor: ActorContext): UseCaseIO[AuthenticatedActor]
  def users(ids: List[com.example.graphQL.cats.domain.model.Identifiers.UserId]): UseCaseIO[List[User]]
  def canViewUserEmail(actor: ActorContext, userId: UserId): UseCaseIO[Boolean]
  def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): UseCaseIO[Set[UserId]]
  def job(id: JobId): UseCaseIO[Option[Job]]
  def jobs(ids: List[JobId]): UseCaseIO[List[Job]]
  def application(id: ApplicationId): UseCaseIO[Option[Application]]
  def canViewApplication(actor: ActorContext, applicationId: ApplicationId): UseCaseIO[Unit]
  def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest): UseCaseIO[List[ApplicationEvent]]
}

trait UserAuthenticator {
  def actorFor(userId: UserId): IO[Either[RepositoryError, Option[ActorContext]]]
}

trait JobUseCases {
  def createJob(request: IdempotencyRequest, actor: ActorContext, input: CreateJobInput): UseCaseIO[Job]
  def updateJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId, input: UpdateJobInput): UseCaseIO[Job]
  def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job]
  def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job]
  def viewJob(actor: ActorContext, jobId: JobId): UseCaseIO[Job]
  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): UseCaseIO[List[Job]]
  def myJobs(actor: ActorContext, page: JobPageRequest): UseCaseIO[List[Job]]
}

trait ApplicationUseCases {
  def submitApplication(
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  ): UseCaseIO[Application]

  def myApplications(actor: ActorContext, page: ApplicationPageRequest): UseCaseIO[List[Application]]
  def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest): UseCaseIO[List[Application]]

  def changeStatus(
      request: IdempotencyRequest,
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): UseCaseIO[Application]
}

trait SearchUseCases {
  def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): UseCaseIO[List[RankedJob]]

  def recommendedJobs(actor: ActorContext, first: PageSize, searchId: UUID): UseCaseIO[List[RankedJob]]

  def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      first: PageSize,
      searchId: UUID
  ): UseCaseIO[List[RankedCandidate]]
}

trait InteractionUseCases {
  def recordJobView(request: IdempotencyRequest, actor: ActorContext, eventId: UUID, jobId: JobId, searchId: Option[UUID]): UseCaseIO[Unit]
  def recordSearchResultClick(request: IdempotencyRequest, actor: ActorContext, eventId: UUID, searchId: UUID, resultId: String): UseCaseIO[Unit]
}

object InteractionUseCases {
  def noop: InteractionUseCases = new InteractionUseCases {
    override def recordJobView(request: IdempotencyRequest, actor: ActorContext, eventId: UUID, jobId: JobId, searchId: Option[UUID]): UseCaseIO[Unit] = {
      val _ = (request, actor, eventId, jobId, searchId)
      UseCaseIO.pure(())
    }
    override def recordSearchResultClick(request: IdempotencyRequest, actor: ActorContext, eventId: UUID, searchId: UUID, resultId: String): UseCaseIO[Unit] = {
      val _ = (request, actor, eventId, searchId, resultId)
      UseCaseIO.pure(())
    }
  }
}
