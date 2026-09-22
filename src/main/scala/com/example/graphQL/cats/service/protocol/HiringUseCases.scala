package com.example.graphQL.cats.service.protocol

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, PageSize}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.repository.protocol.MutationWriteContext
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor, UseCaseError}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import java.time.Instant
import java.util.UUID

trait HiringReadModel {
  def user(id: com.example.graphQL.cats.domain.model.Identifiers.UserId): IO[Either[UseCaseError, Option[User]]]
  def viewer(actor: ActorContext): IO[Either[UseCaseError, AuthenticatedActor]]
  def users(ids: List[com.example.graphQL.cats.domain.model.Identifiers.UserId]): IO[Either[UseCaseError, List[User]]]
  def canViewUserEmail(actor: ActorContext, userId: UserId): IO[Either[UseCaseError, Boolean]]
  def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): IO[Either[UseCaseError, Set[UserId]]]
  def job(id: JobId): IO[Either[UseCaseError, Option[Job]]]
  def jobs(ids: List[JobId]): IO[Either[UseCaseError, List[Job]]]
  def application(id: ApplicationId): IO[Either[UseCaseError, Option[Application]]]
  def canViewApplication(actor: ActorContext, applicationId: ApplicationId): IO[Either[UseCaseError, Unit]]
  def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest): IO[Either[UseCaseError, List[ApplicationEvent]]]
}

trait UserAuthenticator {
  def actorFor(userId: UserId): IO[Either[RepositoryError, Option[ActorContext]]]
}

trait JobUseCases {
  def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, jobId: JobId): IO[Either[UseCaseError, Job]]
  def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, jobId: JobId, _context: MutationWriteContext): IO[Either[UseCaseError, Job]] =
    { val _ = _context; createJob(actor, input, now, jobId) }
  def updateJob(actor: ActorContext, jobId: JobId, input: UpdateJobInput, now: Instant): IO[Either[UseCaseError, Job]]
  def updateJob(actor: ActorContext, jobId: JobId, input: UpdateJobInput, now: Instant, _context: MutationWriteContext): IO[Either[UseCaseError, Job]] =
    { val _ = _context; updateJob(actor, jobId, input, now) }
  def publishJob(actor: ActorContext, jobId: JobId, now: Instant): IO[Either[UseCaseError, Job]]
  def publishJob(actor: ActorContext, jobId: JobId, now: Instant, _context: MutationWriteContext): IO[Either[UseCaseError, Job]] =
    { val _ = _context; publishJob(actor, jobId, now) }
  def closeJob(actor: ActorContext, jobId: JobId, now: Instant): IO[Either[UseCaseError, Job]]
  def closeJob(actor: ActorContext, jobId: JobId, now: Instant, _context: MutationWriteContext): IO[Either[UseCaseError, Job]] =
    { val _ = _context; closeJob(actor, jobId, now) }
  def viewJob(actor: ActorContext, jobId: JobId): IO[Either[UseCaseError, Job]]
  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): IO[Either[UseCaseError, List[Job]]]
  def myJobs(actor: ActorContext, page: JobPageRequest): IO[Either[UseCaseError, List[Job]]]
}

trait ApplicationUseCases {
  def submitApplication(
      actor: ActorContext,
      jobId: JobId,
      applicationId: ApplicationId,
      eventId: ApplicationEventId,
      now: Instant
  ): IO[Either[UseCaseError, Application]]
  def submitApplication(
      actor: ActorContext,
      jobId: JobId,
      applicationId: ApplicationId,
      eventId: ApplicationEventId,
      now: Instant,
      _context: MutationWriteContext
  ): IO[Either[UseCaseError, Application]] =
    { val _ = _context; submitApplication(actor, jobId, applicationId, eventId, now) }

  def myApplications(actor: ActorContext, page: ApplicationPageRequest): IO[Either[UseCaseError, List[Application]]]
  def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest): IO[Either[UseCaseError, List[Application]]]

  def changeStatus(
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String],
      eventId: ApplicationEventId,
      now: Instant
  ): IO[Either[UseCaseError, Application]]
  def changeStatus(
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String],
      eventId: ApplicationEventId,
      now: Instant,
      _context: MutationWriteContext
  ): IO[Either[UseCaseError, Application]] =
    { val _ = _context; changeStatus(actor, applicationId, target, feedback, reason, eventId, now) }
}

trait SearchUseCases {
  def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): IO[Either[UseCaseError, List[RankedJob]]]

  def recommendedJobs(actor: ActorContext, first: PageSize, searchId: UUID): IO[Either[UseCaseError, List[RankedJob]]]

  def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      first: PageSize,
      searchId: UUID
  ): IO[Either[UseCaseError, List[RankedCandidate]]]
}

trait InteractionUseCases {
  def recordJobView(actor: ActorContext, eventId: UUID, jobId: JobId, searchId: Option[UUID], now: Instant): IO[Either[UseCaseError, Unit]]
  def recordJobView(actor: ActorContext, eventId: UUID, jobId: JobId, searchId: Option[UUID], now: Instant, _context: MutationWriteContext): IO[Either[UseCaseError, Unit]] =
    { val _ = _context; recordJobView(actor, eventId, jobId, searchId, now) }
  def recordSearchResultClick(actor: ActorContext, eventId: UUID, searchId: UUID, resultId: String, now: Instant): IO[Either[UseCaseError, Unit]]
  def recordSearchResultClick(actor: ActorContext, eventId: UUID, searchId: UUID, resultId: String, now: Instant, _context: MutationWriteContext): IO[Either[UseCaseError, Unit]] =
    { val _ = _context; recordSearchResultClick(actor, eventId, searchId, resultId, now) }
}

object InteractionUseCases {
  def noop: InteractionUseCases = new InteractionUseCases {
    override def recordJobView(actor: ActorContext, eventId: UUID, jobId: JobId, searchId: Option[UUID], now: Instant): IO[Either[UseCaseError, Unit]] = {
      val _ = (actor, eventId, jobId, searchId, now)
      IO.pure(Right(()))
    }
    override def recordSearchResultClick(actor: ActorContext, eventId: UUID, searchId: UUID, resultId: String, now: Instant): IO[Either[UseCaseError, Unit]] = {
      val _ = (actor, eventId, searchId, resultId, now)
      IO.pure(Right(()))
    }
  }
}
