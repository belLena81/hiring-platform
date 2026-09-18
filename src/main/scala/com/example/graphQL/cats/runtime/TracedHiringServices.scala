package com.example.graphQL.cats.runtime

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, LogField}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.*
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, PageSize}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import java.time.Instant
import java.util.UUID

/** Effect-boundary instrumentation; domain implementations remain unaware of diagnostics. */
private[runtime] object TracedHiringServices {
  private def call[A](diagnostics: Diagnostics, name: String, fields: Map[LogField, String])(action: IO[A]): IO[A] =
    Diagnostics.operation(diagnostics, name, fields)(action)

  def readModel(delegate: HiringReadModel[IO], diagnostics: Diagnostics): HiringReadModel[IO] = new HiringReadModel[IO] {
    def user(id: UserId): IO[Option[User]] = call(diagnostics, "repository.read.user", Map(LogField.EntityId -> id.value.toString))(delegate.user(id))
    def users(ids: List[UserId]): IO[List[User]] = call(diagnostics, "repository.read.users", Map(LogField.Count -> ids.size.toString))(delegate.users(ids))
    def canViewUserEmail(actor: ActorContext, id: UserId) = call(diagnostics, "service.authorization.userEmail", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.canViewUserEmail(actor, id))
    def job(id: JobId): IO[Option[Job]] = call(diagnostics, "repository.read.job", Map(LogField.EntityId -> id.value.toString))(delegate.job(id))
    def jobs(ids: List[JobId]): IO[List[Job]] = call(diagnostics, "repository.read.jobs", Map(LogField.Count -> ids.size.toString))(delegate.jobs(ids))
    def application(id: ApplicationId): IO[Option[Application]] = call(diagnostics, "repository.read.application", Map(LogField.EntityId -> id.value.toString))(delegate.application(id))
    def canViewApplication(actor: ActorContext, id: ApplicationId) = call(diagnostics, "service.authorization.application", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.canViewApplication(actor, id))
    def applicationHistory(id: ApplicationId, page: ApplicationEventPageRequest): IO[List[ApplicationEvent]] = call(diagnostics, "repository.read.applicationHistory", Map(LogField.EntityId -> id.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.applicationHistory(id, page))
  }

  def jobs(delegate: JobUseCases[IO], diagnostics: Diagnostics): JobUseCases[IO] = new JobUseCases[IO] {
    def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, id: JobId) = call(diagnostics, "service.job.create", jobFields(actor, id, input.title, input.skills, input.location.country, input.location.city, input.location.remote, Some(input.status.toString)))(delegate.createJob(actor, input, now, id))
    def updateJob(actor: ActorContext, id: JobId, input: UpdateJobInput, now: Instant) = call(diagnostics, "service.job.update", jobFields(actor, id, input.title, input.skills, input.location.country, input.location.city, input.location.remote, None))(delegate.updateJob(actor, id, input, now))
    def publishJob(actor: ActorContext, id: JobId, now: Instant) = call(diagnostics, "service.job.publish", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.publishJob(actor, id, now))
    def closeJob(actor: ActorContext, id: JobId, now: Instant) = call(diagnostics, "service.job.close", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.closeJob(actor, id, now))
    def viewJob(actor: ActorContext, id: JobId) = call(diagnostics, "service.job.view", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.viewJob(actor, id))
    def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest) = call(diagnostics, "service.job.searchOpen", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.searchOpenJobs(actor, filter, page))
    def myJobs(actor: ActorContext, page: JobPageRequest) = call(diagnostics, "service.job.myJobs", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.myJobs(actor, page))
  }

  def applications(delegate: ApplicationUseCases[IO], diagnostics: Diagnostics): ApplicationUseCases[IO] = new ApplicationUseCases[IO] {
    def submitApplication(actor: ActorContext, jobId: JobId, id: ApplicationId, event: ApplicationEventId, now: Instant) = call(diagnostics, "service.application.submit", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.submitApplication(actor, jobId, id, event, now))
    def myApplications(actor: ActorContext, page: ApplicationPageRequest) = call(diagnostics, "service.application.mine", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.myApplications(actor, page))
    def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest) = call(diagnostics, "service.application.byJob", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> jobId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.jobApplications(actor, jobId, page))
    def changeStatus(actor: ActorContext, id: ApplicationId, target: ApplicationStatus, feedback: Option[String], reason: Option[String], event: ApplicationEventId, now: Instant) = call(diagnostics, "service.application.changeStatus", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.changeStatus(actor, id, target, feedback, reason, event, now))
  }

  def search(delegate: SearchUseCases[IO], diagnostics: Diagnostics): SearchUseCases[IO] = new SearchUseCases[IO] {
    def semanticJobSearch(actor: ActorContext, text: String, filter: JobSearchFilter, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedJob]]] = call(diagnostics, "service.search.jobs", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> first.value.toString))(delegate.semanticJobSearch(actor, text, filter, first, id))
    def recommendedJobs(actor: ActorContext, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedJob]]] = call(diagnostics, "service.search.recommended", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> first.value.toString))(delegate.recommendedJobs(actor, first, id))
    def candidateMatches(actor: ActorContext, jobId: JobId, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedCandidate]]] = call(diagnostics, "service.search.candidates", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> jobId.value.toString, LogField.Count -> first.value.toString))(delegate.candidateMatches(actor, jobId, first, id))
  }

  private def jobFields(
      actor: ActorContext,
      id: JobId,
      title: String,
      skills: Set[String],
      country: String,
      city: String,
      remote: Boolean,
      status: Option[String]
  ): Map[LogField, String] =
    Map(
      LogField.ActorId -> actor.userId.value.toString,
      LogField.EntityId -> id.value.toString,
      LogField.Title -> title,
      LogField.Skills -> skills.toList.sorted.mkString(","),
      LogField.Country -> country,
      LogField.City -> city,
      LogField.Remote -> remote.toString
    ) ++ status.map(LogField.JobStatus -> _)
}
