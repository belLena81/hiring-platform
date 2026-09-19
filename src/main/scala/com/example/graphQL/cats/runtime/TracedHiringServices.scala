package com.example.graphQL.cats.runtime

import cats.effect.{IO, IOLocal}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User, UserPageRequest}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, LogField, UseCaseError}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, BootstrapAdminInput, LoginInput, SignUpInput}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.*
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, PageSize}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import java.time.Instant
import java.util.UUID

/** Effect-boundary instrumentation; domain implementations remain unaware of diagnostics. */
private[runtime] object TracedHiringServices {
  private def call[A](diagnostics: Diagnostics, trace: IOLocal[Option[com.example.graphQL.cats.service.TraceContext]], name: String,
      fields: Map[LogField, String])(action: IO[A]): IO[A] =
    Diagnostics.operation(diagnostics, trace, name, fields)(action)

  def readModel(delegate: HiringReadModel[IO], diagnostics: Diagnostics, trace: IOLocal[Option[com.example.graphQL.cats.service.TraceContext]]): HiringReadModel[IO] = new HiringReadModel[IO] {
    def user(id: UserId): IO[Either[UseCaseError, Option[User]]] = call(diagnostics, trace, "repository.read.user", Map(LogField.EntityId -> id.value.toString))(delegate.user(id))
    def users(ids: List[UserId]): IO[Either[UseCaseError, List[User]]] = call(diagnostics, trace, "repository.read.users", Map(LogField.Count -> ids.size.toString))(delegate.users(ids))
    def canViewUserEmail(actor: ActorContext, id: UserId) = call(diagnostics, trace, "service.authorization.userEmail", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.canViewUserEmail(actor, id))
    def canViewUserEmails(actor: ActorContext, ids: List[UserId]) = call(diagnostics, trace, "service.authorization.userEmails", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> ids.size.toString))(delegate.canViewUserEmails(actor, ids))
    def job(id: JobId): IO[Either[UseCaseError, Option[Job]]] = call(diagnostics, trace, "repository.read.job", Map(LogField.EntityId -> id.value.toString))(delegate.job(id))
    def jobs(ids: List[JobId]): IO[Either[UseCaseError, List[Job]]] = call(diagnostics, trace, "repository.read.jobs", Map(LogField.Count -> ids.size.toString))(delegate.jobs(ids))
    def application(id: ApplicationId): IO[Either[UseCaseError, Option[Application]]] = call(diagnostics, trace, "repository.read.application", Map(LogField.EntityId -> id.value.toString))(delegate.application(id))
    def canViewApplication(actor: ActorContext, id: ApplicationId) = call(diagnostics, trace, "service.authorization.application", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.canViewApplication(actor, id))
    def applicationHistory(id: ApplicationId, page: ApplicationEventPageRequest): IO[Either[UseCaseError, List[ApplicationEvent]]] = call(diagnostics, trace, "repository.read.applicationHistory", Map(LogField.EntityId -> id.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.applicationHistory(id, page))
  }

  def jobs(delegate: JobUseCases[IO], diagnostics: Diagnostics, trace: IOLocal[Option[com.example.graphQL.cats.service.TraceContext]]): JobUseCases[IO] = new JobUseCases[IO] {
    def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, id: JobId) = call(diagnostics, trace, "service.job.create", jobFields(actor, id, input.title, input.skills, input.location.country, input.location.city, input.location.remote, Some(input.status.toString)))(delegate.createJob(actor, input, now, id))
    def updateJob(actor: ActorContext, id: JobId, input: UpdateJobInput, now: Instant) = call(diagnostics, trace, "service.job.update", jobFields(actor, id, input.title, input.skills, input.location.country, input.location.city, input.location.remote, None))(delegate.updateJob(actor, id, input, now))
    def publishJob(actor: ActorContext, id: JobId, now: Instant) = call(diagnostics, trace, "service.job.publish", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.publishJob(actor, id, now))
    def closeJob(actor: ActorContext, id: JobId, now: Instant) = call(diagnostics, trace, "service.job.close", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.closeJob(actor, id, now))
    def viewJob(actor: ActorContext, id: JobId) = call(diagnostics, trace, "service.job.view", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.viewJob(actor, id))
    def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest) = call(diagnostics, trace, "service.job.searchOpen", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.searchOpenJobs(actor, filter, page))
    def myJobs(actor: ActorContext, page: JobPageRequest) = call(diagnostics, trace, "service.job.myJobs", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.myJobs(actor, page))
  }

  def applications(delegate: ApplicationUseCases[IO], diagnostics: Diagnostics, trace: IOLocal[Option[com.example.graphQL.cats.service.TraceContext]]): ApplicationUseCases[IO] = new ApplicationUseCases[IO] {
    def submitApplication(actor: ActorContext, jobId: JobId, id: ApplicationId, event: ApplicationEventId, now: Instant) = call(diagnostics, trace, "service.application.submit", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.submitApplication(actor, jobId, id, event, now))
    def myApplications(actor: ActorContext, page: ApplicationPageRequest) = call(diagnostics, trace, "service.application.mine", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.myApplications(actor, page))
    def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest) = call(diagnostics, trace, "service.application.byJob", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> jobId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.jobApplications(actor, jobId, page))
    def changeStatus(actor: ActorContext, id: ApplicationId, target: ApplicationStatus, feedback: Option[String], reason: Option[String], event: ApplicationEventId, now: Instant) = call(diagnostics, trace, "service.application.changeStatus", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.changeStatus(actor, id, target, feedback, reason, event, now))
  }

  def accounts(delegate: AccountUseCases[IO], diagnostics: Diagnostics, trace: IOLocal[Option[com.example.graphQL.cats.service.TraceContext]]): AccountUseCases[IO] = new AccountUseCases[IO] {
    def signUp(input: SignUpInput, now: Instant, id: UserId) = call(diagnostics, trace, "service.account.signup", Map(LogField.EntityId -> id.value.toString))(delegate.signUp(input, now, id))
    def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, id: UserId) = call(diagnostics, trace, "service.account.bootstrapAdmin", Map(LogField.EntityId -> id.value.toString))(delegate.bootstrapAdmin(input, now, id))
    def login(input: LoginInput, now: Instant) = call(diagnostics, trace, "service.account.login", Map.empty)(delegate.login(input, now))
    def me(actor: ActorContext) = call(diagnostics, trace, "service.account.me", Map(LogField.ActorId -> actor.userId.value.toString))(delegate.me(actor))
    def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant) =
      call(diagnostics, trace, "service.account.updateProfile", Map(LogField.ActorId -> actor.userId.value.toString))(delegate.updateMyProfile(actor, input, now))
    def deleteMyAccount(actor: ActorContext, now: Instant) = call(diagnostics, trace, "service.account.delete", Map(LogField.ActorId -> actor.userId.value.toString))(delegate.deleteMyAccount(actor, now))
    def listUsers(actor: ActorContext, page: UserPageRequest) = call(diagnostics, trace, "service.account.list", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.toString))(delegate.listUsers(actor, page))
  }

  def search(delegate: SearchUseCases[IO], diagnostics: Diagnostics, trace: IOLocal[Option[com.example.graphQL.cats.service.TraceContext]]): SearchUseCases[IO] = new SearchUseCases[IO] {
    def semanticJobSearch(actor: ActorContext, text: String, filter: JobSearchFilter, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedJob]]] = call(diagnostics, trace, "service.search.jobs", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> first.value.toString))(delegate.semanticJobSearch(actor, text, filter, first, id))
    def recommendedJobs(actor: ActorContext, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedJob]]] = call(diagnostics, trace, "service.search.recommended", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> first.value.toString))(delegate.recommendedJobs(actor, first, id))
    def candidateMatches(actor: ActorContext, jobId: JobId, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedCandidate]]] = call(diagnostics, trace, "service.search.candidates", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> jobId.value.toString, LogField.Count -> first.value.toString))(delegate.candidateMatches(actor, jobId, first, id))
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
