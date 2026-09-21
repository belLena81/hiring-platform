package com.example.graphQL.cats.runtime

import cats.effect.IO
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
import org.typelevel.otel4s.trace.Tracer

/** Effect-boundary instrumentation; domain implementations remain unaware of diagnostics. */
private[runtime] object TracedHiringServices {
  private def call[A](diagnostics: Diagnostics, tracer: Tracer[IO], name: String,
      fields: Map[LogField, String])(action: IO[A]): IO[A] =
    Diagnostics.operation(diagnostics, name, fields)(action)(using tracer)

  def readModel(delegate: HiringReadModel[IO], diagnostics: Diagnostics, tracer: Tracer[IO]): HiringReadModel[IO] = new HiringReadModel[IO] {
    def user(id: UserId): IO[Either[UseCaseError, Option[User]]] = call(diagnostics, tracer, "repository.read.user", Map(LogField.EntityId -> id.value.toString))(delegate.user(id))
    def users(ids: List[UserId]): IO[Either[UseCaseError, List[User]]] = call(diagnostics, tracer, "repository.read.users", Map(LogField.Count -> ids.size.toString))(delegate.users(ids))
    def canViewUserEmail(actor: ActorContext, id: UserId) = call(diagnostics, tracer, "service.authorization.userEmail", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.canViewUserEmail(actor, id))
    def canViewUserEmails(actor: ActorContext, ids: List[UserId]) = call(diagnostics, tracer, "service.authorization.userEmails", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> ids.size.toString))(delegate.canViewUserEmails(actor, ids))
    def job(id: JobId): IO[Either[UseCaseError, Option[Job]]] = call(diagnostics, tracer, "repository.read.job", Map(LogField.EntityId -> id.value.toString))(delegate.job(id))
    def jobs(ids: List[JobId]): IO[Either[UseCaseError, List[Job]]] = call(diagnostics, tracer, "repository.read.jobs", Map(LogField.Count -> ids.size.toString))(delegate.jobs(ids))
    def application(id: ApplicationId): IO[Either[UseCaseError, Option[Application]]] = call(diagnostics, tracer, "repository.read.application", Map(LogField.EntityId -> id.value.toString))(delegate.application(id))
    def canViewApplication(actor: ActorContext, id: ApplicationId) = call(diagnostics, tracer, "service.authorization.application", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.canViewApplication(actor, id))
    def applicationHistory(id: ApplicationId, page: ApplicationEventPageRequest): IO[Either[UseCaseError, List[ApplicationEvent]]] = call(diagnostics, tracer, "repository.read.applicationHistory", Map(LogField.EntityId -> id.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.applicationHistory(id, page))
  }

  def jobs(delegate: JobUseCases[IO], diagnostics: Diagnostics, tracer: Tracer[IO]): JobUseCases[IO] = new JobUseCases[IO] {
    def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, id: JobId) = call(diagnostics, tracer, "service.job.create", jobFields(actor, id, input.title, input.skills, input.location.country, input.location.city, input.location.remote, Some(input.status.toString)))(delegate.createJob(actor, input, now, id))
    def updateJob(actor: ActorContext, id: JobId, input: UpdateJobInput, now: Instant) = call(diagnostics, tracer, "service.job.update", jobFields(actor, id, input.title, input.skills, input.location.country, input.location.city, input.location.remote, None))(delegate.updateJob(actor, id, input, now))
    def publishJob(actor: ActorContext, id: JobId, now: Instant) = call(diagnostics, tracer, "service.job.publish", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.publishJob(actor, id, now))
    def closeJob(actor: ActorContext, id: JobId, now: Instant) = call(diagnostics, tracer, "service.job.close", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.closeJob(actor, id, now))
    def viewJob(actor: ActorContext, id: JobId) = call(diagnostics, tracer, "service.job.view", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.viewJob(actor, id))
    def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest) = call(diagnostics, tracer, "service.job.searchOpen", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.searchOpenJobs(actor, filter, page))
    def myJobs(actor: ActorContext, page: JobPageRequest) = call(diagnostics, tracer, "service.job.myJobs", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.myJobs(actor, page))
  }

  def applications(delegate: ApplicationUseCases[IO], diagnostics: Diagnostics, tracer: Tracer[IO]): ApplicationUseCases[IO] = new ApplicationUseCases[IO] {
    def submitApplication(actor: ActorContext, jobId: JobId, id: ApplicationId, event: ApplicationEventId, now: Instant) = call(diagnostics, tracer, "service.application.submit", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.submitApplication(actor, jobId, id, event, now))
    def myApplications(actor: ActorContext, page: ApplicationPageRequest) = call(diagnostics, tracer, "service.application.mine", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.myApplications(actor, page))
    def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest) = call(diagnostics, tracer, "service.application.byJob", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> jobId.value.toString, LogField.Count -> page.pageSize.value.toString))(delegate.jobApplications(actor, jobId, page))
    def changeStatus(actor: ActorContext, id: ApplicationId, target: ApplicationStatus, feedback: Option[String], reason: Option[String], event: ApplicationEventId, now: Instant) = call(diagnostics, tracer, "service.application.changeStatus", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> id.value.toString))(delegate.changeStatus(actor, id, target, feedback, reason, event, now))
  }

  def accounts(delegate: AccountUseCases[IO], diagnostics: Diagnostics, tracer: Tracer[IO]): AccountUseCases[IO] = new AccountUseCases[IO] {
    def signUp(input: SignUpInput, now: Instant, id: UserId) = call(diagnostics, tracer, "service.account.signup", Map(LogField.EntityId -> id.value.toString))(delegate.signUp(input, now, id))
    def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, id: UserId) = call(diagnostics, tracer, "service.account.bootstrapAdmin", Map(LogField.EntityId -> id.value.toString))(delegate.bootstrapAdmin(input, now, id))
    def login(input: LoginInput, now: Instant) = call(diagnostics, tracer, "service.account.login", Map.empty)(delegate.login(input, now))
    def me(actor: ActorContext) = call(diagnostics, tracer, "service.account.me", Map(LogField.ActorId -> actor.userId.value.toString))(delegate.me(actor))
    def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant) =
      call(diagnostics, tracer, "service.account.updateProfile", Map(LogField.ActorId -> actor.userId.value.toString))(delegate.updateMyProfile(actor, input, now))
    def deleteMyAccount(actor: ActorContext, now: Instant) = call(diagnostics, tracer, "service.account.delete", Map(LogField.ActorId -> actor.userId.value.toString))(delegate.deleteMyAccount(actor, now))
    def listUsers(actor: ActorContext, page: UserPageRequest) = call(diagnostics, tracer, "service.account.list", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> page.pageSize.toString))(delegate.listUsers(actor, page))
  }

  def search(delegate: SearchUseCases[IO], diagnostics: Diagnostics, tracer: Tracer[IO]): SearchUseCases[IO] = new SearchUseCases[IO] {
    def semanticJobSearch(actor: ActorContext, text: String, filter: JobSearchFilter, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedJob]]] = call(diagnostics, tracer, "service.search.jobs", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> first.value.toString))(delegate.semanticJobSearch(actor, text, filter, first, id))
    def recommendedJobs(actor: ActorContext, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedJob]]] = call(diagnostics, tracer, "service.search.recommended", Map(LogField.ActorId -> actor.userId.value.toString, LogField.Count -> first.value.toString))(delegate.recommendedJobs(actor, first, id))
    def candidateMatches(actor: ActorContext, jobId: JobId, first: PageSize, id: UUID): IO[Either[com.example.graphQL.cats.service.UseCaseError, List[RankedCandidate]]] = call(diagnostics, tracer, "service.search.candidates", Map(LogField.ActorId -> actor.userId.value.toString, LogField.EntityId -> jobId.value.toString, LogField.Count -> first.value.toString))(delegate.candidateMatches(actor, jobId, first, id))
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
