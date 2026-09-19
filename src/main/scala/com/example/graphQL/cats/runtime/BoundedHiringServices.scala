package com.example.graphQL.cats.runtime

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User, UserPageRequest}
import com.example.graphQL.cats.service.{ActorContext, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.*
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, PageSize}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/** Bounds resolver-owned effects before they cross Sangria's non-cancellable Future boundary. */
private[runtime] object BoundedHiringServices {
  private final class ResolverTimedOut extends RuntimeException(null, null, false, false)

  private def typed[A](timeout: FiniteDuration)(action: IO[Either[UseCaseError, A]]): IO[Either[UseCaseError, A]] =
    action.timeoutTo(timeout, IO.pure(Left(UseCaseError.repository(RepositoryError.Unavailable))))

  private def read[A](timeout: FiniteDuration)(action: IO[A]): IO[A] =
    action.timeoutTo(timeout, IO.raiseError(new ResolverTimedOut))

  def readModel(delegate: HiringReadModel[IO], timeout: FiniteDuration): HiringReadModel[IO] = new HiringReadModel[IO] {
    def user(id: UserId): IO[Option[User]] = read(timeout)(delegate.user(id))
    def users(ids: List[UserId]): IO[List[User]] = read(timeout)(delegate.users(ids))
    def canViewUserEmail(actor: ActorContext, id: UserId): IO[Boolean] = read(timeout)(delegate.canViewUserEmail(actor, id))
    def canViewUserEmails(actor: ActorContext, ids: List[UserId]): IO[Set[UserId]] = read(timeout)(delegate.canViewUserEmails(actor, ids))
    def job(id: JobId): IO[Option[Job]] = read(timeout)(delegate.job(id))
    def jobs(ids: List[JobId]): IO[List[Job]] = read(timeout)(delegate.jobs(ids))
    def application(id: ApplicationId): IO[Option[Application]] = read(timeout)(delegate.application(id))
    def canViewApplication(actor: ActorContext, id: ApplicationId): IO[Either[UseCaseError, Unit]] = typed(timeout)(delegate.canViewApplication(actor, id))
    def applicationHistory(id: ApplicationId, page: ApplicationEventPageRequest): IO[List[ApplicationEvent]] = read(timeout)(delegate.applicationHistory(id, page))
  }

  def jobs(delegate: JobUseCases[IO], timeout: FiniteDuration): JobUseCases[IO] = new JobUseCases[IO] {
    def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, id: JobId) = typed(timeout)(delegate.createJob(actor, input, now, id))
    def updateJob(actor: ActorContext, id: JobId, input: UpdateJobInput, now: Instant) = typed(timeout)(delegate.updateJob(actor, id, input, now))
    def publishJob(actor: ActorContext, id: JobId, now: Instant) = typed(timeout)(delegate.publishJob(actor, id, now))
    def closeJob(actor: ActorContext, id: JobId, now: Instant) = typed(timeout)(delegate.closeJob(actor, id, now))
    def viewJob(actor: ActorContext, id: JobId) = typed(timeout)(delegate.viewJob(actor, id))
    def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest) = typed(timeout)(delegate.searchOpenJobs(actor, filter, page))
    def myJobs(actor: ActorContext, page: JobPageRequest) = typed(timeout)(delegate.myJobs(actor, page))
  }

  def applications(delegate: ApplicationUseCases[IO], timeout: FiniteDuration): ApplicationUseCases[IO] = new ApplicationUseCases[IO] {
    def submitApplication(actor: ActorContext, jobId: JobId, id: ApplicationId, event: ApplicationEventId, now: Instant) = typed(timeout)(delegate.submitApplication(actor, jobId, id, event, now))
    def myApplications(actor: ActorContext, page: ApplicationPageRequest) = typed(timeout)(delegate.myApplications(actor, page))
    def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest) = typed(timeout)(delegate.jobApplications(actor, jobId, page))
    def changeStatus(actor: ActorContext, id: ApplicationId, target: ApplicationStatus, feedback: Option[String], reason: Option[String], event: ApplicationEventId, now: Instant) = typed(timeout)(delegate.changeStatus(actor, id, target, feedback, reason, event, now))
  }

  def accounts(delegate: AccountUseCases[IO], timeout: FiniteDuration): AccountUseCases[IO] = new AccountUseCases[IO] {
    def signUp(input: SignUpInput, now: Instant, id: UserId) = typed(timeout)(delegate.signUp(input, now, id))
    def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, id: UserId) = typed(timeout)(delegate.bootstrapAdmin(input, now, id))
    def login(input: LoginInput, now: Instant) = typed(timeout)(delegate.login(input, now))
    def me(actor: ActorContext) = typed(timeout)(delegate.me(actor))
    def updateMyProfile(actor: ActorContext, input: AccountProfileInput) = typed(timeout)(delegate.updateMyProfile(actor, input))
    def deleteMyAccount(actor: ActorContext, now: Instant) = typed(timeout)(delegate.deleteMyAccount(actor, now))
    def listUsers(actor: ActorContext, page: UserPageRequest) = typed(timeout)(delegate.listUsers(actor, page))
  }

  def search(delegate: SearchUseCases[IO], timeout: FiniteDuration): SearchUseCases[IO] = new SearchUseCases[IO] {
    def semanticJobSearch(actor: ActorContext, text: String, filter: JobSearchFilter, first: PageSize, id: UUID): IO[Either[UseCaseError, List[RankedJob]]] = typed(timeout)(delegate.semanticJobSearch(actor, text, filter, first, id))
    def recommendedJobs(actor: ActorContext, first: PageSize, id: UUID): IO[Either[UseCaseError, List[RankedJob]]] = typed(timeout)(delegate.recommendedJobs(actor, first, id))
    def candidateMatches(actor: ActorContext, jobId: JobId, first: PageSize, id: UUID): IO[Either[UseCaseError, List[RankedCandidate]]] = typed(timeout)(delegate.candidateMatches(actor, jobId, first, id))
  }
}
