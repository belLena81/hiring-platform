package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.http.{ClientAddressResolver, FixedWindowRateLimiter, HiringApiRoutes}
import com.example.graphQL.cats.config.{AuthRateLimitConfig, TrustedProxyConfig}
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserPageRequest}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, ProbeResult}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, AccountUseCases, ApplicationUseCases, BootstrapAdminInput, HiringReadModel, JobUseCases, LoginInput, SignUpInput}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.JobSearchFilter
import org.http4s.Request
import java.time.Instant

import scala.concurrent.duration.*

object TestGraphQLSupport {
  private def unsupported[A]: IO[A] = IO.raiseError(new IllegalStateException("Request context services are not configured"))

  val cursorCodec: CursorCodec.CursorCodecs =
    CursorCodec.fromSecret("test-cursor-secret-01234567890123456789")

  val accountService: AccountUseCases[IO] = new AccountUseCases[IO] {
    def signUp(input: SignUpInput, now: Instant, userId: UserId) = unsupported
    def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: UserId) = unsupported
    def login(input: LoginInput, now: Instant) = unsupported
    def me(actor: ActorContext) = unsupported
    def updateMyProfile(actor: ActorContext, input: AccountProfileInput) = unsupported
    def deleteMyAccount(actor: ActorContext, now: Instant) = unsupported
    def listUsers(actor: ActorContext, page: UserPageRequest) = unsupported
  }

  val emptyServices: HiringGraphQLServices = HiringGraphQLServices(
    new HiringReadModel[IO] {
      def user(id: UserId) = unsupported
      def users(ids: List[UserId]) = unsupported
      def canViewUserEmail(actor: ActorContext, userId: UserId) = unsupported
      def canViewUserEmails(actor: ActorContext, userIds: List[UserId]) = unsupported
      def job(id: JobId) = unsupported
      def jobs(ids: List[JobId]) = unsupported
      def application(id: ApplicationId) = unsupported
      def canViewApplication(actor: ActorContext, applicationId: ApplicationId) = unsupported
      def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest) = unsupported
    },
    new JobUseCases[IO] {
      def createJob(actor: ActorContext, input: CreateJobInput, now: Instant, jobId: JobId) = unsupported
      def updateJob(actor: ActorContext, jobId: JobId, input: UpdateJobInput, now: Instant) = unsupported
      def publishJob(actor: ActorContext, jobId: JobId, now: Instant) = unsupported
      def closeJob(actor: ActorContext, jobId: JobId, now: Instant) = unsupported
      def viewJob(actor: ActorContext, jobId: JobId) = unsupported
      def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest) = unsupported
      def myJobs(actor: ActorContext, page: JobPageRequest) = unsupported
    },
    new ApplicationUseCases[IO] {
      def submitApplication(actor: ActorContext, jobId: JobId, applicationId: ApplicationId, eventId: ApplicationEventId, now: Instant) = unsupported
      def myApplications(actor: ActorContext, page: ApplicationPageRequest) = unsupported
      def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest) = unsupported
      def changeStatus(actor: ActorContext, applicationId: ApplicationId, target: ApplicationStatus, feedback: Option[String], reason: Option[String], eventId: ApplicationEventId, now: Instant) = unsupported
    },
    cursorCodec,
    accountService
  )

  def context(
      probe: IO[ProbeResult],
      actor: Option[ActorContext] = None,
      hiring: HiringGraphQLServices = emptyServices,
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready)
  ): Resource[IO, RequestContext] =
    RequestContextFactory.resource.flatMap(_.resource(probe, actor, hiring, hiringReady))

  def dependencies(
      hiring: HiringGraphQLServices = emptyServices,
      authenticate: Request[IO] => IO[Either[AuthFailure, Option[ActorContext]]] = _ => IO.pure(Right(None)),
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      authRateLimit: AuthRateLimitConfig = AuthRateLimitConfig(60, 100, 1000),
      trustedProxy: TrustedProxyConfig = TrustedProxyConfig(Nil),
      requestTimeout: FiniteDuration = 5.seconds
  ): Resource[IO, HiringApiRoutes.Dependencies] =
    for {
      factory <- RequestContextFactory.resource
      limiter <- Resource.eval(FixedWindowRateLimiter.create(authRateLimit))
    } yield HiringApiRoutes.Dependencies(hiring, authenticate, hiringReady, factory, limiter,
      ClientAddressResolver(trustedProxy), requestTimeout)
}
