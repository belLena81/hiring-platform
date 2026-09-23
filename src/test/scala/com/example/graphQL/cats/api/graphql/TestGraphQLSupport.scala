package com.example.graphQL.cats.api.graphql

import cats.data.Kleisli
import cats.effect.{IO, Resource}
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.http.{ClientAddressResolver, HiringApiRoutes}
import com.example.graphQL.cats.config.{AuthRateLimitConfig, TrustedProxyConfig}
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserPageRequest}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, ProbeResult}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{
  AccountProfileInput,
  AccountUseCases,
  ApplicationUseCases,
  BootstrapAdminInput,
  HiringReadModel,
  IdempotencyRequest,
  JobUseCases,
  LoginInput,
  SignUpInput,
  UseCaseIO
}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.JobSearchFilter
import io.circe.Json
import org.http4s.Request

object TestGraphQLSupport {
  private def unsupported[A]: UseCaseIO[A] =
    UseCaseIO.liftIO(IO.raiseError(new IllegalStateException("Request context services are not configured")))

  val cursorKey: CursorCodec.CursorKey =
    CursorCodec.keyFromSecret("test-cursor-secret-01234567890123456789")

  val accountService: AccountUseCases = new AccountUseCases {
    def signUp(request: IdempotencyRequest, input: SignUpInput) = unsupported
    def bootstrapAdmin(request: IdempotencyRequest, input: BootstrapAdminInput) = unsupported
    def login(request: IdempotencyRequest, input: LoginInput) = unsupported
    def me(actor: ActorContext) = unsupported
    def updateMyProfile(request: IdempotencyRequest, actor: ActorContext, input: AccountProfileInput) = unsupported
    def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext) = unsupported
    def listUsers(actor: ActorContext, page: UserPageRequest) = unsupported
  }

  val emptyServices: HiringGraphQLServices = HiringGraphQLServices(
    new HiringReadModel {
      def user(id: UserId) = unsupported
      def viewer(actor: ActorContext) = unsupported
      def users(ids: List[UserId]) = unsupported
      def canViewUserEmail(actor: ActorContext, userId: UserId) = unsupported
      def canViewUserEmails(actor: ActorContext, userIds: List[UserId]) = unsupported
      def job(id: JobId) = unsupported
      def jobs(ids: List[JobId]) = unsupported
      def application(id: ApplicationId) = unsupported
      def canViewApplication(actor: ActorContext, applicationId: ApplicationId) = unsupported
      def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest) = unsupported
    },
    new JobUseCases {
      def createJob(request: IdempotencyRequest, actor: ActorContext, input: CreateJobInput) = unsupported
      def updateJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId, input: UpdateJobInput) = unsupported
      def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId) = unsupported
      def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId) = unsupported
      def viewJob(actor: ActorContext, jobId: JobId) = unsupported
      def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest) = unsupported
      def myJobs(actor: ActorContext, page: JobPageRequest) = unsupported
    },
    new ApplicationUseCases {
      def submitApplication(request: IdempotencyRequest, actor: ActorContext, jobId: JobId) = unsupported
      def myApplications(actor: ActorContext, page: ApplicationPageRequest) = unsupported
      def jobApplications(actor: ActorContext, jobId: JobId, page: ApplicationPageRequest) = unsupported
      def changeStatus(
          request: IdempotencyRequest,
          actor: ActorContext,
          applicationId: ApplicationId,
          target: ApplicationStatus,
          feedback: Option[String],
          reason: Option[String]
      ) = unsupported
    },
    cursorKey,
    accountService
  )

  def context(
      probe: IO[ProbeResult],
      actor: Option[ActorContext] = None,
      hiring: HiringGraphQLServices = emptyServices,
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      diagnostics: Diagnostics = Diagnostics.noop,
      requestId: Option[String] = None
  ): Resource[IO, RequestContext] =
    RequestContextFactory.resource.flatMap(
      _.resource(
        RequestContextParameters(probe, actor, hiring, hiringReady, diagnostics = diagnostics, requestId = requestId)
      )
    )

  def parseAndExecute(request: GraphQLRequest, context: RequestContext): IO[Either[HiringGraphQLSchema.Failure, Json]] =
    GraphQLDocumentCache.resource.use(_.document(request.query).flatMap {
      case Left(failure)   => IO.pure(Left(failure))
      case Right(document) => HiringGraphQLSchema.executeInContext(request, document, context)
    })

  def dependencies(
      hiring: HiringGraphQLServices = emptyServices,
      authenticate: Request[IO] => IO[Either[AuthFailure, Option[ActorContext]]] = _ => IO.pure(Right(None)),
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      authRateLimit: AuthRateLimitConfig = AuthRateLimitConfig(60, 100, 1000),
      trustedProxy: TrustedProxyConfig = TrustedProxyConfig(Nil)
  ): Resource[IO, HiringApiRoutes.Dependencies] =
    for {
      factory <- RequestContextFactory.resource
      documentCache <- GraphQLDocumentCache.resource
      limiter <- Resource.eval(AuthRateLimiter.create(authRateLimit))
    } yield HiringApiRoutes.Dependencies(
      hiring,
      Kleisli(authenticate),
      hiringReady,
      factory,
      documentCache,
      limiter,
      ClientAddressResolver(trustedProxy)
    )
}
