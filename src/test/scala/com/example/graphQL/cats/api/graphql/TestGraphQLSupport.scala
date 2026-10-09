package com.example.graphQL.cats.api.graphql

import cats.data.Kleisli
import cats.effect.{IO, Resource}
import com.example.graphQL.cats.api.admission.{AuthRateLimiter, FixedWindowRateLimiter, InterviewActionRateLimiter}
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.http.{ClientAddressResolver, HiringApiRoutes}
import com.example.graphQL.cats.config.{AuthRateLimitConfig, InterviewActionRateLimitConfig, TrustedProxyConfig}
import com.example.graphQL.cats.domain.model.{AccountDeletionStatus, ApplicationStatus, UserPageRequest}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, ProbeResult}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.service.protocol.{
  AccountProfileInput,
  AccountUseCases,
  ApplicationUseCases,
  HiringReadModel,
  InteractionUseCases,
  IdempotencyRequest,
  JobUseCases,
  LoginInput,
  SignUpInput,
  UseCaseIO
}
import com.example.graphQL.cats.domain.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession, SearchSessionHandoff}
import com.example.graphQL.cats.service.search.{JobSearchFilter, SearchSessionRecording}
import io.circe.Json
import org.http4s.Request

object TestGraphQLSupport {
  private def unsupported[A]: UseCaseIO[A] =
    UseCaseIO.liftIO(IO.raiseError(new IllegalStateException("Request context services are not configured")))

  val cursorKey: CursorCodec.CursorKey =
    CursorCodec.keyFromSecret("test-cursor-secret-01234567890123456789")

  val accountService: AccountUseCases = new AccountUseCases {
    def signUp(request: IdempotencyRequest, input: SignUpInput) = unsupported
    def login(request: IdempotencyRequest, input: LoginInput) = unsupported
    def me(actor: ActorContext) = unsupported
    def updateMyProfile(request: IdempotencyRequest, actor: ActorContext, input: AccountProfileInput) = unsupported
    def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext) = unsupported
    def accountDeletionStatus(actor: ActorContext, receiptId: String) = UseCaseIO.pure(AccountDeletionStatus.NotFound)
    def listUsers(actor: ActorContext, page: UserPageRequest) = unsupported
  }

  /** Test fixture: accepts interaction events without durable recording. */
  val interactions: InteractionUseCases = new InteractionUseCases {
    def recordJobView(
        request: IdempotencyRequest,
        actor: ActorContext,
        eventId: java.util.UUID,
        jobId: JobId,
        searchId: Option[java.util.UUID]
    ) = UseCaseIO.pure(())
    def recordSearchResultClick(
        request: IdempotencyRequest,
        actor: ActorContext,
        eventId: java.util.UUID,
        searchId: java.util.UUID,
        resultId: String
    ) = UseCaseIO.pure(())
  }

  /** Test fixture: real recording service over a handoff that drops every session. */
  val searchSessions: SearchSessionRecording = SearchSessionRecording(
    new SearchSessionHandoff {
      def enqueue(session: SearchSession, event: OperationalEventEnvelope): IO[Unit] = IO.unit
    }
  )

  val emptyServices: HiringGraphQLServices = HiringGraphQLServices(
    new HiringReadModel {
      def viewer(actor: ActorContext) = unsupported
      def canViewUserEmail(actor: ActorContext, userId: UserId) = unsupported
      def canViewUserEmails(actor: ActorContext, userIds: List[UserId]) = unsupported
      def application(id: ApplicationId) = unsupported
      def canViewApplication(actor: ActorContext, applicationId: ApplicationId) = unsupported
      def relatedUsers(actor: ActorContext, keys: List[com.example.graphQL.cats.service.read.UserRelationKey]) =
        unsupported
      def relatedJobs(actor: ActorContext, keys: List[com.example.graphQL.cats.service.read.JobRelationKey]) =
        unsupported
      def applicationHistory(actor: ActorContext, applicationId: ApplicationId, page: ApplicationEventPageRequest) =
        unsupported
    },
    new JobUseCases {
      def createJob(request: IdempotencyRequest, actor: ActorContext, input: CreateJobInput) = unsupported
      def updateJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId, input: UpdateJobInput) = unsupported
      def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId) = unsupported
      def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId) = unsupported
      def viewJob(actor: ActorContext, jobId: JobId) = unsupported
      def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest) = unsupported
      def nearbyJobs(actor: ActorContext, query: com.example.graphQL.cats.service.search.NearbyJobsQuery, limit: Int) =
        unsupported
      def jobDiscoveryFacets(actor: ActorContext, query: com.example.graphQL.cats.service.search.JobFacetQuery) =
        unsupported
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
    accountService,
    interactions,
    searchSessions
  )

  def context(
      probe: IO[ProbeResult],
      actor: Option[ActorContext] = None,
      hiring: HiringGraphQLServices = emptyServices,
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      diagnostics: Diagnostics = Diagnostics.noop,
      requestId: Option[String] = None,
      interviewActionRateLimit: UserId => IO[Either[FixedWindowRateLimiter.RateLimited, Unit]] = _ => IO.pure(Right(()))
  ): Resource[IO, RequestContext] =
    RequestContextFactory.resource.flatMap(
      _.resource(
        RequestContextParameters(
          probe,
          actor,
          hiring,
          hiringReady,
          diagnostics = diagnostics,
          requestId = requestId,
          interviewActionRateLimit = interviewActionRateLimit
        )
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
      trustedProxy: TrustedProxyConfig = TrustedProxyConfig(Nil),
      interviewActionRateLimit: InterviewActionRateLimitConfig = InterviewActionRateLimitConfig(60, 1000, 1000)
  ): Resource[IO, HiringApiRoutes.Dependencies] =
    for {
      factory <- RequestContextFactory.resource
      documentCache <- GraphQLDocumentCache.resource
      limiter <- Resource.eval(AuthRateLimiter.create(authRateLimit))
      interviewActionLimiter <- Resource.eval(InterviewActionRateLimiter.create(interviewActionRateLimit))
    } yield HiringApiRoutes.Dependencies(
      hiring,
      Kleisli(authenticate),
      hiringReady,
      factory,
      documentCache,
      limiter,
      interviewActionLimiter,
      ClientAddressResolver(trustedProxy)
    )
}
