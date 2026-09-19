package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, IOLocal, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.service.{ActorContext, ProbeResult}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.service.protocol.{AccountUseCases, ApplicationUseCases, HiringReadModel, JobUseCases, SearchUseCases}
import com.example.graphQL.cats.service.job.{CreateJobInput, UpdateJobInput}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.JobSearchFilter
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId}
import java.time.Instant
import scala.concurrent.Future

final case class HiringGraphQLServices(
    readModel: HiringReadModel[IO],
    jobService: JobUseCases[IO],
    applicationService: ApplicationUseCases[IO],
    cursorCodec: CursorCodec,
    semanticSearchService: Option[SearchUseCases[IO]] = None,
    accountService: Option[AccountUseCases[IO]] = None,
    traceLocal: Option[IOLocal[Option[com.example.graphQL.cats.service.TraceContext]]] = None
)

final case class EmailVisibility(userId: UserId)

final class RequestContext private (
    dispatcher: Dispatcher[IO],
    probe: IO[ProbeResult],
    hiringReady: IO[ProbeResult],
    val actor: Option[ActorContext],
    val hiring: HiringGraphQLServices
) {
  def readiness: Future[ProbeResult] = dispatcher.unsafeToFuture(probe)

  def hiringAvailable: IO[ProbeResult] = hiringReady

  def unsafeToFuture[A](action: IO[A]): Future[A] = dispatcher.unsafeToFuture(action)

  def users(ids: List[UserId]): IO[List[User]] =
    hiring.readModel.users(ids.distinct)

  def jobs(ids: List[JobId]): IO[List[Job]] =
    hiring.readModel.jobs(ids.distinct)

  def visibleEmailUsers(ids: List[UserId]): IO[List[EmailVisibility]] =
    actor match {
      case Some(current) => hiring.readModel.canViewUserEmails(current, ids.distinct).map(_.toList.map(EmailVisibility(_)))
      case None => IO.pure(Nil)
    }

}

final class RequestContextFactory private (dispatcher: Dispatcher[IO]) {
  def resource(
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: HiringGraphQLServices,
      ensureHiringReady: IO[ProbeResult]
  ): Resource[IO, RequestContext] =
    RequestContext.withDispatcher(dispatcher, probe, actor, hiring, ensureHiringReady)
}

object RequestContextFactory {
  def resource: Resource[IO, RequestContextFactory] =
    Dispatcher.parallel[IO](await = false).map(new RequestContextFactory(_))
}

object RequestContext {
  private def unsupported[A]: IO[A] = IO.raiseError(new IllegalStateException("Request context services are not configured"))

  private[api] val testCursorCodec: CursorCodec =
    CursorCodec.fromSecret("test-cursor-secret-01234567890123456789")

  private[api] val emptyServices: HiringGraphQLServices = HiringGraphQLServices(
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
    testCursorCodec
  )

  private[graphql] def withDispatcher(
      dispatcher: Dispatcher[IO],
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: HiringGraphQLServices,
      ensureHiringReady: IO[ProbeResult]
  ): Resource[IO, RequestContext] =
    for {
      memoized <- Resource.eval(probe.memoize)
      memoizedHiringReady <- Resource.eval(ensureHiringReady.memoize)
      context <- Resource.eval(IO(new RequestContext(dispatcher, memoized, memoizedHiringReady, actor, hiring)))
    } yield context
}
