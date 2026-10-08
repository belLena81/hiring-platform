package com.example.graphQL.cats.api.graphql

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.service.{
  ActorContext,
  AuthenticatedActor,
  Diagnostics,
  LogEvent,
  LogFields,
  ProbeResult
}
import com.example.graphQL.cats.service.{AnalyticsReportingUseCases, EmbeddingCoverageUseCases}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.service.protocol.{
  AccountUseCases,
  ApplicationUseCases,
  HiringReadModel,
  InteractionUseCases,
  JobUseCases,
  SearchUseCases,
  UseCaseIO
}
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.service.events.SearchSessionHandoff
import org.typelevel.otel4s.trace.{SpanContext, Tracer}

import com.example.graphQL.cats.service.read.*

final case class HiringGraphQLServices(
    readModel: HiringReadModel,
    jobService: JobUseCases,
    applicationService: ApplicationUseCases,
    cursorKey: CursorCodec.CursorKey,
    accountService: AccountUseCases,
    semanticSearchService: Option[SearchUseCases] = None,
    interactionService: InteractionUseCases = InteractionUseCases.noop,
    searchSessionHandoff: SearchSessionHandoff = SearchSessionHandoff.noop,
    analyticsReporting: AnalyticsReportingUseCases = AnalyticsReportingUseCases.unavailable,
    interviewScheduling: Option[com.example.graphQL.cats.service.application.InterviewSchedulingService] = None,
    embeddingCoverage: EmbeddingCoverageUseCases = EmbeddingCoverageUseCases.denyAll
)

final case class EmailVisibility(userId: UserId)

final case class RequestContextParameters(
    probe: IO[ProbeResult],
    actor: Option[ActorContext],
    hiring: HiringGraphQLServices,
    ensureHiringReady: IO[ProbeResult],
    tracer: Tracer[IO] = Tracer.noop[IO],
    diagnostics: Diagnostics,
    requestId: Option[String] = None,
    clientAddress: Option[IpAddress] = None,
    rateLimit: AuthRateLimiter.Key => IO[Either[AuthRateLimiter.RateLimited, Unit]] = _ => IO.pure(Right(())),
    discoveryMaxRoots: Int = 4
)

final class RequestContext private (
    parameters: RequestContextParameters,
    sangriaAdapter: HiringGraphQLSangriaAdapter,
    closed: Deferred[IO, Unit],
    spanContext: Option[SpanContext],
    readinessProbe: IO[ProbeResult],
    hiringReadinessProbe: IO[ProbeResult],
    viewer: Option[UseCaseIO[AuthenticatedActor]],
    viewerInvalidated: Ref[IO, Boolean]
) {
  private[graphql] def effectAdapter: HiringGraphQLSangriaAdapter = sangriaAdapter

  def hiring: HiringGraphQLServices = parameters.hiring

  private[graphql] def discoveryMaxRoots: Int = parameters.discoveryMaxRoots

  def readiness: IO[ProbeResult] = readinessProbe

  def hiringAvailable: IO[ProbeResult] = hiringReadinessProbe

  private[graphql] def authenticatedActor: HiringGraphQLResult[AuthenticatedActor] =
    cats.data.EitherT((parameters.actor, viewer) match {
      case (Some(_), Some(resolveViewer)) =>
        viewerInvalidated.get.flatMap {
          case true  => IO.pure(Left(unauthorizedFailure))
          case false => read(resolveViewer).value
        }
      case _ => IO.pure(Left(unauthorizedFailure))
    })

  /** Deletion replay and status access use verified bearer claims without requiring an active viewer. */
  private[graphql] def deletionActor: HiringGraphQLResult[ActorContext] =
    cats.data.EitherT.fromEither[IO](parameters.actor.toRight(unauthorizedFailure))

  private[graphql] def invalidateViewer: IO[Unit] = viewerInvalidated.set(true)

  private[graphql] def rateLimited(operation: AuthRateLimiter.Operation): HiringGraphQLResult[Unit] =
    cats.data.EitherT(
      parameters
        .rateLimit(AuthRateLimiter.Key(parameters.clientAddress, operation))
        .map(_.leftMap(rejection => HiringGraphQLFailure.RateLimited(rejection.retryAfterSeconds)))
    )

  private[graphql] def requestScoped[A](action: HiringGraphQLResult[A]): HiringGraphQLResult[A] =
    cats.data.EitherT(
      IO.race(closed.get, spanContext.fold(action.value)(parameters.tracer.childScope(_)(action.value))).map {
        case Left(_)        => Left(HiringGraphQLFailure.RequestClosed)
        case Right(outcome) => outcome
      }
    )

  private[graphql] def traceField[A](name: String, action: HiringGraphQLResult[A]): HiringGraphQLResult[A] =
    cats.data.EitherT(parameters.tracer.span(s"graphql.field.$name").surround(action.value))

  private[graphql] def reportExecutionFailure(error: Throwable): IO[Unit] =
    parameters.diagnostics.emit(LogEvent.RuntimeFailed, parameters.requestId, fields = LogFields.failure(error))

  def relatedUsers(keys: List[UserRelationKey]): HiringGraphQLResult[List[RelatedUser]] =
    authenticatedActor.flatMap(actor => read(parameters.hiring.readModel.relatedUsers(actor, keys.distinct)))

  def relatedJobs(keys: List[JobRelationKey]): HiringGraphQLResult[List[RelatedJob]] =
    authenticatedActor.flatMap(actor => read(parameters.hiring.readModel.relatedJobs(actor, keys.distinct)))

  def users(ids: List[UserId]): HiringGraphQLResult[List[User]] =
    read(parameters.hiring.readModel.users(ids.distinct))

  def jobs(ids: List[JobId]): HiringGraphQLResult[List[Job]] =
    read(parameters.hiring.readModel.jobs(ids.distinct))

  def visibleEmailUsers(ids: List[UserId]): HiringGraphQLResult[List[EmailVisibility]] =
    parameters.actor match {
      case None    => cats.data.EitherT.pure[IO, HiringGraphQLFailure](Nil)
      case Some(_) =>
        authenticatedActor.flatMap(current =>
          read(parameters.hiring.readModel.canViewUserEmails(current, ids.distinct))
            .map(_.toList.map(EmailVisibility(_)))
        )
    }

  private def read[A](result: UseCaseIO[A]): HiringGraphQLResult[A] =
    cats.data.EitherT(result.value.map(_.leftMap(HiringGraphQLFailure.UseCase(_))))

  private def unauthorizedFailure: HiringGraphQLFailure =
    HiringGraphQLFailure.UseCase(
      UseCaseError.Authentication(com.example.graphQL.cats.service.AuthenticationError.Unauthorized)
    )

}

final class RequestContextFactory private (dispatcher: Dispatcher[IO]) {
  def resource(parameters: RequestContextParameters): Resource[IO, RequestContext] =
    Resource.eval(parameters.tracer.currentSpanContext).flatMap { spanContext =>
      RequestContext.withAdapter(new HiringGraphQLSangriaAdapter(dispatcher), parameters, spanContext)
    }
}

object RequestContextFactory {
  def resource: Resource[IO, RequestContextFactory] =
    Dispatcher.parallel[IO](await = false).map(new RequestContextFactory(_))
}

object RequestContext {
  private[graphql] def withAdapter(
      sangriaAdapter: HiringGraphQLSangriaAdapter,
      parameters: RequestContextParameters,
      spanContext: Option[SpanContext]
  ): Resource[IO, RequestContext] =
    for {
      closed <- Resource.eval(Deferred[IO, Unit])
      memoized <- Resource.eval(parameters.probe.memoize)
      memoizedHiringReady <- Resource.eval(parameters.ensureHiringReady.memoize)
      memoizedViewer <- Resource.eval(
        parameters.actor
          .traverse(actor => parameters.hiring.readModel.viewer(actor).value.memoize)
          .map(_.map(UseCaseIO.fromIO))
      )
      viewerInvalidated <- Resource.eval(Ref.of[IO, Boolean](false))
      context = new RequestContext(
        parameters,
        sangriaAdapter,
        closed,
        spanContext,
        memoized,
        memoizedHiringReady,
        memoizedViewer,
        viewerInvalidated
      )
      _ <- Resource.onFinalize(closed.complete(()).void)
    } yield context
}
