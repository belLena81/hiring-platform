package com.example.graphQL.cats.api.graphql

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor, Diagnostics, LogEvent, LogFields, ProbeResult}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.repository.protocol.{MutationReceiptRepository, SearchSessionRepository}
import com.example.graphQL.cats.service.protocol.{AccountUseCases, ApplicationUseCases, HiringReadModel, InteractionUseCases, JobUseCases, SearchUseCases}
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.service.events.SearchSessionHandoff
import org.typelevel.otel4s.trace.{SpanContext, Tracer}
import scala.util.control.NoStackTrace

final case class HiringGraphQLServices(
    readModel: HiringReadModel,
    jobService: JobUseCases,
    applicationService: ApplicationUseCases,
    cursorKey: CursorCodec.CursorKey,
    accountService: AccountUseCases,
    semanticSearchService: Option[SearchUseCases] = None,
    interactionService: InteractionUseCases = InteractionUseCases.noop,
    searchSessions: SearchSessionRepository = SearchSessionRepository.noop,
    searchSessionHandoff: SearchSessionHandoff = SearchSessionHandoff.noop,
    mutationReceipts: MutationReceiptRepository = MutationReceiptRepository.noop
)

final case class EmailVisibility(userId: UserId)

final case class RequestContextParameters(
    probe: IO[ProbeResult],
    actor: Option[ActorContext],
    hiring: HiringGraphQLServices,
    ensureHiringReady: IO[ProbeResult],
    tracer: Tracer[IO] = Tracer.noop[IO],
    diagnostics: Diagnostics = Diagnostics.noop,
    requestId: Option[String] = None,
    clientAddress: Option[IpAddress] = None,
    rateLimit: AuthRateLimiter.Key => IO[Either[AuthRateLimiter.RateLimited, Unit]] = _ => IO.pure(Right(()))
)

final class RequestContext private (
    parameters: RequestContextParameters,
    dispatcher: Dispatcher[IO],
    closed: Deferred[IO, Unit],
    spanContext: Option[SpanContext],
    readinessProbe: IO[ProbeResult],
    hiringReadinessProbe: IO[ProbeResult],
    viewer: Option[IO[Either[UseCaseError, AuthenticatedActor]]],
    viewerInvalidated: Ref[IO, Boolean]
) {
  def hiring: HiringGraphQLServices = parameters.hiring

  def readiness: IO[ProbeResult] = readinessProbe

  def hiringAvailable: IO[ProbeResult] = hiringReadinessProbe

  private[graphql] def authenticatedActor: IO[AuthenticatedActor] =
    (parameters.actor, viewer) match {
      case (Some(_), Some(resolveViewer)) =>
        viewerInvalidated.get.flatMap {
          case true => IO.raiseError(RequestContext.ReadFailure(UseCaseError.Authentication(com.example.graphQL.cats.service.AuthenticationError.Unauthorized)))
          case false => read(resolveViewer)
        }
      case _ => IO.raiseError(RequestContext.ReadFailure(UseCaseError.Authentication(com.example.graphQL.cats.service.AuthenticationError.Unauthorized)))
    }

  private[graphql] def invalidateViewer: IO[Unit] = viewerInvalidated.set(true)

  private[graphql] def rateLimited(operation: AuthRateLimiter.Operation): IO[Unit] =
    parameters.rateLimit(AuthRateLimiter.Key(parameters.clientAddress, operation)).flatMap {
      case Right(()) => IO.unit
      case Left(rejection) => IO.raiseError(RequestContext.RateLimited(rejection.retryAfterSeconds))
    }

  private[graphql] def unsafeToFuture[A](action: IO[A]) =
    dispatcher.unsafeToFuture(requestScoped(action))

  private[graphql] def unsafeFieldToFuture[A](name: String, action: IO[A]) =
    dispatcher.unsafeToFuture(requestScoped(
      Diagnostics.spanWith(parameters.diagnostics, s"graphql.field.$name", requestId = parameters.requestId)(action)(using parameters.tracer)))

  private def requestScoped[A](action: IO[A]): IO[A] =
    IO.race(closed.get, spanContext.fold(action)(parameters.tracer.childScope(_)(action))).flatMap {
      case Left(_) => IO.raiseError(RequestContext.RequestClosed)
      case Right(value) => IO.pure(value)
    }

  private[graphql] def reportExecutionFailure(error: Throwable): Unit =
    try dispatcher.unsafeRunAndForget(parameters.diagnostics.emit(LogEvent.RuntimeFailed, parameters.requestId, fields = LogFields.failure(error)))
    catch case _: Throwable => ()

  def users(ids: List[UserId]): IO[List[User]] =
    read(parameters.hiring.readModel.users(ids.distinct))

  def jobs(ids: List[JobId]): IO[List[Job]] =
    read(parameters.hiring.readModel.jobs(ids.distinct))

  def visibleEmailUsers(ids: List[UserId]): IO[List[EmailVisibility]] =
    parameters.actor match {
      case None => IO.pure(Nil)
      case Some(_) => authenticatedActor.flatMap(current =>
        read(parameters.hiring.readModel.canViewUserEmails(current, ids.distinct)).map(_.toList.map(EmailVisibility(_))))
    }

  private def read[A](result: IO[Either[UseCaseError, A]]): IO[A] =
    result.flatMap(_.fold(error => IO.raiseError(RequestContext.ReadFailure(error)), IO.pure))

}

final class RequestContextFactory private (dispatcher: Dispatcher[IO]) {
  def resource(parameters: RequestContextParameters): Resource[IO, RequestContext] =
    Resource.eval(parameters.tracer.currentSpanContext).flatMap { spanContext =>
      RequestContext.withDispatcher(dispatcher, parameters, spanContext)
    }
}

object RequestContextFactory {
  def resource: Resource[IO, RequestContextFactory] =
    Dispatcher.parallel[IO](await = false).map(new RequestContextFactory(_))
}

object RequestContext {
  final case class ReadFailure(error: UseCaseError) extends RuntimeException with NoStackTrace
  final case class FieldFailure(code: String, message: String) extends RuntimeException with NoStackTrace
  final case class RateLimited(retryAfterSeconds: Long) extends RuntimeException with NoStackTrace
  case object RequestClosed extends RuntimeException with NoStackTrace

  private[graphql] def withDispatcher(
      dispatcher: Dispatcher[IO],
      parameters: RequestContextParameters,
      spanContext: Option[SpanContext]
  ): Resource[IO, RequestContext] =
    for {
      closed <- Resource.eval(Deferred[IO, Unit])
      memoized <- Resource.eval(parameters.probe.memoize)
      memoizedHiringReady <- Resource.eval(parameters.ensureHiringReady.memoize)
      memoizedViewer <- Resource.eval(parameters.actor.traverse(actor => parameters.hiring.readModel.viewer(actor).memoize))
      viewerInvalidated <- Resource.eval(Ref.of[IO, Boolean](false))
      context = new RequestContext(parameters, dispatcher, closed, spanContext, memoized, memoizedHiringReady, memoizedViewer, viewerInvalidated)
      _ <- Resource.onFinalize(closed.complete(()).void)
    } yield context
}
