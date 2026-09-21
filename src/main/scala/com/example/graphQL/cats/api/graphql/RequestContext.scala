package com.example.graphQL.cats.api.graphql

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, LogEvent, LogFields, ProbeResult}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.repository.protocol.SearchSessionRepository
import com.example.graphQL.cats.service.protocol.{AccountUseCases, ApplicationUseCases, HiringReadModel, InteractionUseCases, JobUseCases, SearchUseCases}
import com.example.graphQL.cats.service.UseCaseError
import org.typelevel.otel4s.trace.{SpanContext, Tracer}
import scala.util.control.NoStackTrace

final case class HiringGraphQLServices(
    readModel: HiringReadModel,
    jobService: JobUseCases,
    applicationService: ApplicationUseCases,
    cursorCodec: CursorCodec.CursorCodecs,
    accountService: AccountUseCases,
    semanticSearchService: Option[SearchUseCases] = None,
    interactionService: Option[InteractionUseCases] = None,
    searchSessions: SearchSessionRepository[IO] = SearchSessionRepository.noop[IO]
)

final case class EmailVisibility(userId: UserId)

final case class RequestContextParameters(
    probe: IO[ProbeResult],
    actor: Option[ActorContext],
    hiring: HiringGraphQLServices,
    ensureHiringReady: IO[ProbeResult],
    tracer: Tracer[IO] = Tracer.noop[IO],
    diagnostics: Diagnostics = Diagnostics.noop,
    requestId: Option[String] = None
)

final class RequestContext private (
    dispatcher: Dispatcher[IO],
    closed: Deferred[IO, Unit],
    probe: IO[ProbeResult],
    hiringReady: IO[ProbeResult],
    val actor: Option[ActorContext],
    val hiring: HiringGraphQLServices,
    tracer: Tracer[IO],
    spanContext: Option[SpanContext],
    diagnostics: Diagnostics,
    requestId: Option[String]
) {
  def readiness: IO[ProbeResult] = probe

  def hiringAvailable: IO[ProbeResult] = hiringReady

  private[graphql] def unsafeToFuture[A](action: IO[A]) =
    dispatcher.unsafeToFuture(requestScoped(action))

  private[graphql] def unsafeFieldToFuture[A](name: String, action: IO[A]) =
    dispatcher.unsafeToFuture(requestScoped(
      Diagnostics.spanWith(diagnostics, s"graphql.field.$name", requestId = requestId)(action)(using tracer)))

  private def requestScoped[A](action: IO[A]): IO[A] =
    IO.race(closed.get, spanContext.fold(action)(tracer.childScope(_)(action))).flatMap {
      case Left(_) => IO.raiseError(RequestContext.RequestClosed)
      case Right(value) => IO.pure(value)
    }

  private[graphql] def reportExecutionFailure(error: Throwable): Unit =
    try dispatcher.unsafeRunAndForget(diagnostics.emit(LogEvent.RuntimeFailed, requestId, fields = LogFields.failure(error)))
    catch case _: Throwable => ()

  def users(ids: List[UserId]): IO[List[User]] =
    read(hiring.readModel.users(ids.distinct))

  def jobs(ids: List[JobId]): IO[List[Job]] =
    read(hiring.readModel.jobs(ids.distinct))

  def visibleEmailUsers(ids: List[UserId]): IO[List[EmailVisibility]] =
    actor match {
      case Some(current) => read(hiring.readModel.canViewUserEmails(current, ids.distinct)).map(_.toList.map(EmailVisibility(_)))
      case None => IO.pure(Nil)
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
      context = new RequestContext(dispatcher, closed, memoized, memoizedHiringReady, parameters.actor, parameters.hiring,
        parameters.tracer, spanContext, parameters.diagnostics, parameters.requestId)
      _ <- Resource.onFinalize(closed.complete(()).void)
    } yield context
}
