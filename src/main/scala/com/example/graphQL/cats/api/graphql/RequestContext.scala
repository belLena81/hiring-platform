package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.service.{ActorContext, ProbeResult}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.repository.protocol.SearchSessionRepository
import com.example.graphQL.cats.service.protocol.{AccountUseCases, ApplicationUseCases, HiringReadModel, InteractionUseCases, JobUseCases, SearchUseCases}
import com.example.graphQL.cats.service.UseCaseError
import org.typelevel.otel4s.trace.{SpanContext, Tracer}
import scala.util.control.NoStackTrace

final case class HiringGraphQLServices(
    readModel: HiringReadModel[IO],
    jobService: JobUseCases[IO],
    applicationService: ApplicationUseCases[IO],
    cursorCodec: CursorCodec.CursorCodecs,
    accountService: AccountUseCases[IO],
    semanticSearchService: Option[SearchUseCases[IO]] = None,
    interactionService: InteractionUseCases[IO] = InteractionUseCases.noop[IO],
    searchSessions: SearchSessionRepository[IO] = SearchSessionRepository.noop[IO]
)

final case class EmailVisibility(userId: UserId)

final class RequestContext private (
    dispatcher: Dispatcher[IO],
    probe: IO[ProbeResult],
    hiringReady: IO[ProbeResult],
    val actor: Option[ActorContext],
    val hiring: HiringGraphQLServices,
    tracer: Tracer[IO],
    spanContext: Option[SpanContext]
) {
  def readiness: IO[ProbeResult] = probe

  def hiringAvailable: IO[ProbeResult] = hiringReady

  private[graphql] def unsafeToFuture[A](action: IO[A]) =
    dispatcher.unsafeToFuture(spanContext.fold(action)(tracer.childScope(_)(action)))

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
  def resource(
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: HiringGraphQLServices,
      ensureHiringReady: IO[ProbeResult],
      tracer: Tracer[IO] = Tracer.noop[IO]
  ): Resource[IO, RequestContext] =
    Resource.eval(tracer.currentSpanContext).flatMap { spanContext =>
      RequestContext.withDispatcher(dispatcher, probe, actor, hiring, ensureHiringReady, tracer, spanContext)
    }
}

object RequestContextFactory {
  def resource: Resource[IO, RequestContextFactory] =
    Dispatcher.parallel[IO](await = false).map(new RequestContextFactory(_))
}

object RequestContext {
  final case class ReadFailure(error: UseCaseError) extends RuntimeException with NoStackTrace

  private[graphql] def withDispatcher(
      dispatcher: Dispatcher[IO],
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: HiringGraphQLServices,
      ensureHiringReady: IO[ProbeResult],
      tracer: Tracer[IO],
      spanContext: Option[SpanContext]
  ): Resource[IO, RequestContext] =
    for {
      memoized <- Resource.eval(probe.memoize)
      memoizedHiringReady <- Resource.eval(ensureHiringReady.memoize)
      context <- Resource.eval(IO(new RequestContext(dispatcher, memoized, memoizedHiringReady, actor, hiring, tracer, spanContext)))
    } yield context
}
