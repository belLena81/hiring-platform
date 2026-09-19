package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, IOLocal, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.service.{ActorContext, ProbeResult, TraceContext}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.service.protocol.{AccountUseCases, ApplicationUseCases, HiringReadModel, JobUseCases, SearchUseCases}
import scala.concurrent.Future

final case class HiringGraphQLServices(
    readModel: HiringReadModel[IO],
    jobService: JobUseCases[IO],
    applicationService: ApplicationUseCases[IO],
    cursorCodec: CursorCodec,
    accountService: AccountUseCases[IO],
    semanticSearchService: Option[SearchUseCases[IO]] = None,
    traceLocal: Option[IOLocal[Option[com.example.graphQL.cats.service.TraceContext]]] = None
)

final case class EmailVisibility(userId: UserId)

final class RequestContext private (
    dispatcher: Dispatcher[IO],
    probe: IO[ProbeResult],
    hiringReady: IO[ProbeResult],
    val actor: Option[ActorContext],
    val hiring: HiringGraphQLServices,
    val traceContext: Option[TraceContext]
) {
  private def inRequestTrace[A](action: IO[A]): IO[A] =
    (traceContext, hiring.traceLocal) match {
      case (Some(context), Some(local)) =>
        local.get.flatMap(previous => local.set(Some(context)) *> action.guarantee(local.set(previous)))
      case _ => action
    }

  def readiness: Future[ProbeResult] = dispatcher.unsafeToFuture(inRequestTrace(probe))

  def hiringAvailable: IO[ProbeResult] = hiringReady

  def unsafeToFuture[A](action: IO[A]): Future[A] = dispatcher.unsafeToFuture(inRequestTrace(action))

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
      ensureHiringReady: IO[ProbeResult],
      traceContext: Option[TraceContext] = None
  ): Resource[IO, RequestContext] =
    RequestContext.withDispatcher(dispatcher, probe, actor, hiring, ensureHiringReady, traceContext)
}

object RequestContextFactory {
  def resource: Resource[IO, RequestContextFactory] =
    Dispatcher.parallel[IO](await = false).map(new RequestContextFactory(_))
}

object RequestContext {
  private[graphql] def withDispatcher(
      dispatcher: Dispatcher[IO],
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: HiringGraphQLServices,
      ensureHiringReady: IO[ProbeResult],
      traceContext: Option[TraceContext]
  ): Resource[IO, RequestContext] =
    for {
      memoized <- Resource.eval(probe.memoize)
      memoizedHiringReady <- Resource.eval(ensureHiringReady.memoize)
      context <- Resource.eval(IO(new RequestContext(dispatcher, memoized, memoizedHiringReady, actor, hiring, traceContext)))
    } yield context
}
