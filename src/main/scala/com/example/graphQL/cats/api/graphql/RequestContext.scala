package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.service.{ActorContext, ProbeResult}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.service.protocol.{AccountUseCases, ApplicationUseCases, HiringReadModel, JobUseCases, SearchUseCases}
import scala.concurrent.Future

final case class HiringGraphQLServices(
    readModel: HiringReadModel[IO],
    jobService: JobUseCases[IO],
    applicationService: ApplicationUseCases[IO],
    semanticSearchService: Option[SearchUseCases[IO]] = None,
    accountService: Option[AccountUseCases[IO]] = None
)

final class RequestContext private (
    dispatcher: Dispatcher[IO],
    probe: IO[ProbeResult],
    hiringReady: IO[Boolean],
    val actor: Option[ActorContext],
    val hiring: Option[HiringGraphQLServices]
) {
  def readiness: Future[ProbeResult] = dispatcher.unsafeToFuture(probe)

  def hiringAvailable: IO[Boolean] = hiringReady

  def unsafeToFuture[A](action: IO[A]): Future[A] = dispatcher.unsafeToFuture(action)

  def users(ids: List[UserId]): IO[List[User]] =
    hiring.fold(IO.pure(List.empty[User]))(_.readModel.users(ids.distinct))

  def jobs(ids: List[JobId]): IO[List[Job]] =
    hiring.fold(IO.pure(List.empty[Job]))(_.readModel.jobs(ids.distinct))

  def emailFor(user: User): IO[Option[String]] =
    (actor, hiring) match {
      case (Some(current), Some(services)) =>
        services.readModel.canViewUserEmail(current, user.id).map(allowed => if (allowed) user.email else None)
      case _ => IO.pure(None)
    }

}

final class RequestContextFactory private (dispatcher: Dispatcher[IO]) {
  def resource(
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: Option[HiringGraphQLServices],
      ensureHiringReady: IO[Boolean]
  ): Resource[IO, RequestContext] =
    RequestContext.withDispatcher(dispatcher, probe, actor, hiring, ensureHiringReady)
}

object RequestContextFactory {
  def resource: Resource[IO, RequestContextFactory] =
    Dispatcher.parallel[IO](await = false).map(new RequestContextFactory(_))
}

object RequestContext {
  def resource(probe: IO[ProbeResult]): Resource[IO, RequestContext] =
    resource(probe, None, None, IO.pure(true))

  def resource(
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: Option[HiringGraphQLServices]
  ): Resource[IO, RequestContext] =
    resource(probe, actor, hiring, IO.pure(true))

  def resource(
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: Option[HiringGraphQLServices],
      ensureHiringReady: IO[Boolean]
  ): Resource[IO, RequestContext] =
    Dispatcher.parallel[IO](await = false).flatMap { dispatcher =>
      withDispatcher(dispatcher, probe, actor, hiring, ensureHiringReady)
    }

  private[graphql] def withDispatcher(
      dispatcher: Dispatcher[IO],
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: Option[HiringGraphQLServices],
      ensureHiringReady: IO[Boolean]
  ): Resource[IO, RequestContext] =
    for {
      memoized <- Resource.eval(probe.memoize)
      memoizedHiringReady <- Resource.eval(ensureHiringReady.memoize)
      context <- Resource.eval(IO(new RequestContext(dispatcher, memoized, memoizedHiringReady, actor, hiring)))
    } yield context
}
