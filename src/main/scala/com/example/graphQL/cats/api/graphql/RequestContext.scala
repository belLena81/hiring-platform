package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref, Resource}
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
    semanticSearchService: Option[SearchUseCases[IO]] = None,
    accountService: Option[AccountUseCases[IO]] = None,
    private[graphql] traceServices: Option[TraceContext => HiringGraphQLServices] = None
) {
  private[graphql] def forTrace(trace: TraceContext): HiringGraphQLServices =
    traceServices.fold(this)(_(trace))
}

final class RequestContext private (
    dispatcher: Dispatcher[IO],
    probe: IO[ProbeResult],
    hiringReady: IO[Boolean],
    val actor: Option[ActorContext],
    val hiring: Option[HiringGraphQLServices],
    val trace: Option[TraceContext],
    userCache: Ref[IO, Map[UserId, User]],
    jobCache: Ref[IO, Map[JobId, Job]]
) {
  private var open = true

  def readiness: Future[ProbeResult] = synchronized {
    if (!open) throw new IllegalStateException("Request context is closed")
    dispatcher.unsafeToFuture(probe)
  }

  def hiringAvailable: IO[Boolean] = hiringReady

  def unsafeToFuture[A](action: IO[A]): Future[A] = synchronized {
    if (!open) throw new IllegalStateException("Request context is closed")
    dispatcher.unsafeToFuture(action)
  }

  def preloadUsers(ids: List[UserId]): IO[Unit] =
    hiring.fold(IO.unit) { services =>
      userCache.get.flatMap { cached =>
        val missing = ids.distinct.filterNot(cached.contains)
        if (missing.isEmpty) IO.unit
        else services.readModel.users(missing).flatMap(found =>
          userCache.update(_ ++ found.map(user => user.id -> user)))
      }
    }

  def preloadJobs(ids: List[JobId]): IO[Unit] =
    hiring.fold(IO.unit) { services =>
      jobCache.get.flatMap { cached =>
        val missing = ids.distinct.filterNot(cached.contains)
        if (missing.isEmpty) IO.unit
        else services.readModel.jobs(missing).flatMap(found =>
          jobCache.update(_ ++ found.map(job => job.id -> job)))
      }
    }

  def user(id: UserId): IO[Option[User]] =
    preloadUsers(List(id)) *> userCache.get.map(_.get(id))

  def job(id: JobId): IO[Option[Job]] =
    preloadJobs(List(id)) *> jobCache.get.map(_.get(id))

  private def close: IO[Unit] = IO.delay(synchronized { open = false })
}

object RequestContext {
  def resource(probe: IO[ProbeResult]): Resource[IO, RequestContext] =
    resource(probe, None, None, IO.pure(true), None)

  def resource(
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: Option[HiringGraphQLServices]
  ): Resource[IO, RequestContext] =
    resource(probe, actor, hiring, IO.pure(true), None)

  def resource(
      probe: IO[ProbeResult],
      actor: Option[ActorContext],
      hiring: Option[HiringGraphQLServices],
      ensureHiringReady: IO[Boolean],
      trace: Option[TraceContext] = None
  ): Resource[IO, RequestContext] =
    for {
      dispatcher <- Dispatcher.parallel[IO](await = false)
      memoized <- Resource.eval(probe.memoize)
      memoizedHiringReady <- Resource.eval(ensureHiringReady.memoize)
      userCache <- Resource.eval(Ref.of[IO, Map[UserId, User]](Map.empty))
      jobCache <- Resource.eval(Ref.of[IO, Map[JobId, Job]](Map.empty))
      tracedHiring = hiring.map(services => trace.fold(services)(services.forTrace))
      context <- Resource.make(IO(new RequestContext(dispatcher, memoized, memoizedHiringReady, actor, tracedHiring, trace, userCache, jobCache)))(_.close)
    } yield context
}
