package com.example.graphQL.cats.service.job

import com.example.graphQL.cats.service.port.{JobRepository, Versioned}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import cats.Monad
import com.example.graphQL.cats.service.protocol.UseCaseF
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.Job

private[service] final class AuthorizedJobAccess[F[_]: Monad](
    authorization: ActorAuthorization[F],
    jobs: JobRepository[F]
) {
  def manage[A](actor: ActorContext, jobId: JobId)(operation: Job => UseCaseF[F, A]): UseCaseF[F, A] =
    guarded(actor, UseCaseF.found(jobs.find(jobId), "job"))(identity)(operation)

  def manageVersioned[A](actor: ActorContext, jobId: JobId)(
      operation: Versioned[Job] => UseCaseF[F, A]
  ): UseCaseF[F, A] =
    guarded(actor, UseCaseF.found(jobs.findVersioned(jobId), "job"))(_.value)(operation)

  /** The actor is resolved first, then the job is loaded and must be one the actor may manage. */
  private def guarded[J, A](actor: ActorContext, load: => UseCaseF[F, J])(job: J => Job)(
      operation: J => UseCaseF[F, A]
  ): UseCaseF[F, A] =
    for {
      user <- authorization.resolve(actor)
      observed <- load
      _ <- UseCaseF.ensure[F](authorization.canManage(user, job(observed)), UseCaseError.Domain(DomainError.Forbidden))
      result <- operation(observed)
    } yield result
}
