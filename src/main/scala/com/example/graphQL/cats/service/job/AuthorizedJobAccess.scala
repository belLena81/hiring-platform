package com.example.graphQL.cats.service.job

import com.example.graphQL.cats.service.port.{JobRepository, Versioned}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.{UseCaseIO}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.Job

private[service] final class AuthorizedJobAccess(
    authorization: ActorAuthorization,
    jobs: JobRepository
) {
  def manage[A](actor: ActorContext, jobId: JobId)(operation: Job => UseCaseIO[A]): UseCaseIO[A] =
    guarded(actor, UseCaseIO.found(jobs.find(jobId), "job"))(identity)(operation)

  def manageVersioned[A](actor: ActorContext, jobId: JobId)(
      operation: Versioned[Job] => UseCaseIO[A]
  ): UseCaseIO[A] =
    guarded(actor, UseCaseIO.found(jobs.findVersioned(jobId), "job"))(_.value)(operation)

  /** The actor is resolved first, then the job is loaded and must be one the actor may manage. */
  private def guarded[J, A](actor: ActorContext, load: => UseCaseIO[J])(job: J => Job)(
      operation: J => UseCaseIO[A]
  ): UseCaseIO[A] =
    for {
      user <- authorization.resolve(actor)
      observed <- load
      _ <- UseCaseIO.ensure(authorization.canManage(user, job(observed)), UseCaseError.Domain(DomainError.Forbidden))
      result <- operation(observed)
    } yield result
}
