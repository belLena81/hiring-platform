package com.example.graphQL.cats.service.job

import com.example.graphQL.cats.repository.protocol.{JobRepository, Versioned}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.{UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.Job

private[service] final class AuthorizedJobAccess(
    authorization: ActorAuthorization,
    jobs: JobRepository
) {
  def manage[A](
      actor: ActorContext,
      jobId: JobId
  )(operation: Job => UseCaseIO[A]): UseCaseIO[A] =
    for {
      user <- authorization.resolve(actor)
      job <- UseCase
        .repository(jobs.find(jobId))
        .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- UseCase.fromEither(
        Either.cond(authorization.canManage(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
      )
      result <- operation(job)
    } yield result

  def manageVersioned[A](
      actor: ActorContext,
      jobId: JobId
  )(operation: Versioned[Job] => UseCaseIO[A]): UseCaseIO[A] =
    for {
      user <- authorization.resolve(actor)
      observed <- UseCase
        .repository(jobs.findVersioned(jobId))
        .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- UseCase.fromEither(
        Either.cond(authorization.canManage(user, observed.value), (), UseCaseError.Domain(DomainError.Forbidden))
      )
      result <- operation(observed)
    } yield result
}
