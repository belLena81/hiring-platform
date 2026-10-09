package com.example.graphQL.cats.service.job

import com.example.graphQL.cats.service.port.{JobRepository, Versioned}
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
      job <- UseCase.found(jobs.find(jobId), "job")
      _ <- UseCase.ensure(authorization.canManage(user, job), UseCaseError.Domain(DomainError.Forbidden))
      result <- operation(job)
    } yield result

  def manageVersioned[A](
      actor: ActorContext,
      jobId: JobId
  )(operation: Versioned[Job] => UseCaseIO[A]): UseCaseIO[A] =
    for {
      user <- authorization.resolve(actor)
      observed <- UseCase.found(jobs.findVersioned(jobId), "job")
      _ <- UseCase.ensure(authorization.canManage(user, observed.value), UseCaseError.Domain(DomainError.Forbidden))
      result <- operation(observed)
    } yield result
}
