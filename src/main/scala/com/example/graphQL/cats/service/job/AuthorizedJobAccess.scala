package com.example.graphQL.cats.service.job

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.repository.protocol.JobRepository
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.auth.ActorAuthorization
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
  )(operation: Job => IO[Either[UseCaseError, A]]): IO[Either[UseCaseError, A]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[IO](authorization.canManage(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
      result <- EitherT(operation(job))
    } yield result).value
}
