package com.example.graphQL.cats.service.job

import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.JobRepository
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.Job

private[service] final class AuthorizedJobAccess[F[_]: Monad](
    authorization: ActorAuthorization[F],
    jobs: JobRepository[F]
) {
  def manage[A](
      actor: ActorContext,
      jobId: JobId
  )(operation: Job => F[Either[UseCaseError, A]]): F[Either[UseCaseError, A]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[F](authorization.canManage(user, job), (), UseCaseError.domain(DomainError.Forbidden))
      result <- EitherT(operation(job))
    } yield result).value
}
