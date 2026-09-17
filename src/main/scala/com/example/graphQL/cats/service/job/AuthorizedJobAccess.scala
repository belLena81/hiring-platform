package com.example.graphQL.cats.service.job

import cats.Monad
import cats.data.EitherT
import com.example.graphQL.cats.repository.protocol.JobRepository
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
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
      job <- EitherT.fromOptionF(jobs.find(jobId), DomainError.NotFound("job"): UseCaseError)
      _ <- EitherT.cond[F](authorization.canManage(user, job), (), DomainError.Forbidden: UseCaseError)
      result <- EitherT(operation(job))
    } yield result).value
}
