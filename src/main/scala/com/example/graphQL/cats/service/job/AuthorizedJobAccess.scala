package com.example.graphQL.cats.service.job

import com.example.graphQL.cats.service.port.{ActorRecord, JobOwner, JobRepository, Versioned}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.auth.ActorAuthorization
import cats.Monad
import com.example.graphQL.cats.service.protocol.UseCaseF
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.Job

private[service] final class AuthorizedJobAccess[F[_]: Monad](
    authorization: ActorAuthorization[F],
    jobs: JobRepository[F]
) {

  /** The actor is resolved first, then only the job's ownership is read and must be one the actor may manage. */
  def manage[A](actor: ActorContext, jobId: JobId)(
      operation: (ActorRecord, JobOwner) => UseCaseF[F, A]
  ): UseCaseF[F, A] =
    guarded(actor, UseCaseF.found(jobs.findOwner(jobId), "job"))(identity)(operation)

  /** Like [[manage]], but loads the whole job with its revision for a compare-and-set write. */
  def manageVersioned[A](actor: ActorContext, jobId: JobId)(
      operation: Versioned[Job] => UseCaseF[F, A]
  ): UseCaseF[F, A] =
    guarded(actor, UseCaseF.found(jobs.findVersioned(jobId), "job"))(versioned => JobOwner.of(versioned.value))(
      (_, observed) => operation(observed)
    )

  private val jobNotFound: UseCaseError = UseCaseError.Domain(DomainError.NotFound("job"))

  private def guarded[J, A](actor: ActorContext, load: => UseCaseF[F, J])(owner: J => JobOwner)(
      operation: (ActorRecord, J) => UseCaseF[F, A]
  ): UseCaseF[F, A] =
    for {
      record <- authorization.resolve(actor)
      // A role that manages no job is refused before any lookup; a job it may not manage is indistinguishable from a
      // missing one.
      _ <- UseCaseF.forbidUnless[F](authorization.canManageJobs(record))
      observed <- load
      _ <- UseCaseF.ensure[F](authorization.canManage(record, owner(observed)), jobNotFound)
      result <- operation(record, observed)
    } yield result
}
