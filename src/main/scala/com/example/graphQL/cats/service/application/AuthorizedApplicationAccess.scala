package com.example.graphQL.cats.service.application

import cats.Monad
import com.example.graphQL.cats.domain.model.Application
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.{ActorRecord, ApplicationRepository, JobOwner, JobRepository}
import com.example.graphQL.cats.service.protocol.UseCaseF

/** An application together with the resolved actor that may manage it and the ownership of the application's job. */
private[service] final case class ManagedApplication(actor: ActorRecord, application: Application, job: JobOwner)

/** The one chain that lets an actor manage an application: resolve the actor, load the application, load the job's
  * ownership, and require that the actor manages that job. A missing application or job is `NotFound`; an actor that
  * may not manage it is `Forbidden`.
  */
private[service] final class AuthorizedApplicationAccess[F[_]: Monad](
    authorization: ActorAuthorization[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F]
) {
  def manage(actor: ActorContext, applicationId: ApplicationId): UseCaseF[F, ManagedApplication] =
    authorization.resolve(actor).flatMap(record => manageAs(record, applicationId))

  /** [[manage]] for an actor already resolved in this request, so no further actor read is made. */
  def manageAs(record: ActorRecord, applicationId: ApplicationId): UseCaseF[F, ManagedApplication] =
    UseCaseF.found(applications.find(applicationId), "application").flatMap(manageLoaded(record, _))

  /** [[manageAs]] for an application the caller has already loaded. */
  def manageLoaded(record: ActorRecord, application: Application): UseCaseF[F, ManagedApplication] =
    for {
      job <- UseCaseF.found(jobs.findOwner(application.jobId), "job")
      _ <- UseCaseF.forbidUnless[F](authorization.canManage(record, job))
    } yield ManagedApplication(record, application, job)
}
