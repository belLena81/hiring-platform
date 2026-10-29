package com.example.graphQL.cats.service.application

import cats.Monad
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.{ActorRecord, ApplicationRepository, JobOwner, JobRepository}
import cats.syntax.all.*
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
    // A role that manages nothing is refused before any lookup; this is a missing right, not a hidden resource.
    UseCaseF.forbidUnless[F](authorization.canManageJobs(record)) *>
      UseCaseF.found(applications.find(applicationId), "application").flatMap(manageLoaded(record, _))

  /** [[manageAs]] for an application the caller has already loaded. An application the actor may not manage is
    * indistinguishable from a missing one: `NotFound("application")`.
    */
  def manageLoaded(record: ActorRecord, application: Application): UseCaseF[F, ManagedApplication] =
    for {
      job <- UseCaseF.found(jobs.findOwner(application.jobId), "job")
      _ <- UseCaseF.ensure[F](authorization.canManage(record, job), applicationNotFound)
    } yield ManagedApplication(record, application, job)

  /** Who may change an application's status: a Candidate through [[own]], a Recruiter or Admin through [[manage]]. */
  def statusChangeAccess(actor: ActorContext, applicationId: ApplicationId): UseCaseF[F, ManagedApplication] =
    authorization.resolve(actor).flatMap { record =>
      if (record.role == UserRole.Candidate) own(actor, applicationId) else manageAs(record, applicationId)
    }

  /** The Candidate who owns the application, for the actions a Candidate may take on it; any other actor, or another
    * Candidate's application, is `NotFound("application")`.
    */
  def own(actor: ActorContext, applicationId: ApplicationId): UseCaseF[F, ManagedApplication] =
    for {
      record <- authorization.resolve(actor)
      application <- UseCaseF.found(applications.find(applicationId), "application")
      _ <- UseCaseF
        .ensure[F](record.role == UserRole.Candidate && application.candidateId == record.id, applicationNotFound)
      job <- UseCaseF.found(jobs.findOwner(application.jobId), "job")
    } yield ManagedApplication(record, application, job)

  private val applicationNotFound: UseCaseError = UseCaseError.Domain(DomainError.NotFound("application"))
}
