package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, JobSubmissionSnapshot, UserRole}
import com.example.graphQL.cats.domain.pagination.{ApplicationEventPageRequest, ApplicationPageRequest}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.read.HiringReadScope

/** The authenticated actor of an application write, derived from the trusted context and never from request arguments.
  * The store re-validates it inside the write transaction: the account must still be active in this role (the singleton
  * Admin for `Admin`), and a Recruiter must own the job.
  */
final case class ApplicationWriteActor(id: UserId, role: UserRole)

trait ApplicationRepository[F[_]] {
  def find(id: ApplicationId): RepositoryF[F, Option[Application]]
  def findByCandidate(scope: HiringReadScope, page: ApplicationPageRequest): RepositoryF[F, List[Application]]
  def findByJob(scope: HiringReadScope, jobId: JobId, page: ApplicationPageRequest): RepositoryF[F, List[Application]]
  def history(
      scope: HiringReadScope,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): RepositoryF[F, List[ApplicationEvent]]
  def createForOpenJobWithEvents(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      actor: ApplicationWriteActor,
      context: MutationWriteContext
  ): RepositoryF[F, Unit]
  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      actor: ApplicationWriteActor,
      context: MutationWriteContext
  ): RepositoryF[F, Unit]
}
