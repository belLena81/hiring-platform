package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, JobSubmissionSnapshot}
import com.example.graphQL.cats.domain.pagination.{ApplicationEventPageRequest, ApplicationPageRequest}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.read.HiringReadScope

trait ApplicationRepository {
  def find(id: ApplicationId): RepositoryIO[Option[Application]]
  def findByCandidate(scope: HiringReadScope, page: ApplicationPageRequest): RepositoryIO[List[Application]]
  def findByJob(scope: HiringReadScope, jobId: JobId, page: ApplicationPageRequest): RepositoryIO[List[Application]]
  def history(
      scope: HiringReadScope,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): RepositoryIO[List[ApplicationEvent]]
  def createForOpenJobWithEvents(
      observedJob: JobSubmissionSnapshot,
      application: Application,
      initialEvent: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit]
}
