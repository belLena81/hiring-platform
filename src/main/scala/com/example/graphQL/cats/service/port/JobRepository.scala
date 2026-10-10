package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{EntityEmbedding, Job, JobSubmissionSnapshot}
import com.example.graphQL.cats.domain.pagination.JobPageRequest
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.read.*
import com.example.graphQL.cats.service.search.{
  JobDiscoveryFacets,
  JobFacetQuery,
  JobSearchFilter,
  NearbyJob,
  NearbyJobsQuery
}
import java.time.Instant

trait JobRepository {
  def relatedJobs(scope: HiringReadScope, keys: List[JobRelationKey]): RepositoryIO[List[RelatedJob]]
  def find(id: JobId): RepositoryIO[Option[Job]]
  def findVersioned(id: JobId): RepositoryIO[Option[Versioned[Job]]]
  def findSubmissionSnapshot(id: JobId): RepositoryIO[Option[JobSubmissionSnapshot]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): RepositoryIO[List[Job]]
  def nearbyJobs(scope: HiringReadScope, query: NearbyJobsQuery, limit: Int): RepositoryIO[List[NearbyJob]]
  def jobDiscoveryFacets(scope: HiringReadScope, query: JobFacetQuery): RepositoryIO[JobDiscoveryFacets]
  def findAll(page: JobPageRequest): RepositoryIO[List[Job]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): RepositoryIO[List[Job]]
  def createWithEvents(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def updateWithEvents(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Versioned[Job]]
  def updateEmbedding(id: JobId, embedding: EntityEmbedding): RepositoryIO[Unit]
  def updateEmbedding(observed: Versioned[Job], embedding: EntityEmbedding): RepositoryIO[Unit]
}
