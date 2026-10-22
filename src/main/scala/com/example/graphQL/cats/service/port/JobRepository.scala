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

trait JobRepository[F[_]] {
  def relatedJobs(scope: HiringReadScope, keys: List[JobRelationKey]): RepositoryF[F, List[RelatedJob]]

  /** The ownership projection used by authorization; never carries the description or embedding. */
  def findOwner(id: JobId): RepositoryF[F, Option[JobOwner]]

  /** The job for display. The embedding vector is not read: `embedding` is always `None`. */
  def find(id: JobId): RepositoryF[F, Option[Job]]

  /** The whole job including its embedding, with its revision, for compare-and-set writes and search ranking. */
  def findVersioned(id: JobId): RepositoryF[F, Option[Versioned[Job]]]
  def findSubmissionSnapshot(id: JobId): RepositoryF[F, Option[JobSubmissionSnapshot]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): RepositoryF[F, List[Job]]
  def nearbyJobs(scope: HiringReadScope, query: NearbyJobsQuery, limit: Int): RepositoryF[F, List[NearbyJob]]

  /** `matchedIds`, when present, restricts the counted jobs to that set in addition to the eligibility predicate.
    * `maxTime`, when present, is the remaining shared time budget and may only shorten the configured query deadline.
    */
  def jobDiscoveryFacets(
      scope: HiringReadScope,
      query: JobFacetQuery,
      matchedIds: Option[Set[JobId]],
      maxTime: Option[scala.concurrent.duration.FiniteDuration]
  ): RepositoryF[F, JobDiscoveryFacets]
  def findAll(page: JobPageRequest): RepositoryF[F, List[Job]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): RepositoryF[F, List[Job]]
  def createWithEvents(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryF[F, Unit]
  def updateWithEvents(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryF[F, Versioned[Job]]
  def updateEmbedding(id: JobId, embedding: EntityEmbedding): RepositoryF[F, Unit]
  def updateEmbedding(observed: Versioned[Job], embedding: EntityEmbedding): RepositoryF[F, Unit]
}
