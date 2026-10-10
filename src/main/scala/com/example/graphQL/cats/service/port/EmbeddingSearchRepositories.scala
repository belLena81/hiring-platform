package com.example.graphQL.cats.service.port

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.service.search.{CandidateRetrievalHit, JobRetrievalHit, VectorSearchQuery}
import java.time.Instant

trait EmbeddingService {
  def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]]
}

enum EmbeddingWorkKind {
  case Job, CandidateProfile
}

final case class EmbeddingWorkKey(kind: EmbeddingWorkKind, entityId: String) {
  val value: String = s"${kind.toString}:$entityId"
}

final case class ClaimedEmbeddingWork(
    key: EmbeddingWorkKey,
    generation: Long,
    attempts: Int,
    leaseToken: String
)

enum EmbeddingWorkFailure {
  case RetryExhausted, DocumentTooLarge, InvalidWorkKey, InvalidResponse
}

enum EmbeddingWorkState {
  case Ready, Retry, Processing, Failed
}

trait EmbeddingWorkRepository {
  def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryIO[Unit]
  def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedEmbeddingWork]]
  def renew(claim: ClaimedEmbeddingWork, now: Instant, leaseUntil: Instant): RepositoryIO[Boolean]
  def complete(claim: ClaimedEmbeddingWork): RepositoryIO[Unit]

  /** Revision conflicts reschedule with chargeAttempt=false; actual failed processing consumes the durable budget. */
  def retry(claim: ClaimedEmbeddingWork, availableAt: Instant, chargeAttempt: Boolean = true): RepositoryIO[Unit]
  def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): RepositoryIO[Unit]
}

enum EmbeddingInputType {
  case Query, Document
}

final case class EmbeddingInput(text: String, inputType: EmbeddingInputType)
final case class EmbeddingVector(values: List[Float], model: String, dimension: Int)

object EmbeddingVector {
  def validateModel(vector: EmbeddingVector, expectedModel: String): Either[EmbeddingError, EmbeddingVector] =
    Either.cond(vector.model == expectedModel, vector, EmbeddingError.InvalidResponse)
}

enum EmbeddingError {
  case ProviderUnavailable
  case InvalidResponse
}

/** Returns ranked validation candidates up to the configured branch-result cap; the service applies the final page
  * size.
  */
trait SemanticSearchRepository {
  def authorizedJobEligibility(
      scope: com.example.graphQL.cats.service.read.HiringReadScope,
      ids: List[JobId],
      queryCandidate: Option[com.example.graphQL.cats.service.search.CandidateSearchEligibility]
  ): RepositoryIO[List[com.example.graphQL.cats.service.search.JobSearchEligibility]]
  def authorizedCandidateEligibility(
      scope: com.example.graphQL.cats.service.read.HiringReadScope,
      queryJob: com.example.graphQL.cats.service.search.JobSearchEligibility,
      ids: List[UserId]
  ): RepositoryIO[List[com.example.graphQL.cats.service.search.CandidateSearchEligibility]]

  def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]]
  def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]]
  def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]]
}

trait SearchSessionRepository {
  def save(session: SearchSession, event: OperationalEventEnvelope): RepositoryIO[Unit]
  def find(id: java.util.UUID): RepositoryIO[Option[SearchSession]]
  def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext
  ): RepositoryIO[Boolean]
}
