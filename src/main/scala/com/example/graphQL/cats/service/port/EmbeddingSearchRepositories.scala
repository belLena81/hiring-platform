package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.service.search.{CandidateRetrievalHit, JobRetrievalHit, VectorSearchQuery}
import java.time.Instant

trait EmbeddingService[F[_]] {
  def embed(input: EmbeddingInput): F[Either[EmbeddingError, EmbeddingVector]]
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

trait EmbeddingWorkRepository[F[_]] {
  def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryF[F, Unit]
  def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryF[F, Option[ClaimedEmbeddingWork]]
  def renew(claim: ClaimedEmbeddingWork, now: Instant, leaseUntil: Instant): RepositoryF[F, Boolean]
  def complete(claim: ClaimedEmbeddingWork): RepositoryF[F, Unit]

  /** Revision conflicts reschedule with chargeAttempt=false; actual failed processing consumes the durable budget. */
  def retry(claim: ClaimedEmbeddingWork, availableAt: Instant, chargeAttempt: Boolean = true): RepositoryF[F, Unit]
  def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): RepositoryF[F, Unit]
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

  /** The adapter's circuit breaker refused the call; the provider was not contacted. Callers surface it as
    * `ProviderUnavailable`, and background work does not charge an attempt for it.
    */
  case CircuitOpen
  case InvalidResponse
}

/** Returns ranked validation candidates up to the configured branch-result cap; the service applies the final page
  * size.
  */
trait SemanticSearchRepository[F[_]] {
  def authorizedJobEligibility(
      scope: com.example.graphQL.cats.service.read.HiringReadScope,
      ids: List[JobId],
      queryCandidate: Option[com.example.graphQL.cats.service.search.CandidateSearchEligibility]
  ): RepositoryF[F, List[com.example.graphQL.cats.service.search.JobSearchEligibility]]
  def authorizedCandidateEligibility(
      scope: com.example.graphQL.cats.service.read.HiringReadScope,
      queryJob: com.example.graphQL.cats.service.search.JobSearchEligibility,
      ids: List[UserId]
  ): RepositoryF[F, List[com.example.graphQL.cats.service.search.CandidateSearchEligibility]]

  def searchJobs(query: VectorSearchQuery): RepositoryF[F, List[JobRetrievalHit]]
  def recommendedJobs(query: VectorSearchQuery): RepositoryF[F, List[JobRetrievalHit]]
  def candidateMatches(query: VectorSearchQuery): RepositoryF[F, List[CandidateRetrievalHit]]
}

trait SearchSessionRepository[F[_]] {
  def save(session: SearchSession, event: OperationalEventEnvelope): RepositoryF[F, Unit]
  def find(id: java.util.UUID): RepositoryF[F, Option[SearchSession]]
  def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext
  ): RepositoryF[F, Boolean]
}
