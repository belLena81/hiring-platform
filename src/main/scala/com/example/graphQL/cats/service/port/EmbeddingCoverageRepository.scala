package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.service.search.{EmbeddingCoverageObservation, EmbeddingCoverageScanRequest}

/** Read-only, bounded observation of embedding coverage; implementations never write or call providers. */
trait EmbeddingCoverageRepository {
  def observe(request: EmbeddingCoverageScanRequest): RepositoryIO[EmbeddingCoverageObservation]
}
