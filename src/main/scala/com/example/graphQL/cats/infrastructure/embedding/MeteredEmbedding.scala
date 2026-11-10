package com.example.graphQL.cats.infrastructure.embedding

import cats.effect.IO
import com.example.graphQL.cats.service.port.{EmbeddingError, EmbeddingInput, EmbeddingVector}

/** An embedding with the provider-reported token count. `totalTokens` is absent when the response carried no usable
  * `usage.total_tokens`: absence is reported, never invented as zero.
  */
final case class MeteredEmbedding(vector: EmbeddingVector, totalTokens: Option[Long])

/** Optional usage-carrying view of an embedding provider, used by the evaluation runner for billing evidence. Workers
  * keep using the `EmbeddingService` port, whose contract does not change.
  */
trait MeteredEmbeddingService {
  def embedMetered(input: EmbeddingInput): IO[Either[EmbeddingError, MeteredEmbedding]]
}
