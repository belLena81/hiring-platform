package com.example.graphQL.cats.service.search

/** Pure, capped retry scheduling over validated settings and durable attempt counts. */
object EmbeddingRecoveryPolicy {
  def backoffMillis(attempt: Long, initialMillis: Long, maximumMillis: Long): Long =
    (BigInt(initialMillis) << math.max(0L, math.min(attempt - 1L, 63L)).toInt)
      .min(BigInt(maximumMillis))
      .toLong
}
