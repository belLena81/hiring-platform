package com.example.graphQL.cats.domain.policy

/** Exponential retry delay shared by durable work queues, saturating at the maximum without overflow. */
object RetryBackoff {
  def exponentialMillis(attempt: Long, initialMillis: Long, maximumMillis: Long): Long =
    (BigInt(initialMillis) << math.max(0L, math.min(attempt - 1L, 63L)).toInt)
      .min(BigInt(maximumMillis))
      .toLong
}
