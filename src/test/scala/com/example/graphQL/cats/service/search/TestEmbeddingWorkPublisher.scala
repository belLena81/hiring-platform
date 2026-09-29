package com.example.graphQL.cats.service.search

import cats.effect.IO

/** Test-only publisher for service tests that do not exercise embedding wakeups. */
object TestEmbeddingWorkPublisher {
  val noop: EmbeddingWorkPublisher = new EmbeddingWorkPublisher {
    override def wake: IO[Unit] = IO.unit
  }
}
