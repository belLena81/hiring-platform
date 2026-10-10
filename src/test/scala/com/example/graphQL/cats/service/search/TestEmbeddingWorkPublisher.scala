package com.example.graphQL.cats.service.search

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.port.{EmbeddingWorkKey, EmbeddingWorkKind}

/** Test-only publisher for service tests that do not exercise embedding wakeups. */
object TestEmbeddingWorkPublisher {
  val noop: EmbeddingWorkPublisher = new EmbeddingWorkPublisher {
    override def wake: IO[Unit] = IO.unit
  }
}

/** Test vocabulary: an entity change and the durable work key the repositories enqueue for it. */
enum EmbeddingWork {
  case JobChanged(id: JobId)
  case CandidateProfileChanged(id: UserId)
}

object EmbeddingWorkKeys {
  def keyFor(work: EmbeddingWork): EmbeddingWorkKey = work match {
    case EmbeddingWork.JobChanged(id)              => EmbeddingWorkKey(EmbeddingWorkKind.Job, id.value.toString)
    case EmbeddingWork.CandidateProfileChanged(id) =>
      EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, id.value.toString)
  }
}
