package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.IO
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** The closed set of durable queues whose backlog is sampled. It is the only metric attribute value set. */
enum BacklogCapability(val label: String) {
  case EmbeddingWork extends BacklogCapability("embedding_work")
  case OperationalEventOutbox extends BacklogCapability("operational_event_outbox")
}

/** One read of a durable backlog. An empty backlog is a count of zero with no oldest age, never a missing sample. */
final case class BacklogSnapshot(pending: Long, oldestPendingAge: Option[FiniteDuration])

/** Reads one durable backlog without reading content, vectors or payloads. A failure means the sample is unobserved. */
trait BacklogSource {
  def sample(now: Instant): IO[BacklogSnapshot]
}
