package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.port.{
  InterviewPublisherGeneration,
  InterviewPublisherRole,
  ProducerGeneration,
  ProducerGenerationId,
  ProducerWorkerId
}
import java.util.UUID

/** Deterministic producer generations for integration fixtures. */
object ProducerGenerationFixtures {

  /** The same transactional id always yields the same generation, like the single-lifetime ids these fixtures replace.
    */
  def of(transactionalId: String): ProducerGeneration =
    ProducerGeneration(
      transactionalId,
      ProducerGenerationId(UUID.nameUUIDFromBytes(transactionalId.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
    )

  /** A distinct lifetime of the same transactional id. */
  def lifetime(transactionalId: String, n: Long): ProducerGeneration =
    ProducerGeneration(transactionalId, ProducerGenerationId(new UUID(0xf00dL, n)))

  /** A fresh interview producer lifetime of the stable fixture worker. */
  def interview(role: InterviewPublisherRole): InterviewPublisherGeneration =
    InterviewPublisherGeneration(
      role,
      ProducerWorkerId.parse("interview-fixture").toOption.get,
      ProducerGenerationId(UUID.randomUUID())
    )
}
