package com.example.graphQL.cats.infrastructure.kafka

import com.example.graphQL.cats.config.{KafkaConfig, KafkaPublisherConfig, KafkaConsumerConfig, InterviewRuntimeConfig}
import com.example.graphQL.cats.service.port.RepositoryError
import munit.FunSuite
import java.util.UUID

final class KafkaProducerGenerationRetirementSpec extends FunSuite {
  private val config = KafkaConfig(
    true,
    "localhost:9092",
    "hiring.operational-events",
    "test",
    KafkaPublisherConfig("worker", 1, 60, 1, 5, 1000, Some("publisher"), Some("publisher-secret")),
    KafkaConsumerConfig(false, 8, 7),
    interview = InterviewRuntimeConfig(
      orchestratorUsername = Some("orchestrator"),
      orchestratorPassword = Some("orchestrator-secret"),
      workerUsername = Some("worker"),
      workerPassword = Some("worker-secret")
    )
  )

  test("retirement selects only the generation's owning principal") {
    val id = UUID.randomUUID().toString
    assertEquals(
      KafkaProducerGenerationRetirement.credentials(config, s"hiring-publisher-$id"),
      Right("publisher" -> "publisher-secret")
    )
    assertEquals(
      KafkaProducerGenerationRetirement.credentials(config, s"hiring-interview-orchestrator-$id"),
      Right("orchestrator" -> "orchestrator-secret")
    )
    assertEquals(
      KafkaProducerGenerationRetirement.credentials(config, s"hiring-interview-worker-$id"),
      Right("worker" -> "worker-secret")
    )
  }

  test("unknown, noncanonical and unconfigured generation identities fail closed") {
    val invalid = List("arbitrary-" + UUID.randomUUID(), "hiring-publisher-1-1-1-1-1", "hiring-publisher-not-a-uuid")
    invalid.foreach(id =>
      assertEquals(KafkaProducerGenerationRetirement.credentials(config, id), Left(RepositoryError.InvalidStoredData))
    )
    assertEquals(
      KafkaProducerGenerationRetirement
        .credentials(config.copy(interview = InterviewRuntimeConfig()), "hiring-interview-worker-" + UUID.randomUUID()),
      Left(RepositoryError.InvalidStoredData)
    )
  }
}
