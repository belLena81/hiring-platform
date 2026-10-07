package com.example.graphQL.cats.config

import munit.FunSuite

class InterviewWorkflowConfigSpec extends FunSuite {
  test("publication polling accepts only the bounded interval and defaults to one second") {
    List(100, 1000, 60000).foreach { interval =>
      assertEquals(
        KafkaConfigValidation
          .validInterview(
            RawInterviewRuntimeConfig(publicationPollIntervalMs = Some(interval))
          )
          .toOption
          .map(_.publicationPollIntervalMs),
        Some(interval)
      )
    }
    List(Int.MinValue, 0, 99, 60001, Int.MaxValue).foreach { interval =>
      assert(
        KafkaConfigValidation
          .validInterview(
            RawInterviewRuntimeConfig(publicationPollIntervalMs = Some(interval))
          )
          .isInvalid
      )
    }
  }
  test("disabled workflow defaults preserve existing operational Kafka configuration") {
    assertEquals(
      KafkaConfigValidation.validInterview(RawInterviewRuntimeConfig()).toOption,
      Some(InterviewRuntimeConfig())
    )
  }
  test("enabled workflow rejects absent or shared principals and unsafe leases") {
    assert(KafkaConfigValidation.validInterview(RawInterviewRuntimeConfig(enabled = true)).isInvalid)
    val configured = RawInterviewRuntimeConfig(
      enabled = true,
      orchestratorUsername = Some("orchestrator"),
      orchestratorPassword = Some("test-one"),
      workerUsername = Some("worker"),
      workerPassword = Some("test-two"),
      fencerUsername = Some("fencer"),
      fencerPassword = Some("test-three")
    )
    assert(KafkaConfigValidation.validInterview(configured).isValid)
    assert(
      KafkaConfigValidation.validInterview(configured.copy(workerUsername = configured.orchestratorUsername)).isInvalid
    )
    assert(KafkaConfigValidation.validInterview(configured.copy(claimSeconds = 10)).isInvalid)
    assert(KafkaConfigValidation.validInterview(configured.copy(fencerPassword = None)).isInvalid)
    assert(KafkaConfigValidation.validInterview(configured.copy(fencerUsername = configured.workerUsername)).isInvalid)
    assert(
      KafkaConfigValidation.validInterview(configured.copy(fencerUsername = configured.orchestratorUsername)).isInvalid
    )
    assert(KafkaConfigValidation.validInterview(configured, kafkaEnabled = false).isInvalid)
    assert(KafkaConfigValidation.validInterview(configured, operationalPrincipals = List("worker")).isInvalid)
    assert(KafkaConfigValidation.validInterview(configured.copy(providerTimeoutSeconds = 30)).isInvalid)
    assert(KafkaConfigValidation.validInterview(configured.copy(providerTimeoutSeconds = Int.MaxValue)).isInvalid)
    val accumulated = KafkaConfigValidation.validInterview(
      configured.copy(orchestratorPassword = None, workerPassword = None, claimSeconds = 10)
    )
    assertEquals(accumulated.swap.toOption.map(_.length), Some(3))
  }
}
