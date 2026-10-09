package com.example.graphQL.cats.config

import io.github.iltotore.iron.autoRefine
import munit.FunSuite

class InterviewWorkflowConfigSpec extends FunSuite {
  private def configuration(overrides: String) = AppConfigFixtures.withPackagedDefaults(overrides)
  private def errorsOf(overrides: String): List[ConfigError] =
    configuration(overrides).swap.toOption.fold(fail(s"Expected rejected configuration for $overrides"))(_.toList)
  private def interviewErrors(
      raw: RawInterviewRuntimeConfig,
      kafkaEnabled: Boolean = true,
      operational: List[String] = Nil
  ) =
    KafkaConfigValidation
      .validInterview(raw, kafkaEnabled, operational)
      .swap
      .toOption
      .fold(List.empty[ConfigError])(_.toList)

  test("publication polling accepts only the bounded interval and defaults to one second") {
    assertEquals(configuration("").map(_.kafka.interview.publicationPollIntervalMs), Right(1000))
    List(100, 1000, 60000).foreach { interval =>
      assertEquals(
        configuration(s"kafka.interview.publication-poll-interval-ms = $interval")
          .map(_.kafka.interview.publicationPollIntervalMs),
        Right(interval)
      )
    }
    List(Int.MinValue, 0, 99, 60001, Int.MaxValue).foreach { interval =>
      assert(
        errorsOf(s"kafka.interview.publication-poll-interval-ms = $interval")
          .contains(ConfigError.InvalidInterviewPublicationPollInterval),
        clues(interval)
      )
    }
  }

  test("disabled workflow defaults preserve existing operational Kafka configuration") {
    assertEquals(
      KafkaConfigValidation.validInterview(RawInterviewRuntimeConfig()).toOption,
      Some(InterviewRuntimeConfig())
    )
  }

  test("each interview bound names its own setting") {
    List(
      ("max-attempts = 0", ConfigError.InvalidInterviewMaxAttempts),
      ("max-attempts = 101", ConfigError.InvalidInterviewMaxAttempts),
      ("max-attempts = \"many\"", ConfigError.InvalidInterviewMaxAttempts),
      ("retry-base-seconds = 0", ConfigError.InvalidInterviewRetryBase),
      ("retry-base-seconds = 31", ConfigError.InvalidInterviewRetryWindow),
      ("retry-cap-seconds = 301", ConfigError.InvalidInterviewRetryCap),
      ("provider-timeout-seconds = 0", ConfigError.InvalidInterviewProviderTimeout),
      ("provider-timeout-seconds = 2147483647", ConfigError.InvalidInterviewProviderTimeout),
      ("provider-timeout-seconds = 30", ConfigError.InvalidInterviewClaimWindow),
      ("claim-seconds = 3601", ConfigError.InvalidInterviewClaimSeconds),
      ("claim-seconds = 10", ConfigError.InvalidInterviewClaimWindow),
      ("pre-commit-deadline-seconds = 0", ConfigError.InvalidInterviewPreCommitDeadline),
      ("pre-commit-deadline-seconds = 301", ConfigError.InvalidInterviewPreCommitDeadline),
      ("replay-retention-seconds = 604801", ConfigError.InvalidInterviewReplayRetention),
      ("completed-dedup-retention-seconds = 691199", ConfigError.InvalidInterviewCompletedDedupRetention),
      ("completed-dedup-retention-seconds = 31536001", ConfigError.InvalidInterviewCompletedDedupRetention),
      ("publication-batch-size = 0", ConfigError.InvalidInterviewPublicationBatchSize),
      ("publication-batch-size = 65", ConfigError.InvalidInterviewPublicationBatchSize),
      ("clock-skew-tolerance-millis = -1", ConfigError.InvalidInterviewClockSkewTolerance),
      ("clock-skew-tolerance-millis = 60001", ConfigError.InvalidInterviewClockSkewTolerance),
      ("partition-concurrency = 0", ConfigError.InvalidInterviewPartitionConcurrency),
      ("partition-concurrency = 65", ConfigError.InvalidInterviewPartitionConcurrency),
      ("enabled = \"yes\"", ConfigError.InvalidInterviewEnabled)
    ).foreach { case (setting, expected) =>
      assert(errorsOf(s"kafka.interview.$setting").contains(expected), clues(setting))
    }
    List(
      "max-attempts = 100",
      "retry-cap-seconds = 1",
      "claim-seconds = 3600",
      "completed-dedup-retention-seconds = 31536000",
      "clock-skew-tolerance-millis = 0"
    ).foreach(setting => assert(configuration(s"kafka.interview.$setting").isRight, clues(setting)))
  }

  test("independent interview violations are all reported") {
    assertEquals(
      errorsOf("kafka.interview { max-attempts = 0, publication-batch-size = 65 }").toSet,
      Set[ConfigError](ConfigError.InvalidInterviewMaxAttempts, ConfigError.InvalidInterviewPublicationBatchSize)
    )
    // Cross-field rules run once the section decodes, and they accumulate with each other.
    assertEquals(
      errorsOf("kafka.interview { claim-seconds = 10, retry-base-seconds = 31 }").toSet,
      Set[ConfigError](ConfigError.InvalidInterviewClaimWindow, ConfigError.InvalidInterviewRetryWindow)
    )
  }

  test("enabled workflow rejects absent or shared principals and unsafe leases") {
    assertEquals(
      interviewErrors(RawInterviewRuntimeConfig(enabled = true)),
      List.fill(3)(ConfigError.InvalidKafkaCredentials)
    )
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
    assertEquals(
      interviewErrors(configured.copy(workerUsername = configured.orchestratorUsername)),
      List(ConfigError.InvalidInterviewPrincipals)
    )
    assertEquals(interviewErrors(configured.copy(claimSeconds = 10)), List(ConfigError.InvalidInterviewClaimWindow))
    assertEquals(interviewErrors(configured.copy(fencerPassword = None)), List(ConfigError.InvalidKafkaCredentials))
    assertEquals(
      interviewErrors(configured.copy(fencerUsername = configured.workerUsername)),
      List(ConfigError.InvalidInterviewPrincipals)
    )
    assertEquals(
      interviewErrors(configured.copy(fencerUsername = configured.orchestratorUsername)),
      List(ConfigError.InvalidInterviewPrincipals)
    )
    assertEquals(interviewErrors(configured, kafkaEnabled = false), List(ConfigError.InvalidInterviewEnabled))
    assertEquals(
      interviewErrors(configured, operational = List("worker")),
      List(ConfigError.InvalidInterviewPrincipals)
    )
    assertEquals(
      interviewErrors(configured.copy(providerTimeoutSeconds = 30)),
      List(ConfigError.InvalidInterviewClaimWindow)
    )
    assertEquals(
      interviewErrors(configured.copy(providerTimeoutSeconds = 3600)),
      List(ConfigError.InvalidInterviewClaimWindow)
    )
    assertEquals(
      interviewErrors(configured.copy(orchestratorPassword = None, workerPassword = None, claimSeconds = 10)),
      List(
        ConfigError.InvalidKafkaCredentials,
        ConfigError.InvalidKafkaCredentials,
        ConfigError.InvalidInterviewClaimWindow
      )
    )
  }
}
