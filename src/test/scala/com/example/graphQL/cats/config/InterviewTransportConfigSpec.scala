package com.example.graphQL.cats.config

import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import munit.FunSuite

final class InterviewTransportConfigSpec extends FunSuite {
  private def configuration(overrides: String) = AppConfigFixtures.withPackagedDefaults(overrides)
  private def errorsOf(overrides: String): List[ConfigError] =
    configuration(overrides).swap.toOption.fold(fail(s"Expected rejected configuration for $overrides"))(_.toList)

  test("interview transport retains the existing production topics and groups") {
    val result = configuration("").fold(errors => fail(errors.toString), identity)
    assertEquals(result.kafka.interview.topics, InterviewTopicPair.Default)
    assertEquals(result.kafka.interview.workerGroup, "hiring-interview-workers")
    assertEquals(result.kafka.interview.orchestratorGroup, "hiring-interview-orchestrator")
  }

  test("interview transport reads an isolated physical topic pair and consumer groups") {
    val result = configuration("""kafka.interview {
      commands-topic = "hiring.test.commands.owned"
      results-topic = "hiring.test.results.owned"
      worker-group = "hiring.test.workers.owned"
      orchestrator-group = "hiring.test.orchestrator.owned"
    }""").fold(errors => fail(errors.toString), identity)
    assertEquals(
      result.kafka.interview.topics,
      InterviewTopicPair("hiring.test.commands.owned", "hiring.test.results.owned")
    )
    assertEquals(result.kafka.interview.workerGroup, "hiring.test.workers.owned")
  }

  test("interview transport names each rejected topic, group or shared pair") {
    List(
      ("kafka.interview.commands-topic=\"hiring.interview-results\"", ConfigError.InvalidInterviewTopicPair),
      ("kafka.interview.worker-group=\"hiring-interview-orchestrator\"", ConfigError.InvalidInterviewGroupPair),
      ("kafka.interview.commands-topic=\"..\"", ConfigError.InvalidInterviewCommandsTopic),
      ("kafka.interview.commands-topic=\".\"", ConfigError.InvalidInterviewCommandsTopic),
      ("kafka.interview.results-topic=\"invalid topic\"", ConfigError.InvalidInterviewResultsTopic),
      ("kafka.interview.results-topic=\"" + "t" * 250 + "\"", ConfigError.InvalidInterviewResultsTopic),
      ("kafka.interview.worker-group=\"bad group\"", ConfigError.InvalidInterviewWorkerGroup),
      ("kafka.interview.orchestrator-group=\"" + "g" * 201 + "\"", ConfigError.InvalidInterviewOrchestratorGroup)
    ).foreach { case (override_, expected) => assert(errorsOf(override_).contains(expected), clues(override_)) }
  }

  test("independent interview transport violations are all reported") {
    assertEquals(
      errorsOf("kafka.interview.commands-topic=\"..\"\nkafka.interview.worker-group=\"bad group\"").toSet,
      Set[ConfigError](ConfigError.InvalidInterviewCommandsTopic, ConfigError.InvalidInterviewWorkerGroup)
    )
  }
}
