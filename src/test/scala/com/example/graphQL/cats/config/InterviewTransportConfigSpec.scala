package com.example.graphQL.cats.config

import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import munit.FunSuite

final class InterviewTransportConfigSpec extends FunSuite {
  private def configuration(overrides: String) = AppConfig.fromConfig(
    "include classpath(\"application.conf\")\nhttp.host=\"127.0.0.1\"\nhttp.port=8080\nmongo.uri=\"mongodb://127.0.0.1:27018\"\nauth.jwt.hs256-secret=\"synthetic-test-signing-key-material\"\n" + overrides,
    Map.empty
  )

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

  test("interview transport rejects shared topics, shared groups and invalid topic syntax") {
    List(
      "kafka.interview.commands-topic=\"hiring.interview-results\"",
      "kafka.interview.worker-group=\"hiring-interview-orchestrator\"",
      "kafka.interview.commands-topic=\"..\"",
      "kafka.interview.results-topic=\"invalid topic\""
    ).foreach(value => assert(configuration(value).isLeft, value))
  }
}
