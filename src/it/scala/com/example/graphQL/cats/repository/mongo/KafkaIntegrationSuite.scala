package com.example.graphQL.cats.repository.mongo

import com.example.hiring.testing.KafkaTestNamespace

/** Kafka scenarios are explicitly skipped without a verified isolated test manifest. */
abstract class KafkaIntegrationSuite extends MongoIntegrationSuite {
  private val namespaceFixture = ResourceTestLocalFixture("hiring-kafka-namespace", KafkaTestNamespace.resource)
  override def munitFixtures: List[munit.AnyFixture[?]] = super.munitFixtures :+ namespaceFixture
  protected def kafkaNamespace: KafkaTestNamespace.Namespace = {
    assume(
      namespaceFixture().nonEmpty,
      "BLOCKED: isolated test service manifest absent; use scripts/run-local-tests.sh"
    )
    namespaceFixture().get
  }
  protected def kafkaEvidenceEnabled: Boolean = sys.env.contains("HIRING_TEST_MANIFEST")
  protected def kafkaEnvironment: Map[String, String] = {
    val namespace = kafkaNamespace
    val config = namespace.manifest
    Map(
      "INTERVIEW_KAFKA_EVIDENCE" -> "true",
      "INTERVIEW_KAFKA_BOOTSTRAP" -> config.kafkaBootstrap,
      "KAFKA_BROKER_PASSWORD" -> config.brokerPassword,
      "KAFKA_PUBLISHER_V2_PASSWORD" -> config.publisherPassword,
      "KAFKA_READER_PASSWORD" -> config.readerPassword,
      "KAFKA_INTERVIEW_ORCHESTRATOR_PASSWORD" -> config.orchestratorPassword,
      "KAFKA_INTERVIEW_WORKER_PASSWORD" -> config.workerPassword,
      "KAFKA_INTERVIEW_FENCER_PASSWORD" -> config.interviewFencerPassword
    )
  }
  protected def interviewTopics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair =
    com.example.graphQL.cats.domain.workflow.InterviewTopicPair(kafkaNamespace.commands, kafkaNamespace.results)
}
