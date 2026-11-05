package com.example.graphQL.cats.config

import com.example.graphQL.cats.service.port.ProducerWorkerId
import munit.FunSuite

class KafkaConsumerTuningConfigSpec extends FunSuite {
  private def configuration(overrides: String, env: Map[String, String] = Map.empty) =
    AppConfigFixtures.withPackagedDefaults(overrides, env)

  private def errorsOf(overrides: String): List[ConfigError] =
    configuration(overrides).swap.toOption.fold(fail(s"Expected rejected configuration for $overrides"))(_.toList)

  test("consumer and interview poll and commit settings default to 100, 300 s, 100 and 5000 ms") {
    val kafka = configuration("").toOption.get.kafka
    assertEquals(
      (
        kafka.consumer.maxPollRecords,
        kafka.consumer.maxPollIntervalSeconds,
        kafka.consumer.commitBatchSize,
        kafka.consumer.commitBatchIntervalMillis
      ),
      (100, 300, 100, 5000)
    )
    assertEquals(
      (
        kafka.interview.maxPollRecords,
        kafka.interview.maxPollIntervalSeconds,
        kafka.interview.commitBatchSize,
        kafka.interview.commitBatchIntervalMillis
      ),
      (100, 300, 100, 5000)
    )
  }

  test("environment variables override the consumer and interview poll and commit settings") {
    val kafka = configuration(
      "",
      Map(
        "KAFKA_CONSUMER_MAX_POLL_RECORDS" -> "50",
        "KAFKA_CONSUMER_MAX_POLL_INTERVAL_SECONDS" -> "120",
        "KAFKA_CONSUMER_COMMIT_BATCH_SIZE" -> "25",
        "KAFKA_CONSUMER_COMMIT_BATCH_INTERVAL_MS" -> "1000",
        "INTERVIEW_MAX_POLL_RECORDS" -> "40",
        "INTERVIEW_MAX_POLL_INTERVAL_SECONDS" -> "90",
        "INTERVIEW_COMMIT_BATCH_SIZE" -> "20",
        "INTERVIEW_COMMIT_BATCH_INTERVAL_MS" -> "2000"
      )
    ).toOption.get.kafka
    assertEquals(
      (
        kafka.consumer.maxPollRecords,
        kafka.consumer.maxPollIntervalSeconds,
        kafka.consumer.commitBatchSize,
        kafka.consumer.commitBatchIntervalMillis
      ),
      (50, 120, 25, 1000)
    )
    assertEquals(
      (
        kafka.interview.maxPollRecords,
        kafka.interview.maxPollIntervalSeconds,
        kafka.interview.commitBatchSize,
        kafka.interview.commitBatchIntervalMillis
      ),
      (40, 90, 20, 2000)
    )
  }

  test("every poll and commit setting must be at least one") {
    List(
      "kafka.consumer.max-poll-records" -> ConfigError.InvalidKafkaMaxPollRecords,
      "kafka.consumer.max-poll-interval-seconds" -> ConfigError.InvalidKafkaMaxPollInterval,
      "kafka.consumer.commit-batch-size" -> ConfigError.InvalidKafkaCommitBatchSize,
      "kafka.consumer.commit-batch-interval-millis" -> ConfigError.InvalidKafkaCommitBatchInterval,
      "kafka.interview.max-poll-records" -> ConfigError.InvalidInterviewMaxPollRecords,
      "kafka.interview.max-poll-interval-seconds" -> ConfigError.InvalidInterviewMaxPollInterval,
      "kafka.interview.commit-batch-size" -> ConfigError.InvalidInterviewCommitBatchSize,
      "kafka.interview.commit-batch-interval-millis" -> ConfigError.InvalidInterviewCommitBatchInterval
    ).foreach { case (path, expected) =>
      List(0, -1).foreach(value => assert(errorsOf(s"$path = $value").contains(expected), clues(path, value)))
    }
  }

  test("KFH-08.1 poll counts are capped at 10000 and the poll interval at 3600 s, inclusive") {
    List("consumer", "interview").foreach { section =>
      val (recordsError, intervalError, batchError) =
        if (section == "consumer")
          (
            ConfigError.InvalidKafkaMaxPollRecords,
            ConfigError.InvalidKafkaMaxPollInterval,
            ConfigError.InvalidKafkaCommitBatchSize
          )
        else
          (
            ConfigError.InvalidInterviewMaxPollRecords,
            ConfigError.InvalidInterviewMaxPollInterval,
            ConfigError.InvalidInterviewCommitBatchSize
          )
      val p = s"kafka.$section"
      assert(configuration(s"$p.max-poll-records = 10000\n$p.commit-batch-size = 10000").isRight, section)
      assert(errorsOf(s"$p.max-poll-records = 10001").contains(recordsError), section)
      assert(
        errorsOf(s"$p.max-poll-records = 10000\n$p.commit-batch-size = 10001").contains(batchError),
        section
      )
      assert(configuration(s"$p.max-poll-interval-seconds = 3600").isRight, section)
      assert(errorsOf(s"$p.max-poll-interval-seconds = 3601").contains(intervalError), section)
    }
  }

  test("a commit batch larger than one poll is rejected") {
    assert(
      errorsOf("kafka.consumer.max-poll-records = 10\nkafka.consumer.commit-batch-size = 11")
        .contains(ConfigError.InvalidKafkaCommitBatchSize)
    )
    assert(
      errorsOf("kafka.interview.max-poll-records = 10\nkafka.interview.commit-batch-size = 11")
        .contains(ConfigError.InvalidInterviewCommitBatchSize)
    )
    assert(configuration("kafka.consumer.max-poll-records = 10\nkafka.consumer.commit-batch-size = 10").isRight)
  }

  test("a commit flush interval must be shorter than the poll interval") {
    assert(
      errorsOf("kafka.consumer.max-poll-interval-seconds = 5\nkafka.consumer.commit-batch-interval-millis = 5000")
        .contains(ConfigError.InvalidKafkaCommitBatchWindow)
    )
    assert(
      errorsOf("kafka.interview.max-poll-interval-seconds = 5\nkafka.interview.commit-batch-interval-millis = 5000")
        .contains(ConfigError.InvalidInterviewCommitBatchWindow)
    )
    assert(
      configuration(
        "kafka.consumer.max-poll-interval-seconds = 6\nkafka.consumer.commit-batch-interval-millis = 5000"
      ).isRight
    )
  }

  test("the SASL mechanism defaults to SCRAM-SHA-512 and is overridable by environment") {
    assertEquals(configuration("").map(_.kafka.saslMechanism), Right(KafkaSaslMechanism.ScramSha512))
    assertEquals(
      configuration("", Map("KAFKA_SASL_MECHANISM" -> "PLAIN")).map(_.kafka.saslMechanism),
      Right(KafkaSaslMechanism.Plain)
    )
  }

  test("PLAIN is accepted only for loopback bootstrap servers, SCRAM everywhere") {
    val remote = "kafka.bootstrap-servers = \"broker.example:9092\"\n"
    assert(errorsOf(remote + "kafka.sasl-mechanism = \"PLAIN\"").contains(ConfigError.InvalidKafkaSaslMechanism))
    assert(configuration(remote + "kafka.sasl-mechanism = \"SCRAM-SHA-512\"").isRight)
    assert(configuration("kafka.sasl-mechanism = \"PLAIN\"").isRight)
  }

  test("an unknown SASL mechanism is rejected with its own error") {
    assert(errorsOf("kafka.sasl-mechanism = \"GSSAPI\"").contains(ConfigError.InvalidKafkaSaslMechanism))
  }

  test("worker ids default to local names and are overridable by environment") {
    val kafka = configuration("").toOption.get.kafka
    assertEquals(kafka.publisher.workerId, ProducerWorkerId.parse("local-publisher").toOption.get)
    assertEquals(kafka.interview.workerId, ProducerWorkerId.parse("interview-local").toOption.get)
    val overridden =
      configuration("", Map("KAFKA_WORKER_ID" -> "pub-2", "INTERVIEW_WORKER_ID" -> "iv-2")).toOption.get.kafka
    assertEquals(overridden.publisher.workerId.value, "pub-2")
    assertEquals(overridden.interview.workerId.value, "iv-2")
  }

  test("KFH-04.4 a non-canonical worker id fails configuration") {
    List("Upper", "-leading", "has_underscore", "has space", "", "a" * 65).foreach { id =>
      assert(
        errorsOf(s"kafka.publisher.worker-id = \"$id\"").contains(ConfigError.InvalidKafkaWorkerId),
        clues(id)
      )
      assert(
        errorsOf(s"kafka.interview.worker-id = \"$id\"").contains(ConfigError.InvalidInterviewWorkerId),
        clues(id)
      )
    }
    assert(configuration("kafka.publisher.worker-id = \"" + "a" * 64 + "\"").isRight)
  }

  test("KFH-04.4 a non-canonical KAFKA_WORKER_ID override fails configuration") {
    val errors = configuration("", Map("KAFKA_WORKER_ID" -> "Bad_Id!")).swap.toOption
      .fold(fail("Expected rejected configuration"))(_.toList)
    assert(errors.contains(ConfigError.InvalidKafkaWorkerId), clues(errors))
  }

  test("KFH-04.4 a non-canonical INTERVIEW_WORKER_ID override fails configuration") {
    val errors = configuration("", Map("INTERVIEW_WORKER_ID" -> "Bad_Id!")).swap.toOption
      .fold(fail("Expected rejected configuration"))(_.toList)
    assert(errors.contains(ConfigError.InvalidInterviewWorkerId), clues(errors))
  }

  test("rendered consumer settings stay redacted with the new tuning fields") {
    val consumer = KafkaConsumerConfig(true, 7, 14, Some("synthetic-user"), Some("synthetic-secret"))
    assert(!consumer.toString.contains("synthetic-secret"))
    assert(!consumer.toString.contains("synthetic-user"))
  }
}
