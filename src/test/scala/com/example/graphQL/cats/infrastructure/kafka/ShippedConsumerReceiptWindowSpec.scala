package com.example.graphQL.cats.infrastructure.kafka

import com.example.graphQL.cats.domain.policy.ConsumerReceiptWindow
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import munit.FunSuite
import scala.concurrent.duration.*

/** DDC-02.5: the shipped defaults satisfy the rule the consumer enforces at start. */
final class ShippedConsumerReceiptWindowSpec extends FunSuite {
  private def shipped(path: String): String = Files.readString(Path.of(path))

  private def scriptDefault(script: String, variable: String): Long =
    s"""(?m)^\\w+="\\$$\\{$variable:-(\\d+)\\}"""".r
      .findFirstMatchIn(script)
      .map(_.group(1).toLong)
      .getOrElse(fail(s"$variable default not found in provision-access.sh"))

  test("DDC-02.5 application.conf, provision-access.sh and analytics config satisfy ConsumerReceiptWindow") {
    val receiptTtl = ConfigFactory
      .parseString(shipped("src/main/resources/application.conf"))
      .resolve()
      .getInt("kafka.consumer.receipt-ttl-days")
      .days
    val script = shipped("docker/kafka/provision-access.sh")
    val retentionMs = scriptDefault(script, "KAFKA_TOPIC_RETENTION_MS")
    val segmentMs = scriptDefault(script, "KAFKA_TOPIC_SEGMENT_MS")

    assertEquals(ConsumerReceiptWindow.check(receiptTtl, retentionMs, segmentMs), Right(()))

    val analytics = ConfigFactory.parseString(shipped("analytics/src/main/resources/application.conf")).resolve()
    val retention = analytics.getConfig("analytics.operational.retention")
    assertEquals(retention.getDuration("kafka-retention").toMillis, retentionMs)
    assertEquals(retention.getDuration("kafka-segment-roll").toMillis, segmentMs)
  }

  test("DDC-02.5 the provisioning script grants DescribeConfigs on the events topic only") {
    // The kafka-acls.sh spelling is DescribeConfigs; the tool refuses DESCRIBE_CONFIGS (observed on apache/kafka:3.9.2).
    val script = shipped("docker/kafka/provision-access.sh")
    val commands = script.linesIterator.filterNot(_.trim.startsWith("#")).toList
    // Any spelling of the operation, whatever its case or underscore.
    val describeConfigs = commands.filter(_.toLowerCase.replace("_", "").contains("describeconfigs"))
    assertEquals(
      describeConfigs,
      List("""acls --add --allow-principal User:analytics_reader --operation DescribeConfigs --topic "$topic"""")
    )
    assert(
      !commands.exists(_.contains("DESCRIBE_CONFIGS")),
      "kafka-acls.sh rejects the DESCRIBE_CONFIGS spelling"
    )
  }
}
