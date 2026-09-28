package com.example.hiring.analytics.adapter.kafka

import com.example.hiring.analytics.config.KafkaConnection
import cats.syntax.all.*
import com.typesafe.config.{ConfigRenderOptions, ConfigValueFactory}

/** Kafka client and Spark connector options derived from validated connection settings. */
private[analytics] object KafkaClientProperties {
  def clientProperties(connection: KafkaConnection): Map[String, String] = {
    val validated = KafkaConnection
      .validate(connection)
      .toEither
      .fold(
        _ => throw new IllegalArgumentException("Kafka connection settings are invalid"),
        identity
      )
    val transport = Map("security.protocol" -> validated.securityProtocol)
    (validated.saslUsername, validated.saslPassword) match {
      case (Some(username), Some(password)) =>
        transport ++ Map(
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" ->
            s"org.apache.kafka.common.security.plain.PlainLoginModule required username=${quoted(username)} password=${quoted(password)};"
        )
      case (None, None) => transport
      case _            => throw new IllegalArgumentException("Kafka connection settings are invalid")
    }
  }

  def sparkOptions(connection: KafkaConnection): Map[String, String] =
    clientProperties(connection).map { case (key, value) => s"kafka.$key" -> value } ++
      Map("kafka.group.id" -> "hiring-analytics-batch", "kafka.isolation.level" -> "read_committed")

  private def quoted(value: String): String =
    ConfigValueFactory.fromAnyRef(value).render(ConfigRenderOptions.concise())
}
