package com.example.hiring.analytics.adapter.kafka

import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.errors.AnalyticsError

import com.typesafe.config.{ConfigRenderOptions, ConfigValueFactory}

/** Kafka client and Spark connector options derived from validated connection settings. */
private[analytics] object KafkaClientProperties {
  def clientProperties(connection: KafkaConnection): Either[AnalyticsError, Map[String, String]] =
    KafkaConnection
      .validate(connection)
      .toEither
      .left
      .map(_ => AnalyticsError.InvalidConfiguration("Kafka connection settings are invalid"))
      .flatMap { validated =>
        val transport = Map("security.protocol" -> validated.securityProtocol.kafkaValue)
        (validated.saslUsername, validated.saslPassword) match {
          case (Some(username), Some(password)) =>
            Right(
              transport ++ Map(
                "sasl.mechanism" -> "PLAIN",
                "sasl.jaas.config" ->
                  s"org.apache.kafka.common.security.plain.PlainLoginModule required username=${quoted(username)} password=${quoted(password)};"
              )
            )
          case (None, None) => Right(transport)
          case _            => Left(AnalyticsError.InvalidConfiguration("Kafka connection settings are invalid"))
        }
      }

  def sparkOptions(connection: KafkaConnection): Either[AnalyticsError, Map[String, String]] =
    clientProperties(connection).map(properties =>
      properties.map { case (key, value) => s"kafka.$key" -> value } ++
        Map("kafka.group.id" -> "hiring-analytics-batch", "kafka.isolation.level" -> "read_committed")
    )

  private def quoted(value: String): String =
    ConfigValueFactory.fromAnyRef(value).render(ConfigRenderOptions.concise())
}
