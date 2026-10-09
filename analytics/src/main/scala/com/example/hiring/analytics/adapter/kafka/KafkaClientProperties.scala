package com.example.hiring.analytics.adapter.kafka

import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.errors.AnalyticsError

import org.apache.kafka.common.serialization.ByteArrayDeserializer

/** Kafka client and Spark connector options derived from validated connection settings. */
private[analytics] object KafkaClientProperties {
  val ClientTimeoutMillis: Int = 10000

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
                  s"org.apache.kafka.common.security.plain.PlainLoginModule required username=${jaasLiteral(username)} password=${jaasLiteral(password)};"
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

  /** Admin client settings; both timeouts match the application build's producer fencer. */
  def adminProperties(connection: KafkaConnection): Either[AnalyticsError, Map[String, String]] =
    clientProperties(connection).map(
      Map(
        "bootstrap.servers" -> connection.bootstrapServers,
        "request.timeout.ms" -> ClientTimeoutMillis.toString,
        "default.api.timeout.ms" -> ClientTimeoutMillis.toString
      ) ++ _
    )

  /** Offset-inspection consumer settings; the consumer never subscribes, commits or deserializes record contents. */
  def retentionConsumerProperties(connection: KafkaConnection): Either[AnalyticsError, Map[String, String]] =
    clientProperties(connection).map(
      Map(
        "bootstrap.servers" -> connection.bootstrapServers,
        "group.id" -> "hiring-analytics-erasure",
        "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
        "value.deserializer" -> classOf[ByteArrayDeserializer].getName,
        "enable.auto.commit" -> "false",
        "default.api.timeout.ms" -> ClientTimeoutMillis.toString
      ) ++ _
    )

  /** A quoted JAAS string literal: escapes backslash, double quote and the control characters the parser unescapes. */
  private[analytics] def jaasLiteral(value: String): String =
    value
      .flatMap {
        case '\\'  => "\\\\"
        case '"'   => "\\\""
        case '\n'  => "\\n"
        case '\r'  => "\\r"
        case '\t'  => "\\t"
        case '\b'  => "\\b"
        case '\f'  => "\\f"
        case other => other.toString
      }
      .mkString("\"", "", "\"")
}
