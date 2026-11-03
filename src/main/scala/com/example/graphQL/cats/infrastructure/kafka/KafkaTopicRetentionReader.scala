package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import com.example.graphQL.cats.config.KafkaConfig
import fs2.kafka.*
import org.apache.kafka.common.config.ConfigResource

/** The effective retention and segment roll of one topic, in milliseconds as the broker reports them. */
final case class TopicRetention(retentionMs: Long, segmentMs: Long)

/** The topic configuration could not be read: ACL denied, broker unreachable, or an entry is missing or not a number.
  * The message names only the topic, never client messages that can embed configuration values.
  */
final case class TopicConfigUnreadable(topic: String, cause: Option[Throwable] = None)
    extends RuntimeException(s"topic configuration unreadable: $topic", cause.orNull)

/** Reads `retention.ms` and `segment.ms` of one topic through the Kafka admin client (DESCRIBE_CONFIGS on that topic).
  */
object KafkaTopicRetentionReader {
  private val RetentionKey = "retention.ms"
  private val SegmentKey = "segment.ms"

  def read(config: KafkaConfig): IO[TopicRetention] = {
    val settings = KafkaClientSettings.admin(
      config.bootstrapServers,
      config.consumer.saslUsername,
      config.consumer.saslPassword,
      config.saslSecurityProtocol
    )
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, config.topic)
    KafkaAdminClient
      .resource[IO](settings)
      .use(_.describeConfigs(List(resource)))
      .adaptError { case error => TopicConfigUnreadable(config.topic, Some(error)) }
      .flatMap { described =>
        val entries = described.getOrElse(resource, Nil).map(entry => entry.name -> entry.value).toMap
        IO.fromEither(parse(config.topic, entries))
      }
  }

  /** Pure interpretation of the reported entries. */
  private[kafka] def parse(topic: String, entries: Map[String, String]): Either[TopicConfigUnreadable, TopicRetention] =
    for {
      retention <- long(topic, entries, RetentionKey)
      segment <- long(topic, entries, SegmentKey)
    } yield TopicRetention(retention, segment)

  private def long(topic: String, entries: Map[String, String], key: String): Either[TopicConfigUnreadable, Long] =
    entries.get(key).flatMap(_.toLongOption).toRight(TopicConfigUnreadable(topic))
}
