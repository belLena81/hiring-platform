package com.example.hiring.analytics.domain

import cats.data.{Validated, ValidatedNec}
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.Match

opaque type AnalyticsReplayRequestId = String :| Match["[A-Za-z0-9][A-Za-z0-9._-]{0,127}"]

object AnalyticsReplayRequestId {
  def from(value: String): Either[String, AnalyticsReplayRequestId] =
    value
      .refineEither[Match["[A-Za-z0-9][A-Za-z0-9._-]{0,127}"]]
      .leftMap(_ => "replay request ID must contain 1 to 128 ASCII letters, digits, dots, underscores, or dashes")

  extension (value: AnalyticsReplayRequestId) def value: String = value
}

final case class AnalyticsReplayCoordinate private (
    topic: AnalyticsTopic,
    partition: AnalyticsPartition,
    offset: AnalyticsOffset
)

object AnalyticsReplayCoordinate {
  private def validKafkaTopicName(value: String): Boolean =
    value.length <= 249 && value != "." && value != ".." && value.matches("[A-Za-z0-9._-]+")

  def from(topic: String, partition: Int, offset: Long): ValidatedNec[String, AnalyticsReplayCoordinate] =
    (
      AnalyticsTopic
        .from(topic)
        .flatMap(validated =>
          Either.cond(validKafkaTopicName(topic), validated, "replay topic must be a valid Kafka topic name")
        )
        .toValidatedNec,
      AnalyticsPartition.from(partition).toValidatedNec,
      AnalyticsOffset.from(offset).toValidatedNec
    ).mapN(AnalyticsReplayCoordinate.apply)
}

/** Immutable request identity and explicit coordinate selection for late-fact replay. */
final case class AnalyticsLateFactReplayRequest private (
    requestId: AnalyticsReplayRequestId,
    coordinates: Vector[AnalyticsReplayCoordinate],
    selectionDigest: RangeFingerprint
)

object AnalyticsLateFactReplayRequest {
  val MaximumCoordinates: Int = 1000

  def from(
      requestId: String,
      coordinates: Vector[(String, Int, Long)],
      configuredMaximum: Int = MaximumCoordinates
  ): ValidatedNec[String, AnalyticsLateFactReplayRequest] =
    build(AnalyticsReplayRequestId.from(requestId).toValidatedNec, coordinates, configuredMaximum)

  @scala.annotation.targetName("fromRequestId")
  def from(
      requestId: AnalyticsReplayRequestId,
      coordinates: Vector[(String, Int, Long)],
      configuredMaximum: Int
  ): ValidatedNec[String, AnalyticsLateFactReplayRequest] =
    build(requestId.validNec, coordinates, configuredMaximum)

  private def build(
      validId: ValidatedNec[String, AnalyticsReplayRequestId],
      coordinates: Vector[(String, Int, Long)],
      configuredMaximum: Int
  ): ValidatedNec[String, AnalyticsLateFactReplayRequest] = {
    val validCoordinates = coordinates.traverse { case (topic, partition, offset) =>
      AnalyticsReplayCoordinate.from(topic, partition, offset)
    }
    val nonEmpty =
      Validated.condNec(coordinates.nonEmpty, (), "at least one Kafka coordinate is required")
    val configuredMaximumIsValid = configuredMaximum >= 1 && configuredMaximum <= MaximumCoordinates
    val validLimit =
      Validated.condNec(
        configuredMaximumIsValid,
        (),
        s"replay coordinate limit must be between 1 and $MaximumCoordinates"
      )
    val withinLimit =
      Validated.condNec(
        !configuredMaximumIsValid || coordinates.size <= configuredMaximum,
        (),
        s"replay request exceeds its configured limit of $configuredMaximum coordinates"
      )

    (validId, validCoordinates, nonEmpty, validLimit, withinLimit)
      .mapN { (id, selected, _, _, _) =>
        val duplicates = selected.groupBy(identity).collect { case (_, copies) if copies.size > 1 => copies.head }
        val canonical = "analytics-late-fact-replay-selection-v1;" + selected
          .sortBy(coordinate =>
            (
              AnalyticsTopic.unwrap(coordinate.topic),
              AnalyticsPartition.unwrap(coordinate.partition),
              AnalyticsOffset.unwrap(coordinate.offset)
            )
          )
          .map { coordinate =>
            val topicBytes = AnalyticsTopic.unwrap(coordinate.topic).getBytes(java.nio.charset.StandardCharsets.UTF_8)
            s"${topicBytes.length}:" + new String(topicBytes, java.nio.charset.StandardCharsets.UTF_8) +
              s";${AnalyticsPartition.unwrap(coordinate.partition)};${AnalyticsOffset.unwrap(coordinate.offset)};"
          }
          .mkString
        (id, selected, duplicates, canonical)
      }
      .andThen { case (id, selected, duplicates, canonical) =>
        if (duplicates.nonEmpty) "replay Kafka coordinates must be unique".invalidNec
        else
          AnalyticsLateFactReplayRequest(
            id,
            selected,
            RangeFingerprint.ofSha256(canonical)
          ).validNec
      }
  }
}
