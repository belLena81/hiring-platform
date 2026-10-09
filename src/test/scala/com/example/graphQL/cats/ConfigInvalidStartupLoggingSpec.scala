package com.example.graphQL.cats

import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import com.example.graphQL.cats.config.ConfigError
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import munit.CatsEffectSuite

class ConfigInvalidStartupLoggingSpec extends CatsEffectSuite {
  private def recording(sink: Ref[IO, Vector[(LogEvent, Map[LogField, String])]]): Diagnostics =
    new Diagnostics {
      def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
        sink.update(_ :+ (event, fields))
    }

  test("startup logs one CONFIG_INVALID event per distinct public key and never a value") {
    val errors = NonEmptyList.of(
      ConfigError.InvalidHost,
      ConfigError.InvalidKafkaCredentials,
      ConfigError.InvalidKafkaCredentials,
      ConfigError.InvalidInterviewClaimWindow
    )
    for {
      sink <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      _ <- Main.configInvalidEvents(recording(sink), errors)
      emitted <- sink.get
    } yield {
      assertEquals(emitted.map(_._1), Vector.fill(3)(LogEvent.ConfigInvalid))
      assertEquals(
        emitted.map(_._2),
        Vector("HTTP_HOST", "KAFKA_CREDENTIALS", "INTERVIEW_CLAIM_WINDOW").map(key => Map(LogField.ConfigKey -> key))
      )
      emitted.flatMap(_._2).foreach { case (field, value) =>
        assert(LogFields.validPublic(field, value), clues(field, value))
      }
    }
  }
}
