package com.example.hiring.analytics.config

import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank
import pureconfig.*
import pureconfig.error.UserValidationFailed

/** Shared HOCON input records used by the analytics batch/worker and the key-retirement audit. */
private[analytics] object AnalyticsRawConfig {
  given ConfigReader[AnalyticsNonBlank] =
    ConfigReader[String].emap(value =>
      value.refineEither[Not[Blank]].leftMap(_ => UserValidationFailed("must be non-empty"))
    )

  given ConfigReader[Option[AnalyticsNonBlank]] with ReadsMissingKeys {
    override def from(cursor: ConfigCursor): ConfigReader.Result[Option[AnalyticsNonBlank]] =
      if (cursor.isUndefined || cursor.isNull) Right(None)
      else
        ConfigReader[String].from(cursor).flatMap { value =>
          if (value.trim.isEmpty) Right(None)
          else ConfigReader[AnalyticsNonBlank].from(cursor).map(Some(_))
        }
  }

  given ConfigReader[Int] = ConfigReader.fromCursor { cursor =>
    cursor.asInt match {
      case success @ Right(_) => success
      case Left(_)            =>
        cursor.asString.flatMap(value =>
          cursor.scopeFailure(value.toIntOption.toRight(UserValidationFailed("must be an integer")))
        )
    }
  }
  given ConfigReader[Long] = ConfigReader.fromCursor { cursor =>
    cursor.asLong match {
      case success @ Right(_) => success
      case Left(_)            =>
        cursor.asString.flatMap(value =>
          cursor.scopeFailure(value.toLongOption.toRight(UserValidationFailed("must be an integer")))
        )
    }
  }
  given ConfigReader[Boolean] = ConfigReader.fromCursor { cursor =>
    cursor.asString match {
      case Right(value) =>
        cursor.scopeFailure(
          value.toLowerCase match {
            case "true"  => Right(true)
            case "false" => Right(false)
            case _       => Left(UserValidationFailed("must be true or false"))
          }
        )
      case Left(_) => cursor.asBoolean
    }
  }

  final case class Mongo(uri: Option[AnalyticsNonBlank], database: Option[AnalyticsNonBlank]) derives ConfigReader
  final case class Spark(master: Option[AnalyticsNonBlank]) derives ConfigReader
  final case class Lakehouse(root: Option[AnalyticsNonBlank]) derives ConfigReader
}
