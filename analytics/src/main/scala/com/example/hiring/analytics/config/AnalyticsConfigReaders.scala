package com.example.hiring.analytics.config

import cats.syntax.all.*
import com.example.hiring.analytics.domain.AnalyticsTopic
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.numeric.Interval
import io.github.iltotore.iron.constraint.string.Blank
import pureconfig.*
import pureconfig.error.UserValidationFailed

/** Shared HOCON input records used by the analytics batch/worker and the key-retirement audit. */
private[analytics] object AnalyticsConfigReaders {
  private val NonBlankReader =
    ConfigReader[String].emap(value =>
      value.refineEither[Not[Blank]].leftMap(_ => UserValidationFailed("must be non-empty"))
    )

  given ConfigReader[AnalyticsNonBlank] = NonBlankReader

  given ConfigReader[AnalyticsTopic] =
    ConfigReader[String].emap(value =>
      AnalyticsTopic.from(value).leftMap(_ => UserValidationFailed("must be non-empty"))
    )

  given ConfigReader[MaximumErasureEvidenceFiles] =
    ConfigReader[Int].emap(value =>
      value
        .refineEither[Interval.Closed[1, 2147483646]]
        .leftMap(_ => UserValidationFailed("must be between one and 2147483646"))
    )

  given ConfigReader[MongoPublisherBufferSize] =
    ConfigReader[Int].emap(value =>
      value.refineEither[Interval.Closed[1, 65536]].leftMap(_ => UserValidationFailed("must be between one and 65536"))
    )

  given ConfigReader[Option[AnalyticsNonBlank]] with ReadsMissingKeys {
    override def from(cursor: ConfigCursor): ConfigReader.Result[Option[AnalyticsNonBlank]] =
      if (cursor.isUndefined || cursor.isNull) Right(None)
      else
        ConfigReader[String].from(cursor).flatMap { value =>
          if (value.trim.isEmpty) Right(None)
          else NonBlankReader.from(cursor).map(Some(_))
        }
  }

  final case class Mongo(uri: AnalyticsNonBlank, database: AnalyticsNonBlank) derives ConfigReader
  final case class Spark(master: AnalyticsNonBlank) derives ConfigReader
  final case class Lakehouse(root: AnalyticsNonBlank) derives ConfigReader
}
