package com.example.hiring.analytics.adapter.spark

import cats.effect.Sync
import cats.syntax.all.*
import com.example.hiring.analytics.domain.SubjectToken
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.ActiveDeletionMarkerSource
import org.apache.spark.sql.DataFrame
import scala.util.control.NonFatal

final case class DataFrameDeletionMarkerSource[F[_]: Sync](tokens: DataFrame) extends ActiveDeletionMarkerSource[F] {
  override def activeSubjectTokens: F[Vector[SubjectToken]] =
    Sync[F]
      .delay {
        if (!tokens.columns.contains("subjectToken")) Left(AnalyticsError.InvalidSourceSchema(Vector("subjectToken")))
        else
          tokens
            .select("subjectToken")
            .collect()
            .toVector
            .traverse(row => SubjectToken.fromHmac(row.getString(0)).leftMap(AnalyticsError.InvalidConfiguration.apply))
      }
      .flatMap(Sync[F].fromEither)
      .handleErrorWith {
        case error: AnalyticsError => Sync[F].raiseError(error)
        case NonFatal(cause)       => Sync[F].raiseError(AnalyticsError.LakehouseFailure(cause))
      }
}
