package com.example.hiring.analytics.adapter.spark

import cats.Applicative
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class DataFrameDeletionMarkerSource[F[_]: Applicative](tokens: DataFrame)
    extends ActiveDeletionMarkerSource[F] {
  override def activeSubjectTokens(spark: SparkSession): F[DataFrame] = Applicative[F].pure(tokens)
}
