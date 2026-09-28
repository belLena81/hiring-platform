package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.domain.SubjectPseudonymizer

import cats.effect.IO
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import java.time.Instant

private[spark] trait AnalyticsBatchBlocking {
  def apply[A](work: => A): IO[A]
  def either[A](work: => Either[AnalyticsError, A]): IO[A]
}

private[spark] final case class AnalyticsBatchStageRuntime(
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    blocking: AnalyticsBatchBlocking,
    currentTime: () => IO[Instant],
    persistManifest: (SparkSession, AnalyticsRunManifest, String, String) => IO[Unit],
    merge: (DataFrame, String, String) => IO[Unit],
    readOrEmpty: (SparkSession, String, StructType) => IO[DataFrame],
    withExpiry: (DataFrame, Instant, Int) => DataFrame,
    quarantineId: () => Column,
    configureRawTablePrivacy: SparkSession => IO[Unit],
    maximumErasureEvidenceFiles: Int,
    retirementAuthorizations: String => IO[Vector[HmacKeyRetirementAuthorization]]
)

private[spark] final case class AnalyticsBronzeInput(frame: DataFrame, startedAt: Instant, records: Long)

private[spark] final case class AnalyticsPreparedEvents(
    incomingSilver: DataFrame,
    conflicts: DataFrame,
    validRecords: Long,
    suppressedRecords: Long,
    quarantinedRecords: Long,
    conflictingEventIds: Long
)
