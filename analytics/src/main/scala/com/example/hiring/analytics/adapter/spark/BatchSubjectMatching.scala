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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_contains, col}

/** Shared subject-token semi-join used by erasure counting, evidence capture, and verification. */
private[analytics] object BatchSubjectMatching {
  def markerRows(frame: DataFrame): DataFrame =
    frame.select(col("subjectToken")).filter(col("subjectToken").isNotNull).distinct()

  /** Match array attribution first, then scalar attribution. Unattributed datasets match every row. */
  def matchedBySubject(frame: DataFrame, marker: DataFrame): DataFrame = {
    val columns = frame.columns.toSet
    if (columns.contains("subjectTokens"))
      frame
        .as("stored")
        .join(
          marker.as("marker"),
          array_contains(col("stored.subjectTokens"), col("marker.subjectToken")),
          "left_semi"
        )
    else if (columns.contains("subjectToken"))
      frame
        .as("stored")
        .join(marker.as("marker"), col("stored.subjectToken") === col("marker.subjectToken"), "left_semi")
    else frame
  }
}
