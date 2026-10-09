package com.example.hiring.analytics.adapter.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_contains, col}

/** Shared subject-token semi-join used by erasure counting, evidence capture, and verification. */
private[analytics] object BatchSubjectMatching {
  def markerRows(frame: DataFrame): DataFrame =
    frame.select(col(Columns.SubjectToken)).filter(col(Columns.SubjectToken).isNotNull).distinct()

  /** Match array attribution first, then scalar attribution. Unattributed datasets match every row. */
  def matchedBySubject(frame: DataFrame, marker: DataFrame): DataFrame = {
    val columns = frame.columns.toSet
    if (columns.contains(Columns.SubjectTokens))
      frame
        .as("stored")
        .join(
          marker.as("marker"),
          array_contains(col(s"stored.${Columns.SubjectTokens}"), col(s"marker.${Columns.SubjectToken}")),
          "left_semi"
        )
    else if (columns.contains(Columns.SubjectToken))
      frame
        .as("stored")
        .join(
          marker.as("marker"),
          col(s"stored.${Columns.SubjectToken}") === col(s"marker.${Columns.SubjectToken}"),
          "left_semi"
        )
    else frame
  }
}
