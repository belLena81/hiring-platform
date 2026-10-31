package com.example.hiring.analytics.adapter.spark

import java.util.Locale

/** Which columns Delta may gather data-skipping statistics on. Statistics are min/max/null-count values copied into the
  * `_delta_log` add actions, and that log outlives the data rows, so a subject token or raw identifier column must
  * never be indexed. Subject-attributed tables declare an explicit allow-list and the stored table must carry exactly
  * that list and no `delta.dataSkippingNumIndexedCols`, which would otherwise index the leading columns by position.
  */
private[analytics] object DeltaStatsPolicy {
  val StatsColumnsProperty = "delta.dataSkippingStatsColumns"
  val NumIndexedColumnsProperty = "delta.dataSkippingNumIndexedCols"

  /** Subject pseudonyms and the raw identifiers of the people and jobs an event is about. */
  val ForbiddenColumns: Set[String] =
    Set(Columns.SubjectToken, Columns.SubjectTokens, Columns.AggregateId, Columns.JobId, Columns.ApplicationId).map(
      _.toLowerCase(Locale.ROOT)
    )

  private def topLevel(column: String): String = column.takeWhile(_ != '.').toLowerCase(Locale.ROOT)

  def isForbidden(column: String): Boolean = ForbiddenColumns.contains(topLevel(column))

  def render(columns: Vector[String]): String = columns.mkString(",")

  def parse(value: String): Vector[String] = value.split(',').toVector.map(_.trim).filter(_.nonEmpty)

  /** Whether the stored properties are exactly the declared allow-list. Tables that are not subject-attributed carry no
    * tokens and are not constrained.
    */
  def permits(spec: DeltaTableSpec, configuration: Map[String, String]): Boolean =
    !spec.subjectAttributed || {
      val stored = configuration.get(StatsColumnsProperty).map(parse)
      stored.contains(spec.statsColumns) &&
      !spec.statsColumns.exists(isForbidden) &&
      !configuration.contains(NumIndexedColumnsProperty)
    }
}
