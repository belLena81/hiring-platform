package com.example.hiring.analytics.adapter.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, min}

/** Lets a MERGE keyed on Kafka coordinates use the table's data-skipping statistics.
  *
  * Delta prunes target files only with literal predicates on target columns; a join key such as
  * `target.offset = source.offset` carries no literal, so without help every data file is read. A source row can match
  * a target row only when topic, partition and offset are all equal, so restricting the target to the offset range the
  * source holds per topic and partition cannot change which rows match. The bounds are added to the condition as
  * literals and Delta applies them to the min/max statistics of the coordinate columns (which are on every
  * subject-attributed table's statistics allow-list).
  */
private[analytics] object MergeKeyBounds {

  /** The one merge condition shared by the Bronze and late-fact writers. */
  val CoordinateCondition: String =
    "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"

  /** Beyond this many topic partitions in one batch the bounds would be long, so the merge keeps the plain condition.
    */
  val MaximumRanges = 256

  private def literal(value: String): String = s"'${value.replace("'", "''")}'"

  /** `condition` plus per topic-partition offset bounds from `source`; unchanged for any other condition. */
  def bounded(source: DataFrame, condition: String): String =
    if (condition != CoordinateCondition) condition
    else {
      val ranges = source
        .groupBy(col(Columns.Topic), col(Columns.Partition))
        .agg(min(col(Columns.Offset)), max(col(Columns.Offset)))
        .limit(MaximumRanges + 1)
        .collect()
        .toVector
        .map(row => (row.getString(0), row.getInt(1), row.getLong(2), row.getLong(3)))
        .sorted
      if (ranges.isEmpty || ranges.size > MaximumRanges) condition
      else {
        val alternatives = ranges.map { case (topic, partition, first, last) =>
          s"(target.topic = ${literal(topic)} AND target.partition = $partition AND " +
            s"target.offset >= $first AND target.offset <= $last)"
        }
        s"$condition AND (${alternatives.mkString(" OR ")})"
      }
    }
}
