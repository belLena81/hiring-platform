package com.example.hiring.analytics.adapter.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit, when}

/** Reference single-aggregate measurement used by tests to check the production classified measurement. */
object AdmissionQualityBaseline {

  /** One aggregate action preserves malformed/future/closed row counts and distinct conflicting event IDs. */
  def measure(
      malformedEvents: DataFrame,
      conflictingEvents: DataFrame,
      futureEvents: DataFrame,
      closedEvents: DataFrame
  ): SparkStreamingBatchStages.AdmissionQuality = {
    val category = "_hiringAdmissionQuality"
    val records = malformedEvents
      .select(lit("MALFORMED").as(category))
      .unionByName(conflictingEvents.select(Columns.EventId).distinct().select(lit("CONFLICT").as(category)))
      .unionByName(futureEvents.select(lit("FUTURE").as(category)))
      .unionByName(closedEvents.select(lit("CLOSED").as(category)))
    val measured = records
      .agg(
        count(when(col(category) === lit("MALFORMED"), lit(1))),
        count(when(col(category) === lit("CONFLICT"), lit(1))),
        count(when(col(category) === lit("FUTURE"), lit(1))),
        count(when(col(category) === lit("CLOSED"), lit(1)))
      )
      .head()
    SparkStreamingBatchStages.AdmissionQuality(
      measured.getLong(0),
      measured.getLong(1),
      measured.getLong(2),
      measured.getLong(3)
    )
  }
}
