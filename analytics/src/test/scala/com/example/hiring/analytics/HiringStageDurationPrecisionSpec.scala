package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.*

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, unix_timestamp}

import java.sql.Timestamp
import java.time.Instant

/** Hiring stage durations come from exact timestamp differences, not from whole-second values (finding F13). */
class HiringStageDurationPrecisionSpec extends AnalyticsBatchSpecFixtures {
  private val Created = Instant.parse("2026-10-30T10:00:00.900Z")
  private val Hour = 3600.0

  private def applicationAt(index: Int, created: Option[Instant], hired: Option[Instant]): Row =
    Row(
      s"application-$index",
      s"subject-$index",
      Seq(s"subject-$index"),
      created.map(Timestamp.from).orNull,
      null,
      null,
      null,
      hired.map(Timestamp.from).orNull,
      null,
      Timestamp.from(Created),
      null,
      Timestamp.from(Created.plusSeconds(86400L * 30))
    )

  private def stageTimes(rows: Seq[Row]): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.stageTimes)
    )

  private def timeToHireRow(rows: Seq[Row]): Row = {
    val result = HiringGoldTransforms.stageDurations(stageTimes(rows)).fold(error => fail(error.toString), identity)
    result.filter(result("metric") === "TIME_TO_HIRE").head()
  }

  private def hiredAfterMicros(index: Int, micros: Long): Row =
    applicationAt(index, Some(Created), Some(Created.plusNanos(micros * 1000L)))

  test("stage durations keep sub-second precision: 1.5 s to 1.82 s are not rounded to whole seconds") {
    val offsetsMicros =
      Vector(1500000L, 1820000L, 1500001L, 1819999L, 1600000L, 1700000L, 1650000L, 1750000L, 1550000L, 1800000L)
    val row = timeToHireRow(offsetsMicros.zipWithIndex.map { case (micros, index) => hiredAfterMicros(index, micros) })
    Vector("p50Hours", "p75Hours", "p90Hours", "p95Hours").foreach { name =>
      val hours = row.getAs[Double](name)
      assert(hours >= 1.5 / Hour - 1e-12 && hours <= 1.82 / Hour + 1e-12, s"$name $hours h outside 1.5 s .. 1.82 s")
    }
    assertEqualsDouble(row.getAs[Double]("p50Hours") * Hour, 1.65, 0.051)
  }

  test("a boundary just across a whole second is 1 ms, not 1 s, and just below a whole second is not 0") {
    // Created at .900 and hired 101 ms later (1.001): whole-second values 0 and 1 gave 1 s, about 10 times too large.
    // Created at .000 and hired 999 ms later: whole-second values gave 0 s.
    val across = (0 until 10).map(index => applicationAt(index, Some(Created), Some(Created.plusMillis(101)))) // .900 -> 1.001
    val rowAcross = timeToHireRow(across)
    assertEqualsDouble(rowAcross.getAs[Double]("p50Hours") * Hour, 0.101, 1e-6)
    // The replaced whole-second calculation, evaluated on the same rows, gives 1 s.
    val oldSeconds = stageTimes(across).select(
      (unix_timestamp(col("hiredAt")) - unix_timestamp(col("createdAt"))).as("seconds")
    ).head().getLong(0)
    assertEquals(oldSeconds, 1L)
    val whole = Instant.parse("2026-10-30T10:00:00Z")
    val below = (0 until 10).map(index => applicationAt(index, Some(whole), Some(whole.plusMillis(999))))
    assertEqualsDouble(timeToHireRow(below).getAs[Double]("p50Hours") * Hour, 0.999, 1e-6)
  }

  test("zero and negative durations stay excluded and incomplete applications are not counted") {
    val eligible = (0 until 10).map(index => hiredAfterMicros(index, 1500000L))
    val zero = applicationAt(20, Some(Created), Some(Created))
    val negative = applicationAt(21, Some(Created), Some(Created.minusNanos(1000L)))
    val missing = applicationAt(22, Some(Created), None)
    val result = HiringGoldTransforms
      .stageDurations(stageTimes(eligible ++ Seq(negative, missing)))
      .fold(error => fail(error.toString), identity)
    val hire = result.filter(result("metric") === "TIME_TO_HIRE").head()
    assertEquals(hire.getAs[Long]("eligibleCount"), 10L)
    assertEqualsDouble(hire.getAs[Double]("p50Hours") * Hour, 1.5, 1e-6)
    // Exactly zero is a valid, non-negative duration (as before); a negative one by one microsecond is not.
    val withZero = HiringGoldTransforms
      .stageDurations(stageTimes(eligible ++ Seq(zero)))
      .fold(error => fail(error.toString), identity)
    assertEquals(withZero.filter(withZero("metric") === "TIME_TO_HIRE").head().getAs[Long]("eligibleCount"), 11L)
  }

  test("fewer than ten candidates still suppress the stage duration") {
    val rows = (0 until 9).map(index => hiredAfterMicros(index, 1500000L))
    val result = HiringGoldTransforms.stageDurations(stageTimes(rows)).fold(error => fail(error.toString), identity)
    assertEquals(result.count(), 0L)
  }
}
