package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.AnalyticsTestSpark
import com.example.hiring.analytics.domain.{AnalyticsEventTimePolicy, EventTimeAdmission}
import munit.FunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.scalacheck.Gen
import org.scalacheck.rng.Seed

import java.sql.Timestamp
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** AC-06: the built-in admission expression equals `AnalyticsEventTimePolicy.admit` for every generated input. */
class EventTimeAdmissionExpressionSpec extends FunSuite {
  private lazy val spark = AnalyticsTestSpark.session("event-time-admission-expression")
  import spark.implicits.*

  private val Day = 86400L * 1000000L
  private val Minute = 60L * 1000000L
  private val FarFuture = Instant.parse("9999-12-31T23:59:59.999999Z")
  private val FarPast = Instant.parse("0002-01-01T00:00:00Z")

  private def instantOfMicros(micros: Long): Instant =
    Instant.ofEpochSecond(Math.floorDiv(micros, 1000000L), Math.floorMod(micros, 1000000L) * 1000L)

  /** Observed instants include sub-microsecond nanoseconds, the epoch and a far-future clock. */
  private val observedInstants = Vector(
    Instant.parse("2026-09-30T12:00:00Z"),
    Instant.parse("2026-09-30T12:00:00.123456789Z"),
    Instant.parse("2026-09-30T00:00:00Z"),
    Instant.EPOCH,
    Instant.parse("1969-12-31T23:59:59.999999Z"),
    Instant.parse("9999-12-30T12:00:00.000001Z")
  )

  /** Watermarks sit on, just before and just after day ends, with and without nanoseconds. */
  private def watermarksFor(observedAt: Instant): Vector[Option[Instant]] = {
    val dayEnd = Instant.ofEpochSecond(Math.floorDiv(observedAt.getEpochSecond, 86400L) * 86400L + 86400L)
    Vector(
      None,
      Some(dayEnd),
      Some(dayEnd.minusNanos(1)),
      Some(dayEnd.plusNanos(1)),
      Some(dayEnd.minusSeconds(86400L)),
      Some(observedAt.minusSeconds(86400L)),
      Some(Instant.EPOCH),
      Some(Instant.parse("1969-12-31T23:59:59.999999Z"))
    )
  }

  /** Event times: fixed boundaries around the observed instant, watermark and day ends, plus generated ones. */
  private def eventMicrosFor(observedAt: Instant, watermark: Option[Instant], seed: Long): Vector[Long] = {
    val observed = EventTimeAdmissionExpression.epochMicros(observedAt)
    def around(center: Long): Vector[Long] = Vector(-Day, -1L, 0L, 1L, Day).map(center + _)
    val skew = AnalyticsEventTimePolicy.AllowedFutureSkew.toMicros
    val watermarkMicros = watermark.map(EventTimeAdmissionExpression.epochMicros).toVector
    val fixed = (around(observed) ++ around(observed + skew) ++ watermarkMicros.flatMap(around) ++
      Vector(
        0L,
        -1L,
        1L,
        EventTimeAdmissionExpression.epochMicros(FarFuture),
        EventTimeAdmissionExpression.epochMicros(FarPast)
      ) ++
      Vector(Minute, -Minute).map(observed + _)).distinct
    val minMicros = EventTimeAdmissionExpression.epochMicros(FarPast)
    val maxMicros = EventTimeAdmissionExpression.epochMicros(FarFuture)
    val generator = Gen.oneOf(
      Gen.choose(minMicros, maxMicros),
      Gen.choose(observed - 3 * Day, observed + 3 * Day),
      Gen.choose(-3 * Day, 3 * Day)
    )
    val generated = Iterator
      .iterate(Seed(seed))(_.next)
      .take(200)
      .flatMap { s =>
        generator.apply(Gen.Parameters.default, s)
      }
      .toVector
    fixed ++ generated
  }

  private def expected(eventMicros: Long, observedAt: Instant, watermark: Option[Instant]): (String, Option[Long]) =
    AnalyticsEventTimePolicy.admit(instantOfMicros(eventMicros), observedAt, watermark) match {
      case admitted @ EventTimeAdmission.Admitted(effective) =>
        (admitted.wireStatus, Some(EventTimeAdmissionExpression.epochMicros(effective)))
      case other => (other.wireStatus, None)
    }

  private def classified(eventMicros: Vector[Long], observedAt: Instant, watermark: Option[Instant]) = {
    val frame = eventMicros.map(Long.box).toDF("micros").withColumn("t", timestamp_micros(col("micros")))
    frame
      .select(
        col("micros"),
        EventTimeAdmissionExpression.classify(col("t"), observedAt, watermark).as("admission")
      )
      .select(
        col("micros"),
        col("admission.status").as("status"),
        unix_micros(col("admission.effectiveTime")).as("effective")
      )
  }

  test("AC-06.1 the expression equals AnalyticsEventTimePolicy over generated and boundary inputs") {
    var compared = 0
    for {
      (observedAt, index) <- observedInstants.zipWithIndex
      watermark <- watermarksFor(observedAt)
    } {
      val micros = eventMicrosFor(observedAt, watermark, 1000L + index)
      val rows = classified(micros, observedAt, watermark).collect()
      assertEquals(rows.length, micros.length)
      rows.foreach { row =>
        val eventMicros = row.getLong(0)
        val actual = (row.getString(1), Option(row.get(2)).map(_ => row.getLong(2)))
        assertEquals(
          actual,
          expected(eventMicros, observedAt, watermark),
          s"event=${instantOfMicros(eventMicros)} observedAt=$observedAt watermark=$watermark"
        )
        compared += 1
      }
    }
    assert(compared > 5000, s"only $compared inputs compared")
  }

  test("DDR-04.2 the expression classifies facts inside, at and past a configured allowed-lateness bound") {
    val lateness = scala.concurrent.duration.DurationInt(48).hours
    val newest = Instant.parse("2026-10-02T00:00:00Z")
    val observedAt = newest
    val watermark = AnalyticsEventTimePolicy.candidateWatermark(None, List(newest), observedAt, lateness)
    assertEquals(watermark, Some(Instant.parse("2026-09-30T00:00:00Z")))
    val bound = watermark.get
    val facts = Vector(
      "inside: first instant of the open day" -> (bound, EventTimeAdmission.OpenStatus),
      "inside: last microsecond of the open day" -> (
        bound.plusSeconds(86399L).plusNanos(999000L),
        EventTimeAdmission.OpenStatus
      ),
      "at: last microsecond of the day ending exactly at the bound" -> (
        bound.minusNanos(1000L),
        EventTimeAdmission.ClosedStatus
      ),
      "past: five days before the bound" -> (bound.minusSeconds(5L * 86400L), EventTimeAdmission.ClosedStatus)
    )
    val frame = spark
      .createDataFrame(
        facts.map { case (name, (time, _)) => Row(name, Timestamp.from(time)) }.asJava,
        StructType(
          Vector(StructField("name", StringType, nullable = false), StructField("t", TimestampType, nullable = false))
        )
      )
      .select(col("name"), EventTimeAdmissionExpression.classify(col("t"), observedAt, watermark).as("a"))
      .select(col("name"), col("a.status"), col("a.effectiveTime"))
    val rows = frame.collect().map(row => row.getString(0) -> (row.getString(1), Option(row.get(2)))).toMap
    facts.foreach { case (name, (_, status)) =>
      assertEquals(rows(name)._1, status, name)
      assertEquals(rows(name)._2.isDefined, status == EventTimeAdmission.OpenStatus, name)
      val policy = AnalyticsEventTimePolicy.admit(facts.find(_._1 == name).get._2._1, observedAt, watermark)
      assertEquals(policy.wireStatus, status, name)
    }
  }

  test("AC-06.1 a null event time is rejected as FUTURE with no effective time, as before") {
    val frame = spark.createDataFrame(
      List(Row(null)).asJava,
      StructType(Vector(StructField("t", TimestampType, nullable = true)))
    )
    val row = frame
      .select(EventTimeAdmissionExpression.classify(col("t"), Instant.parse("2026-09-30T12:00:00Z"), None).as("a"))
      .select(col("a.status"), col("a.effectiveTime"))
      .head()
    assertEquals(row.getString(0), EventTimeAdmission.FutureStatus)
    assert(row.isNullAt(1))
  }

  test("AC-06.1 the result does not depend on the session time zone") {
    val observedAt = Instant.parse("2026-09-30T12:00:00Z")
    val watermark = Some(Instant.parse("2026-09-30T00:00:00Z"))
    val micros = eventMicrosFor(observedAt, watermark, 7L)
    val inUtc = classified(micros, observedAt, watermark).collect().map(_.toSeq).toVector
    spark.conf.set("spark.sql.session.timeZone", "Pacific/Kiritimati")
    try assertEquals(classified(micros, observedAt, watermark).collect().map(_.toSeq).toVector, inUtc)
    finally spark.conf.set("spark.sql.session.timeZone", "UTC")
  }

  test("AC-06.2 the executed plan has no user-defined function and runs in whole-stage codegen") {
    val observedAt = Instant.parse("2026-09-30T12:00:00Z")
    val watermark = Some(Instant.parse("2026-09-30T00:00:00Z"))
    val source = spark.range(10).select(timestamp_micros(col("id")).as("t"))
    val builtIn = source.select(EventTimeAdmissionExpression.classify(col("t"), observedAt, watermark).as("a"))
    val builtInPlan = builtIn.queryExecution.executedPlan.toString
    // The previous implementation, kept only as the plan control.
    val control = udf(
      (t: Timestamp) =>
        if (t == null) 0 else AnalyticsEventTimePolicy.admit(t.toInstant, observedAt, watermark).ordinal,
      IntegerType
    )
    val controlPlan = source.select(control(col("t")).as("a")).queryExecution.executedPlan.toString
    println(s"ADMISSION_PLAN_BUILT_IN\n$builtInPlan")
    println(s"ADMISSION_PLAN_UDF_CONTROL\n$controlPlan")
    assert(!builtInPlan.contains("UDF"), builtInPlan)
    assert(builtInPlan.contains("*("), builtInPlan)
    assert(controlPlan.contains("UDF"), controlPlan)
  }
}
