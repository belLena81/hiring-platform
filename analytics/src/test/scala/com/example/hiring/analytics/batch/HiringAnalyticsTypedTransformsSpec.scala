package com.example.hiring.analytics.batch

import com.example.hiring.analytics.{AnalyticsEventType, AnalyticsApplicationStatus}
import com.example.hiring.analytics.AnalyticsError

import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}
import cats.effect.unsafe.implicits.global

import java.sql.Timestamp
import java.time.Instant
import scala.reflect.runtime.universe.TypeTag

class HiringAnalyticsTypedTransformsSpec extends FunSuite {
  private given TypeTag[SilverHiringEvent] = SparkProductTypeTag[SilverHiringEvent]
  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("HiringAnalyticsTypedTransformsSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  test("Silver and Gold Dataset boundaries retain persisted columns and aggregate metrics") {
    import spark.implicits.*

    val start = Instant.parse("2026-01-01T10:00:00Z")
    val subjects = (1 to 10).map(i => s"token-$i")
    val events = subjects.zipWithIndex.flatMap { case (token, index) =>
      val suffix = index.toString
      Seq(
        SilverHiringEvent(
          Some(s"created-$suffix"),
          Some(AnalyticsEventType.ApplicationCreated.wire),
          Some(ts(start)),
          Some("Application"),
          Some(s"application-$suffix"),
          Some(s"application-$suffix"),
          Some(s"job-$suffix"),
          None,
          None,
          Some(token),
          Seq(token),
          Some(s"fingerprint-created-$suffix")
        ),
        SilverHiringEvent(
          Some(s"hired-$suffix"),
          Some(AnalyticsEventType.ApplicationStatusChanged.wire),
          Some(ts(start.plusSeconds(3600))),
          Some("Application"),
          Some(s"application-$suffix"),
          Some(s"application-$suffix"),
          Some(s"job-$suffix"),
          Some(AnalyticsApplicationStatus.Hired.wire),
          None,
          Some(token),
          Seq(token),
          Some(s"fingerprint-hired-$suffix")
        ),
        SilverHiringEvent(
          Some(s"job-$suffix"),
          Some(AnalyticsEventType.JobCreated.wire),
          Some(ts(start)),
          Some("Job"),
          Some(s"job-$suffix"),
          None,
          Some(s"job-$suffix"),
          None,
          Some(Seq("Scala")),
          Some(token),
          Seq(token),
          Some(s"fingerprint-job-$suffix")
        )
      )
    }
    val silver = events.toDS().toDF()
    val persistedSilver = OperationalEventTransforms.typedSilver(silver)

    assertEquals(
      persistedSilver.schema.fieldNames.toSeq,
      Seq(
        "eventId",
        "eventType",
        "occurredAt",
        "aggregateType",
        "aggregateId",
        "applicationId",
        "jobId",
        "newStatus",
        "jobSkills",
        "subjectToken",
        "subjectTokens",
        "eventFingerprint"
      )
    )
    assertEquals(
      persistedSilver.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq(
        "string",
        "string",
        "timestamp",
        "string",
        "string",
        "string",
        "string",
        "string",
        "array<string>",
        "string",
        "array<string>",
        "string"
      )
    )
    assertEquals(persistedSilver.count(), 30L)

    val funnel = HiringGoldTransforms.funnelActivity(silver)
    assertEquals(funnel.schema.fieldNames.toSeq, Seq("day", "eventType", "newStatus", "contributingApplications"))
    assertEquals(
      funnel.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq("timestamp", "string", "string", "bigint")
    )
    assertEquals(funnel.select("contributingApplications").as[Long].collect().toSeq, Seq(10L, 10L))

    val wide = HiringGoldTransforms.wideFunnelDay(silver)
    assertEquals(
      wide.schema.fieldNames.toSeq,
      Seq("day", "created", "accepted", "declined", "interview", "hired", "rejected")
    )
    assertEquals(
      wide.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq.fill(1)("timestamp") ++ Seq.fill(6)("bigint")
    )
    val wideRow = wide.head()
    assertEquals(wideRow.getAs[Long]("created"), 10L)
    assertEquals(wideRow.getAs[Long]("hired"), 10L)

    val skills = HiringGoldTransforms.skillPostingActivity(silver)
    assertEquals(skills.schema.fieldNames.toSeq, Seq("day", "skill", "postings"))
    assertEquals(skills.schema.fields.map(_.dataType.simpleString).toSeq, Seq("timestamp", "string", "bigint"))
    assertEquals(
      skills.select("skill", "postings").collect().toSeq.map(r => r.getString(0) -> r.getLong(1)),
      Seq("scala" -> 10L)
    )

    val timeToHire = HiringGoldTransforms.timeToHire(silver).unsafeRunSync()
    assertEquals(
      timeToHire.schema.fieldNames.toSeq,
      Seq("p50Hours", "p75Hours", "p90Hours", "p95Hours", "eligibleCount", "excludedCount")
    )
    assertEquals(
      timeToHire.schema.fields.map(_.dataType.simpleString).toSeq,
      Seq.fill(4)("double") ++ Seq.fill(2)("bigint")
    )
    assertEquals(timeToHire.head().getAs[Long]("eligibleCount"), 10L)
    assertEquals(timeToHire.head().getAs[Long]("excludedCount"), 0L)
  }

  test("report extraction schema contract detects field and type drift") {
    val invalid = StructType.fromDDL("day STRING, created BIGINT")
    val valid = StructType.fromDDL(
      "day TIMESTAMP, created BIGINT, accepted BIGINT, declined BIGINT, interview BIGINT, hired BIGINT, rejected BIGINT"
    )
    val expected = Vector(
      "day" -> DataTypes.TimestampType,
      "created" -> DataTypes.LongType,
      "accepted" -> DataTypes.LongType,
      "declined" -> DataTypes.LongType,
      "interview" -> DataTypes.LongType,
      "hired" -> DataTypes.LongType,
      "rejected" -> DataTypes.LongType
    )
    assertEquals(AnalyticsGoldStage.validateOutputSchema(invalid, expected), Left(AnalyticsError.InvalidGoldSchema))
    assertEquals(AnalyticsGoldStage.validateOutputSchema(valid, expected), Right(()))
  }

  private def ts(value: Instant): Timestamp = Timestamp.from(value)
}
