package com.example.graphQL.cats.acceptance

import cats.syntax.all.*

/** The hired cohort's report read through the real Admin `analyticsReport` (spec: "Hired-cohort dataset"). Expected
  * values are the spec's literals; evidence is provisional until the analytics refactoring closes.
  */
class HiringCohortAnalyticsAdminReportIntegrationSpec
    extends StoryAdminReportSuite("cohort-events.jsonl", "cohort-report-documents.jsonl") {
  test("HEA-12.1 / HEA-13.1 the Admin report of the cohort shows the funnel and every duration distribution") {
    requireChain()
    for {
      admin <- adminActor
      _ <- storePublishedReport
      report <- operations.read(query, "analyticsReport", period, admin)
    } yield {
      val funnel = report.hcursor.downField("funnel").focus.flatMap(_.asArray).getOrElse(Vector.empty)
      def total(field: String): Long = funnel.flatMap(_.hcursor.get[Long](field).toOption).sum
      val observed =
        (total("created"), total("accepted"), total("interview"), total("hired"), total("rejected"), total("declined"))
      val eligibleByMetric = report.hcursor
        .downField("stageDurations")
        .focus
        .flatMap(_.asArray)
        .getOrElse(Vector.empty)
        .flatMap(d => (d.hcursor.get[String]("metric"), d.hcursor.get[Long]("eligibleCount")).tupled.toOption)
        .toMap
      val hire = report.hcursor.downField("timeToHire")
      println(s"HEA-12.1 Admin cohort funnel rows=${funnel.size} totals=$observed")
      println(s"HEA-13.1 Admin cohort timeToHire=${hire.focus.map(_.noSpaces)} eligible=$eligibleByMetric")
      assertEquals(funnel.size, 1)
      assertEquals(observed, (60L, 30L, 20L, 10L, 20L, 10L), clues(report))
      assertEquals(
        eligibleByMetric,
        Map(
          "TIME_TO_ACCEPT" -> 30L,
          "TIME_TO_INTERVIEW" -> 20L,
          "TIME_TO_HIRE" -> 10L,
          "ACCEPTED_TO_INTERVIEW" -> 20L,
          "INTERVIEW_TO_HIRE" -> 10L
        )
      )
      // Created-to-hired hours computed from the published events (the file the digest check just bound), not the report.
      val eventsFile = java.nio.file.Path
        .of(stack.running.namespace.manifest.workspace)
        .resolve(".local/data/hiring-story-analytics/cohort-events.jsonl")
      val stages = scala.jdk.CollectionConverters
        .ListHasAsScala(java.nio.file.Files.readAllLines(eventsFile))
        .asScala
        .toVector
        .flatMap(line => io.circe.parser.parse(line).toOption)
        .flatMap(_.hcursor.get[String]("value").toOption)
        .flatMap(io.circe.parser.parse(_).toOption)
        .flatMap { e =>
          val c = e.hcursor
          val at = c.get[String]("occurredAt").toOption.map(java.time.Instant.parse)
          val app = c.downField("payload").get[String]("applicationId").toOption
          val kind = c.get[String]("eventType").toOption
          // Only status changes carry `newStatus`; creation events carry `status`, so absence must not drop the event.
          val status = c.downField("payload").get[String]("newStatus").toOption.getOrElse("")
          (app, at, kind).tupled.map { case (a, t, k) => (a, k, status, t) }.toVector
        }
      val created = stages.collect { case (a, "APPLICATION_CREATED", _, t) => a -> t }.toMap
      val hired = stages.collect { case (a, "APPLICATION_STATUS_CHANGED", "Hired", t) => a -> t }.toMap
      val hours = hired.toVector.map { case (a, t) => java.time.Duration.between(created(a), t).toMillis / 3600000.0 }
      assertEquals(hours.size, 10)
      val percentiles =
        List("p50Hours", "p75Hours", "p90Hours", "p95Hours").map(name => hire.get[Double](name).toOption)
      println(
        s"HEA-13.1 Admin cohort p50..p95 hours=$percentiles seconds=${percentiles.map(_.map(_ * 3600))} " +
          s"events min/max hours=${(hours.min, hours.max)} seconds=${(hours.min * 3600, hours.max * 3600)}"
      )
      val values = percentiles.flatten
      assertEquals(values.size, 4)
      val tolerance = 1e-9
      assert(values.forall(v => v >= hours.min - tolerance && v <= hours.max + tolerance), clues(values, hours))
      assertEquals(values, values.sorted)
      assertEquals((hire.get[Long]("eligibleCount"), hire.get[Long]("excludedCount")), (Right(10L), Right(50L)))
    }
  }
}
