package com.example.graphQL.cats.acceptance

/** The hired cohort's report read through the real Admin `analyticsReport` (spec: "Hired-cohort dataset"). Expected
  * values are the spec's literals; evidence is provisional until the analytics refactoring closes.
  */
class HiringCohortAnalyticsAdminReportIntegrationSpec extends StoryAdminReportSuite("cohort-report-documents.jsonl") {
  test("HEA-12.1 / HEA-13.1 the Admin report of the cohort shows the funnel and every duration distribution") {
    for {
      admin <- adminActor
      _ <- storePublishedReport
      report <- operations.read(query, "analyticsReport", period, admin)
    } yield {
      val funnel = report.hcursor.downField("funnel").focus.flatMap(_.asArray).getOrElse(Vector.empty)
      def total(field: String): Long = funnel.flatMap(_.hcursor.get[Long](field).toOption).sum
      val observed =
        (total("created"), total("accepted"), total("interview"), total("hired"), total("rejected"), total("declined"))
      val metrics = report.hcursor
        .downField("stageDurations")
        .focus
        .flatMap(_.asArray)
        .getOrElse(Vector.empty)
        .flatMap(_.hcursor.get[String]("metric").toOption)
        .toSet
      val hire = report.hcursor.downField("timeToHire")
      println(s"HEA-12.1 Admin cohort funnel rows=${funnel.size} totals=$observed")
      println(s"HEA-13.1 Admin cohort timeToHire=${hire.focus.map(_.noSpaces)} metrics=$metrics")
      assertEquals(funnel.size, 1)
      assertEquals(observed, (60L, 30L, 20L, 10L, 20L, 10L), clues(report))
      assertEquals(
        metrics,
        Set("TIME_TO_ACCEPT", "TIME_TO_INTERVIEW", "TIME_TO_HIRE", "ACCEPTED_TO_INTERVIEW", "INTERVIEW_TO_HIRE")
      )
      assertEquals((hire.get[Long]("eligibleCount"), hire.get[Long]("excludedCount")), (Right(10L), Right(50L)))
    }
  }
}
