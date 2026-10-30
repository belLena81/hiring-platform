package com.example.hiring.analytics.hiringstory

/** The hired-cohort dataset (60 candidates, spec section "Hired-cohort dataset") through the real batch.
  *
  * Expected values are the literals of the spec's table, written before any run; they are not derived from the report
  * or the aggregation code. Evidence is provisional until the analytics refactoring closes.
  */
final class HiringCohortAnalyticsReportIntegrationSpec
    extends StoryAnalyticsReportSuite("cohort-events.jsonl", "cohort-report-documents.jsonl", "hiring-cohort-run") {

  test("HEA-12.1 the report's funnel equals the cohort table: one day, every stage from at least ten candidates") {
    val b = observation().before
    println(s"HEA-12.1 cohort report funnel: $b")
    assertEquals(b.funnelDays, 1, "all events of one run fall on one UTC day (rerun if the run crossed midnight)")
    assertEquals(
      (b.created, b.accepted, b.interview, b.hired, b.rejected, b.declined),
      (60L, 30L, 20L, 10L, 20L, 10L),
      clues(b)
    )
  }

  test("HEA-13.1 time to hire and all five stage durations are present and consistent with the event timestamps") {
    val o = observation()
    val b = o.before
    println(s"HEA-13.1 cohort timeToHire=${b.timeToHire} stageEligible=${b.stageEligible}")
    assertEquals(
      b.stageMetrics,
      Set("TIME_TO_ACCEPT", "TIME_TO_INTERVIEW", "TIME_TO_HIRE", "ACCEPTED_TO_INTERVIEW", "INTERVIEW_TO_HIRE")
    )
    assertEquals(
      b.stageEligible,
      Map(
        "TIME_TO_ACCEPT" -> 30L,
        "TIME_TO_INTERVIEW" -> 20L,
        "TIME_TO_HIRE" -> 10L,
        "ACCEPTED_TO_INTERVIEW" -> 20L,
        "INTERVIEW_TO_HIRE" -> 10L
      )
    )
    // Separate computation from the published events: created-to-hired hours of the hired applications.
    val hours = stagesFromEvents(o.events).values.flatMap { stages =>
      for { created <- stages.get("createdAt"); hired <- stages.get("hiredAt") } yield java.time.Duration
        .between(created, hired)
        .toMillis / 3600000.0
    }.toVector
    assertEquals(hours.size, 10, "ten hired applications in the published events")
    val hire = b.timeToHire.getOrElse(fail("timeToHire missing"))
    assertEquals((hire.eligibleCount, hire.excludedCount), (10L, 50L), clues(hire))
    val tolerance = 1e-9
    println(
      s"HEA-13.1 cohort report p50..p95 hours=${(hire.p50, hire.p75, hire.p90, hire.p95)} " +
        s"seconds=${(hire.p50 * 3600, hire.p75 * 3600, hire.p90 * 3600, hire.p95 * 3600)} " +
        s"events min/max hours=${(hours.min, hours.max)} seconds=${(hours.min * 3600, hours.max * 3600)}"
    )
    assert(hire.p50 >= 0.0, clues(hire))
    assert(hire.p50 >= hours.min - tolerance && hire.p95 <= hours.max + tolerance, clues(hire, hours.min, hours.max))
    assert(hire.p50 <= hire.p75 && hire.p75 <= hire.p90 && hire.p90 <= hire.p95, clues(hire))
  }

  test("HEA-11.2 reading the cohort's Kafka range twice leaves a non-empty report and the stage times unchanged") {
    val o = observation()
    assertEquals(o.stagesBefore.size, 60, "one stage-time row per cohort application")
    assertEquals(o.stagesAfter, o.stagesBefore, "the per-application stage times changed on the second read")
    assertEquals(o.before.created, 60L, "the report is not empty, so equality below is not trivial")
    assertEquals(o.after, o.before, "the report changed after reading the same range again")
    assertEquals(o.bronzeAfterSecond, o.eventCount.toLong, "the second read must not duplicate Bronze")
    assertEquals(o.silverAfterSecond, o.eventCount.toLong, "Silver holds one row per distinct event")
  }
}
