package com.example.hiring.analytics.hiringstory

import com.example.hiring.analytics.service.batch.AnalyticsRunOutcome

/** The 12-candidate story's events through the real batch (spec: docs/specs/hiring-end-to-end-acceptance.md). Evidence
  * is provisional until the analytics refactoring closes. The funnel and duration reports are checked on the hired
  * cohort (`HiringCohortAnalyticsReportIntegrationSpec`); here the small slice proves stage facts and suppression.
  */
final class HiringStoryAnalyticsReportIntegrationSpec
    extends StoryAnalyticsReportSuite("story-events.jsonl", "report-documents.jsonl", "hiring-story-run") {
  // Oracle: the spec's story table. Stages reached, system-wide (J1 plus the two J2 applications).
  private object Oracle {
    val applicationsCreated = 14L // 12 on J1, C01 on J2, C02 on J2
    val candidatesApplying = 12L
    val accepted = 3L // C01, C03, C05
    val interview = 2L // C01, C03
    val hired = 1L // C01
    val rejected = 2L // C02, C03
    val declined = 1L // C04
    val minimumDistinctCandidates = 10L
  }

  test("HEA-11.2 reading the same Kafka range twice leaves the report, Bronze, Silver and stage times unchanged") {
    val o = observation()
    println(s"HEA-11.2 events=${o.eventCount} range=[0,${o.eventCount * 2}) bronze=${o.bronzeAfterSecond} silver=${o.silverAfterSecond}")
    assertEquals(o.first.outcome, AnalyticsRunOutcome.Published)
    assertEquals(o.second.outcome, AnalyticsRunOutcome.Published)
    assertEquals(o.first.bronzeRecords, o.eventCount.toLong)
    assertEquals(o.first.quarantinedRecords, 0L, "no story event may be quarantined")
    assertEquals(o.second.quarantinedRecords, 0L)
    assertEquals(o.bronzeAfterSecond, o.eventCount.toLong, "the second read must not duplicate Bronze")
    assertEquals(o.silverAfterSecond, o.eventCount.toLong, "Silver holds one row per distinct event")
    assertEquals(o.stagesAfter, o.stagesBefore, "the per-application stage times changed on the second read")
    assertEquals(o.after, o.before, "the report changed after reading the same range again")
  }


  test("HEA-12.3 the stored stage facts equal the story: stages reached per application (Silver, before any suppression)") {
    val o = observation()
    val reached = StageColumns.map(name => name -> o.stagesAfter.count(_.stages.contains(name)).toLong).toMap
    println(s"HEA-12.3 applications with each stage reached: $reached of ${o.stagesAfter.size} applications")
    assertEquals(o.stagesAfter.size.toLong, Oracle.applicationsCreated)
    assertEquals(reached("createdAt"), Oracle.applicationsCreated)
    assertEquals(reached("acceptedAt"), Oracle.accepted)
    assertEquals(reached("interviewAt"), Oracle.interview)
    assertEquals(reached("hiredAt"), Oracle.hired)
    assertEquals(reached("rejectedAt"), Oracle.rejected)
    assertEquals(reached("declinedAt"), Oracle.declined)
  }


  test("HEA-13.3 stage times equal the earliest event time per stage taken from the published events") {
    val o = observation()
    val expected = stagesFromEvents(o.events)
    assertEquals(o.stagesAfter.map(row => row.applicationId -> row.stages).toMap, expected)
  }


  test("HEA-13.2 a distribution built from fewer than ten distinct candidates is withheld") {
    val o = observation()
    // Candidates that reached each stage, from the oracle: every pair of stages involves fewer than ten candidates.
    val candidatesReaching = Vector(Oracle.accepted, Oracle.interview, Oracle.hired)
    assert(candidatesReaching.forall(_ < Oracle.minimumDistinctCandidates))
    assert(!o.before.timeToHirePresent, "time to hire built from one hired candidate must be withheld")
    assertEquals(o.before.stageMetrics, Set.empty[String])
  }
}
