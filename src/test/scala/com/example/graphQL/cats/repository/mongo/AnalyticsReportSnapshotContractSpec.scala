package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.{
  AnalyticsFunnelDay,
  AnalyticsHiringStageDuration,
  AnalyticsHiringStageMetric,
  AnalyticsReportSnapshot,
  AnalyticsSkillPostingDay,
  AnalyticsTimeToHire
}
import org.bson.Document

import java.nio.file.{Files, Path}
import java.time.Instant

class AnalyticsReportSnapshotContractSpec extends munit.FunSuite {
  private val fixture: String = {
    val relative = Path.of("test-support", "src", "main", "resources", "analytics-report-snapshot-contract.json")
    val start = Path.of(System.getProperty("user.dir")).toAbsolutePath
    val found = Iterator
      .iterate(Option(start))(_.flatMap(dir => Option(dir.getParent)))
      .flatten
      .map(_.resolve(relative))
      .find(Files.isRegularFile(_))
    Files.readString(found.getOrElse(fail("Analytics report snapshot contract fixture is missing")))
  }

  test("HCS-01.3 AC-03 the stored analytics report snapshot contract decodes for Admin reporting") {
    val day = Instant.parse("2026-09-16T00:00:00Z")
    val expected = AnalyticsReportSnapshot(
      asOf = Instant.parse("2026-09-17T08:00:00Z"),
      funnel = List(AnalyticsFunnelDay(day, 12L, 5L, 1L, 3L, 2L, 1L)),
      timeToHire = Some(AnalyticsTimeToHire(24.5, 48.0, 72.25, 96.0, 2L, 1L)),
      stageDurations = Vector(
        AnalyticsHiringStageDuration(AnalyticsHiringStageMetric.TimeToHire, 24.5, 48.0, 72.25, 96.0, 12L),
        AnalyticsHiringStageDuration(AnalyticsHiringStageMetric.InterviewToHire, 6.0, 12.0, 12.0, 30.5, 10L)
      ),
      skillPostingActivity = List(AnalyticsSkillPostingDay(day, "scala", 12L))
    )

    assertEquals(AnalyticsReportSnapshotDocument.read(Document.parse(fixture)), Right(expected))
  }

  test("AC-03 the contract document without stageDurations is unreadable") {
    val document = Document.parse(fixture)
    document.remove("stageDurations")

    assert(AnalyticsReportSnapshotDocument.read(document).isLeft)
  }
}
