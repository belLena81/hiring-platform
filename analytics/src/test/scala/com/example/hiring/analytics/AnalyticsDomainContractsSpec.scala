package com.example.hiring.analytics

import com.example.hiring.analytics.domain.*

class AnalyticsDomainContractsSpec extends munit.FunSuite {
  test("account subject identifiers are UUID validated independently of analytics run IDs") {
    val raw = "7b68ad01-b570-4de8-8ab9-564ae5dd0aa6"
    assertEquals(AccountSubjectId.from(raw).map(_.value), Right(raw))
    assert(AccountSubjectId.from("analytics-run-1").isLeft)
    assert(AccountSubjectId.from("1-1-1-1-1").isLeft)
  }

  test("range fingerprints accept only lowercase SHA-256 hex values") {
    val digest = "0123456789abcdef" * 4
    assertEquals(RangeFingerprint.from(digest).map(_.value), Right(digest))
    assert(RangeFingerprint.from("0123456789abcdef").isLeft)
    assert(RangeFingerprint.from(digest.toUpperCase).isLeft)
  }

  test("validated ranges remain trusted when constructing manifests") {
    val range = PartitionOffsetRange.from("hiring.operational-events", 0, 2L, 7L).toEither.toOption.get
    val runId = RunId.from("valid-run").toEither.toOption.get
    val manifest = AnalyticsRunManifest.from(runId, Vector(range))
    assert(manifest.isValid)
    assertEquals(manifest.toEither.toOption.get.offsetRanges, Vector(range))
  }
}
