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
    val abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    import io.github.iltotore.iron.autoRefine
    assertEquals(RunId.prefixed("stream-", abc).value, s"stream-$abc")
    assertEquals(RangeFingerprint.ofSha256("abc").value, abc)
  }

  test("late-fact replay selections are bounded, unique, and canonically fingerprinted") {
    val left = AnalyticsLateFactReplayRequest.from(
      "replay-request-1",
      Vector(("hiring.events", 1, 8L), ("hiring.events", 0, 3L))
    )
    val reordered = AnalyticsLateFactReplayRequest.from(
      "replay-request-2",
      Vector(("hiring.events", 0, 3L), ("hiring.events", 1, 8L))
    )
    assert(left.isValid)
    assert(reordered.isValid)
    assertEquals(left.toEither.toOption.get.selectionDigest, reordered.toEither.toOption.get.selectionDigest)
    assertEquals(left.toEither.toOption.get.coordinates.size, 2)
    val changed = AnalyticsLateFactReplayRequest.from(
      "replay-request-3",
      Vector(("hiring.events", 1, 9L), ("hiring.events", 0, 3L))
    )
    assertNotEquals(left.toEither.toOption.get.selectionDigest, changed.toEither.toOption.get.selectionDigest)

    val duplicate = AnalyticsLateFactReplayRequest.from(
      "replay-request-1",
      Vector(("hiring.events", 0, 3L), ("hiring.events", 0, 3L))
    )
    assert(
      duplicate.toEither.swap.toOption
        .exists(_.toNonEmptyList.toList.contains("replay Kafka coordinates must be unique"))
    )

    val maximum =
      Vector.tabulate(AnalyticsLateFactReplayRequest.MaximumCoordinates)(index => ("hiring.events", 0, index.toLong))
    assert(AnalyticsLateFactReplayRequest.from("maximum-request", maximum).isValid)
    assert(!AnalyticsLateFactReplayRequest.from("oversized-request", maximum :+ ("hiring.events", 0, 1000L)).isValid)
    assert(!AnalyticsLateFactReplayRequest.from("empty-request", Vector.empty).isValid)
    assert(!AnalyticsLateFactReplayRequest.from("zero-limit", Vector(("hiring.events", 0, 1L)), 0).isValid)
    assert(!AnalyticsLateFactReplayRequest.from("too-large-limit", Vector(("hiring.events", 0, 1L)), 1001).isValid)
    assert(!AnalyticsLateFactReplayRequest.from("below-count-limit", maximum.take(2), 1).isValid)
    assert(!AnalyticsLateFactReplayRequest.from("control-request\n", Vector(("hiring.events", 0, 1L))).isValid)
    assert(
      !AnalyticsLateFactReplayRequest.from("oversized-id-" + ("x" * 128), Vector(("hiring.events", 0, 1L))).isValid
    )
    assert(!AnalyticsLateFactReplayRequest.from("invalid-topic", Vector(("bad:topic", 0, 1L))).isValid)
    assert(!AnalyticsLateFactReplayRequest.from("invalid-topic", Vector(("bad\ntopic", 0, 1L))).isValid)
  }

  test("late-fact replay selections accumulate independent identity and coordinate errors") {
    val invalid = AnalyticsLateFactReplayRequest.from(" ", Vector(("", -1, -1L)))
    val errors = invalid.toEither.swap.toOption.toList.flatMap(_.toNonEmptyList.toList)
    assert(errors.exists(_.contains("request ID")))
    assert(errors.exists(_.contains("topic")))
    assert(errors.exists(_.contains("partition")))
    assert(errors.exists(_.contains("offset")))
  }

  test("validated ranges remain trusted when constructing manifests") {
    val range = TestPartitionOffsetRange.from("hiring.operational-events", 0, 2L, 7L).toEither.toOption.get
    val runId = RunId.from("valid-run").toOption.get
    val manifest = TestPartitionOffsetRange.manifest(runId, Vector(range))
    assert(manifest.isValid)
    assertEquals(manifest.toEither.toOption.get.offsetRanges, Vector(range))
  }

  test("validated domain values have structural equality") {
    val first = TestPartitionOffsetRange.from("topic", 1, 2L, 3L).toEither.toOption.get
    val second = TestPartitionOffsetRange.from("topic", 1, 2L, 3L).toEither.toOption.get
    val runId = RunId.from("run").toOption.get
    val firstManifest = TestPartitionOffsetRange.manifest(runId, Vector(first)).toEither.toOption.get
    val secondManifest = TestPartitionOffsetRange.manifest(runId, Vector(second)).toEither.toOption.get
    val tokenValue = "key_" + ("a" * 43)
    val firstToken = SubjectToken.fromHmac(tokenValue).toOption.get
    val secondToken = SubjectToken.fromHmac(tokenValue).toOption.get

    assertEquals(first, second)
    assertEquals(first.hashCode(), second.hashCode())
    assertEquals(firstManifest, secondManifest)
    assertEquals(firstManifest.hashCode(), secondManifest.hashCode())
    assertEquals(firstToken, secondToken)
    assertEquals(firstToken.hashCode(), secondToken.hashCode())
    assert(!firstToken.toString.contains(tokenValue))
  }
}
