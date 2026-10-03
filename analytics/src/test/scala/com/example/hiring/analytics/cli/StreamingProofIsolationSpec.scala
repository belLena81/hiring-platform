package com.example.hiring.analytics.cli

final class StreamingProofIsolationSpec extends munit.FunSuite {
  private val nonce = "0123456789abcdef"
  private val database = "hiring_streaming_proof_" + nonce
  private val topic = "hiring.streaming.proof." + nonce
  private val namespace = "/tmp/hiring-streaming-proof-" + nonce
  private val checkpointPath =
    "/tmp/project/.local/data/analytics/checkpoints/hiring-streaming-proof-" + nonce + "/query"
  private val spillPath = "/tmp/project/.local/data/analytics/spark-temp/hiring-streaming-proof-" + nonce + "/driver"
  private def validate(
      db: String = database,
      source: String = topic,
      root: String = namespace + "/lakehouse",
      checkpoint: String = checkpointPath,
      spill: String = spillPath
  ) =
    StreamingProofIsolation.validate(db, source, root, checkpoint, spill)

  test("isolated source and owned filesystem namespace must share the exact nonce") {
    assert(validate().isRight)
    assert(validate(root = "file://" + namespace + "/lakehouse", checkpoint = "file://" + checkpointPath).isRight)
    assert(
      validate(
        checkpoint = "/var/lib/hiring-analytics/checkpoints/hiring-streaming-proof-" + nonce + "/query",
        spill = "/var/lib/hiring-analytics/spark-temp/hiring-streaming-proof-" + nonce + "/driver"
      ).isRight
    )
    assert(validate(db = "hiring").isLeft)
    assert(validate(source = "hiring.operational-events").isLeft)
    assert(validate(source = "hiring.streaming.proof.fedcba9876543210").isLeft)
    assert(validate(root = "/var/lib/hiring-analytics/lakehouse").isLeft)
    assert(validate(checkpoint = "/var/lib/hiring-analytics/checkpoint").isLeft)
    assert(validate(spill = "/var/lib/hiring-analytics/spark-temp").isLeft)
    assert(validate(checkpoint = checkpointPath.replace(nonce, "fedcba9876543210")).isLeft)
    assert(validate(spill = spillPath.replace(nonce, "fedcba9876543210")).isLeft)
    assert(validate(root = "s3://production/lakehouse").isLeft)
    assert(validate(root = "file://another-host" + namespace + "/lakehouse").isLeft)
  }

  test("an activation review cannot omit changed files, exclude files or approve its author's own scope") {
    val scope = Vector("src/main/scala/Report.scala", "src/test/scala/ReportSpec.scala")
    assert(StreamingProofIsolation.independentCoverage(scope, scope, Vector.empty, Vector.empty))
    assert(!StreamingProofIsolation.independentCoverage(scope, scope.take(1), Vector.empty, Vector.empty))
    assert(!StreamingProofIsolation.independentCoverage(scope, scope, scope.take(1), Vector.empty))
    assert(!StreamingProofIsolation.independentCoverage(scope, scope, Vector.empty, scope.take(1)))
    assert(!StreamingProofIsolation.independentCoverage(Vector.empty, scope, Vector.empty, Vector.empty))
  }

  test("a role may aggregate independent cross reviews while each author excludes their own files") {
    val first = Vector("src/main/scala/Reports.scala")
    val second = Vector("src/main/scala/Erasure.scala")
    assert(
      StreamingProofIsolation.independentCoverageUnion(
        first ++ second,
        Vector((first, second, second), (second, first, first))
      )
    )
    assert(!StreamingProofIsolation.independentCoverageUnion(first ++ second, Vector((first, second, second))))
    assert(
      !StreamingProofIsolation.independentCoverageUnion(
        first ++ second,
        Vector((first, Vector.empty, first), (second, Vector.empty, Vector.empty))
      )
    )
  }

  test("active shutdown accepts only the exact workload database and state nonce") {
    assertEquals(StreamingActiveTerminationProofMain.validateFixtureNonce(database, nonce, nonce), Right(()))
  }

  test("active shutdown rejects a different database before any native client or producer") {
    assert(
      StreamingActiveTerminationProofMain
        .validateFixtureNonce("hiring_streaming_proof_fedcba9876543210", nonce, nonce)
        .isLeft
    )
    assert(StreamingActiveTerminationProofMain.validateFixtureNonce("hiring", nonce, nonce).isLeft)
  }

  test("active shutdown rejects a different or missing state nonce") {
    assert(StreamingActiveTerminationProofMain.validateFixtureNonce(database, nonce, "fedcba9876543210").isLeft)
    assert(StreamingActiveTerminationProofMain.validateFixtureNonce(database, nonce, null).isLeft)
  }

  test("active shutdown rejects malformed workload nonces even when the other fields agree") {
    Vector("", "0123456789abcde", "0123456789abcdef0", "0123456789abcdeF", "../fixture").foreach { invalid =>
      assert(
        StreamingActiveTerminationProofMain
          .validateFixtureNonce("hiring_streaming_proof_" + invalid, invalid, invalid)
          .isLeft
      )
    }
    assert(StreamingActiveTerminationProofMain.validateFixtureNonce("hiring_streaming_proof_null", null, null).isLeft)
  }
}
