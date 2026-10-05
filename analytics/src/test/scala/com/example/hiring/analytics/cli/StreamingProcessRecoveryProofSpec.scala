package com.example.hiring.analytics.cli

import munit.CatsEffectSuite
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import org.bson.Document
import scala.jdk.CollectionConverters.*
import com.example.hiring.analytics.adapter.spark.{AnalyticsTableSchemas, SparkBlockingExecution}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.*

final class StreamingProcessRecoveryProofSpec extends CatsEffectSuite {
  test("persisted recovery identity accepts relaxed JSON Int32 batch IDs") {
    val document = Document.parse("""{"lineage":"hiring-recovery","batchId":0}""")
    val identity = StreamingProcessRecoveryProofMain.identity(document)
    assertEquals(identity.lineage.value, "hiring-recovery")
    assertEquals(identity.batchId.value, 0L)
  }

  test("persisted recovery identity accepts extended JSON Int64 batch IDs") {
    val document = Document.parse("""{"lineage":"hiring-recovery","batchId":{"$numberLong":"2147483648"}}""")
    assertEquals(StreamingProcessRecoveryProofMain.identity(document).batchId.value, 2147483648L)
  }

  private val dataId = "data-lock"
  private val streamId = "stream-lock"
  private val token = "4a7dd2a1-6d87-41c1-b8ca-2b3b5bb46d95"
  private def owners(ids: Vector[String]): Document =
    new Document("mutexOwners", ids.map(id => new Document("lockId", id).append("ownerToken", token)).asJava)

  test("recovery requires exactly the two distinct data and lifetime stream owners") {
    val captured = StreamingProcessRecoveryProofMain.capturedMutexes(owners(Vector(dataId, streamId)), dataId, streamId)
    assertEquals(captured.toOption.get.map(_.lockId).toSet, Set(dataId, streamId))
    Vector(Vector(dataId), Vector(dataId, dataId), Vector(dataId, "foreign"), Vector(dataId, streamId, "extra"))
      .foreach(ids => assert(StreamingProcessRecoveryProofMain.capturedMutexes(owners(ids), dataId, streamId).isLeft))
    assert(StreamingProcessRecoveryProofMain.capturedMutexes(owners(Vector(dataId, streamId)), dataId, dataId).isLeft)
  }

  test("missing or malformed captured tokens fail before any release") {
    assert(StreamingProcessRecoveryProofMain.capturedMutexes(new Document(), dataId, streamId).isLeft)
    val malformed = owners(Vector(dataId, streamId))
    malformed.getList("mutexOwners", classOf[Document]).get(1).put("ownerToken", "")
    assert(StreamingProcessRecoveryProofMain.capturedMutexes(malformed, dataId, streamId).isLeft)
  }

  test("actual stream ownership has a distinct canonical mutex identity") {
    val root = "file:///tmp/hiring-recovery-ownership"
    val owner = com.example.hiring.analytics.service.batch.AnalyticsStreamingRegistry.ownerLockRoot(root).toOption.get
    val data = com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock.lockId(root).toOption.get
    val stream = com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock.lockId(owner).toOption.get
    assertNotEquals(data, stream)
    assertEquals(
      StreamingProcessRecoveryProofMain.capturedMutexes(owners(Vector(data, stream)), data, stream).isRight,
      true
    )
  }

  test("mismatched second captured owner prevents both deletes") {
    val pair =
      StreamingProcessRecoveryProofMain.capturedMutexes(owners(Vector(dataId, streamId)), dataId, streamId).toOption.get
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- StreamingProcessRecoveryProofMain
        .removeCapturedMutexes(pair)(
          owner =>
            if (owner.lockId == streamId) IO.raiseError(new IllegalStateException("owner mismatch")) else IO.unit,
          owner => events.update(_ :+ ("delete:" + owner.lockId)),
          owner => events.update(_ :+ ("receipt:" + owner.lockId))
        )
        .attempt
      recorded <- events.get
      _ <- IO { assert(result.isLeft); assertEquals(recorded, Vector.empty[String]) }
    } yield ()
  }

  test("second deletion failure preserves first completion receipt and fails recovery") {
    val pair =
      StreamingProcessRecoveryProofMain.capturedMutexes(owners(Vector(dataId, streamId)), dataId, streamId).toOption.get
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- StreamingProcessRecoveryProofMain
        .removeCapturedMutexes(pair)(
          _ => IO.unit,
          owner =>
            if (owner.lockId == streamId) IO.raiseError(new IllegalStateException("delete failed"))
            else events.update(_ :+ ("delete:" + owner.lockId)),
          owner => events.update(_ :+ ("receipt:" + owner.lockId))
        )
        .attempt
      recorded <- events.get
      _ <- IO { assert(result.isLeft); assertEquals(recorded, Vector("delete:" + dataId, "receipt:" + dataId)) }
    } yield ()
  }

  test("inspection never writes acceptance snapshot names") {
    assertEquals(StreamingProcessRecoveryProofMain.snapshotFile("inspect"), "inspection.json")
    assertEquals(StreamingProcessRecoveryProofMain.snapshotFile("recover"), "recovered.json")
  }

  test("diagnostics expose only bounded stage and class and never exception message or nested payload") {
    val failure = new IllegalStateException(
      "mongodb://private-password@example.invalid secret payload",
      new IllegalArgumentException("private subject")
    )
    assertEquals(
      StreamingProcessRecoveryProofMain
        .failureDiagnostic(StreamingProcessRecoveryProofMain.DiagnosticStage.Report, failure),
      "STREAMING_PROCESS_RECOVERY_FAILED stage=Report errorClass=IllegalStateException"
    )
    assertEquals(
      StreamingProcessRecoveryProofMain.failureDiagnostic(
        StreamingProcessRecoveryProofMain.DiagnosticStage.Coordinates,
        StreamingProcessRecoveryProofMain.InspectionMismatch(StreamingProcessRecoveryProofMain.DiagnosticStage.Counts)
      ),
      "STREAMING_PROCESS_RECOVERY_FAILED stage=Counts errorClass=InspectionMismatch"
    )
  }

  private def withSpark(check: SparkSession => Unit): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      Resource
        .make(execution {
          SparkSession
            .builder()
            .master("local[2]")
            .appName("HiringRecoveryAdmissionEvidence")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        })(spark => execution.blocking(spark.stop()))
        .use { spark =>
          execution.attachSparkContext(spark.sparkContext) *> execution(check(spark))
        }
    }

  test("native Silver schema admission evidence uses event identity and fingerprint without Kafka coordinates") {
    withSpark { spark =>
      assert(!AnalyticsTableSchemas.silver.map(_._1).exists(Set("topic", "partition", "offset")))
      val expected = Set(
        StreamingProcessRecoveryProofMain.AdmittedIdentity("seed-a", "fingerprint-a"),
        StreamingProcessRecoveryProofMain.AdmittedIdentity("seed-b", "fingerprint-b")
      )
      def frame(values: Vector[(String, String)]) = spark.createDataFrame(
        values.map { case (id, fingerprint) => Row(id, fingerprint) }.asJava,
        StructType(
          Vector(StructField("eventId", StringType, false), StructField("eventFingerprint", StringType, false))
        )
      )
      val positive = StreamingProcessRecoveryProofMain.admittedIdentities(
        frame(Vector("seed-a" -> "fingerprint-a", "seed-b" -> "fingerprint-b", "unrelated" -> "outside")),
        expected
      )
      StreamingProcessRecoveryProofMain.requireAdmittedIdentities(positive, expected)
      Vector(
        Vector("seed-a" -> "wrong", "seed-b" -> "fingerprint-b"),
        Vector("seed-a" -> "fingerprint-a"),
        Vector("seed-a" -> "fingerprint-a", "seed-a" -> "fingerprint-a", "seed-b" -> "fingerprint-b")
      ).foreach { values =>
        val invalid = StreamingProcessRecoveryProofMain.admittedIdentities(frame(values), expected)
        intercept[IllegalArgumentException](
          StreamingProcessRecoveryProofMain.requireAdmittedIdentities(invalid, expected)
        )
      }
      intercept[IllegalArgumentException](
        StreamingProcessRecoveryProofMain.requireAdmittedIdentities(positive ++ positive.take(1), expected)
      )
    }
  }

  test("recovery and inspection require complete coordinate-bound seed identities") {
    val entries = (0 until 12)
      .map(index => new Document("partition", Int.box(index % 3)).append("offset", Long.box(index / 3)))
      .toVector
    val seed = new Document("coordinates", entries.asJava)
    intercept[IllegalArgumentException](StreamingProcessRecoveryProofMain.seedIdentityMapping(seed))
    entries.head.append("eventId", "seed-one")
    intercept[IllegalArgumentException](StreamingProcessRecoveryProofMain.seedIdentityMapping(seed))
    entries.zipWithIndex.foreach { case (entry, index) =>
      entry.append("eventId", s"seed-$index").append("eventFingerprint", s"fingerprint-$index")
    }
    assertEquals(StreamingProcessRecoveryProofMain.seedIdentityMapping(seed).size, 12)
  }

  private def nativeSnapshot: Document = new Document("nonce", "1234567890abcdef")
    .append("batchId", Long.box(0L))
    .append("bronze", "bronze-digest:12")
    .append("silver", "silver-digest:12")
    .append("lateFacts", "late-digest:0")
    .append("lateIdentities", "late-identity-digest:0")
    .append("watermark", "2026-10-03T12:00:00Z")
    .append("outcome", "Published")
    .append("publicationGeneration", Long.box(0L))
    .append("publicationRevision", Long.box(1L))
    .append("publicationReceipt", "CurrentGeneration")
    .append("reportFingerprint", "report-digest")
    .append("optional", null)
    .append("flag", Boolean.box(false))

  test("native snapshot equality preserves relaxed JSON numeric round trips") {
    val observed = nativeSnapshot
    val recorded = Document.parse(observed.toJson)
    assert(recorded.get("batchId").isInstanceOf[java.lang.Integer])
    assert(observed.get("batchId").isInstanceOf[java.lang.Long])
    assert(recorded != observed, "raw BSON numeric widths reproduce the persisted snapshot failure")
    assert(StreamingProcessRecoveryProofMain.sameSnapshot(recorded, observed))
    assert(StreamingProcessRecoveryProofMain.sameSnapshot(observed, recorded))
    val large = nativeSnapshot.append("batchId", Long.box(2147483648L))
    assert(StreamingProcessRecoveryProofMain.sameSnapshot(Document.parse(large.toJson), large))
    val maximum = nativeSnapshot.append("batchId", Long.box(Long.MaxValue))
    assert(StreamingProcessRecoveryProofMain.sameSnapshot(Document.parse(maximum.toJson), maximum))
    assert(!StreamingProcessRecoveryProofMain.sameSnapshot(observed, nativeSnapshot.append("batchId", "0")))
  }

  test("native snapshot equality rejects every changed field, count, missing key, extra key and null difference") {
    val recorded = nativeSnapshot
    Vector[(String, AnyRef)](
      "nonce" -> "foreign",
      "batchId" -> Long.box(1L),
      "bronze" -> "bronze-other:12",
      "bronze" -> "bronze-digest:13",
      "silver" -> "silver-other:12",
      "silver" -> "silver-digest:11",
      "lateFacts" -> "late-other:0",
      "lateFacts" -> "late-digest:1",
      "lateIdentities" -> "changed:0",
      "watermark" -> "2026-10-03T13:00:00Z",
      "watermark" -> null,
      "publicationGeneration" -> Long.box(1L),
      "publicationRevision" -> Long.box(2L),
      "outcome" -> "ErasurePending",
      "publicationReceipt" -> "Superseded",
      "reportFingerprint" -> "changed",
      "optional" -> Boolean.box(false),
      "flag" -> null
    ).foreach { case (key, value) =>
      assert(!StreamingProcessRecoveryProofMain.sameSnapshot(recorded, nativeSnapshot.append(key, value)), key)
    }
    recorded.keySet().asScala.foreach { key =>
      val missing = nativeSnapshot
      missing.remove(key)
      assert(!StreamingProcessRecoveryProofMain.sameSnapshot(recorded, missing), key)
    }
    assert(!StreamingProcessRecoveryProofMain.sameSnapshot(recorded, nativeSnapshot.append("extra", null)))
  }

  test("snapshot normalization preserves nested fields, array order and noninteger numeric types") {
    val recorded = nativeSnapshot.append(
      "details",
      Vector(new Document("count", Long.box(2L)), new Document("count", Long.box(3L))).asJava
    )
    assert(StreamingProcessRecoveryProofMain.sameSnapshot(Document.parse(recorded.toJson), recorded))
    val reordered = nativeSnapshot.append(
      "details",
      Vector(new Document("count", Long.box(3L)), new Document("count", Long.box(2L))).asJava
    )
    assert(!StreamingProcessRecoveryProofMain.sameSnapshot(recorded, reordered))
    assert(
      !StreamingProcessRecoveryProofMain.sameSnapshot(nativeSnapshot, nativeSnapshot.append("batchId", Double.box(0.0)))
    )
  }

}
