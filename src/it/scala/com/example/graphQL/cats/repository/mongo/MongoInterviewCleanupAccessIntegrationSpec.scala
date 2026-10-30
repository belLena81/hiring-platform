package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, Sorts, Updates}
import io.circe.Json
import io.circe.parser.parse
import mongo4cats.collection.MongoCollection
import org.bson.Document
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

/** Paired bounded local workload: dense completed evidence must not make active selection scan that evidence. */
final class MongoInterviewCleanupAccessIntegrationSpec extends MongoIntegrationSuite {
  // Physical footprint comparisons flush the entire server; own that server exclusively.
  override protected def dedicatedMongo: Boolean = true
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val completedCount = 10000
  private val activeCount = 33

  // A single before block followed by a single after block compares ~2 ms p95 values taken minutes apart, so drift
  // (GC, neighbouring load, cache state) is indistinguishable from index cost. The index is therefore toggled in many
  // short alternating rounds (without/with, with/without, ...) so load bursts hit both states equally, and the gate
  // compares the write-pair p95 pooled over all rounds (2000 samples per state). The agreed 10% ceiling is unchanged,
  // with no absolute noise floor; the per-round p95 values and raw samples are written to paired.json.
  private val rounds = 20
  private val writeSamplesPerRound = 100
  private val readSamplesPerRound = 10
  private val warmup = 30
  private val readWarmup = 5
  private val writeRatioCeiling = 1.10

  private def row(ordinal: Int, at: Instant): Document = {
    val value = new Document("_id", new UUID(0L, ordinal.toLong).toString)
      .append("revision", Long.box(0L))
      .append("requestedAt", Date.from(at))
      .append("producerRegistry", true)
      .append("interviewTransactionalIds", java.util.List.of[String]())
      .append("state", if (ordinal <= completedCount) "Complete" else "Pending")
    if (ordinal <= completedCount) value.append("completedAt", Date.from(at)) else value
  }

  private def percentile(values: List[Long], fraction: Double): Long =
    values.sorted.lift((values.size * fraction).ceil.toInt - 1).getOrElse(0L)
  private def p95(values: List[Long]): Long = percentile(values, 0.95)
  private def p99(values: List[Long]): Long = percentile(values, 0.99)
  private def median(values: List[Double]): Double = {
    val sorted = values.sorted
    val middle = sorted.size / 2
    if (sorted.size % 2 == 1) sorted(middle) else (sorted(middle - 1) + sorted(middle)) / 2.0
  }
  private def throughput(values: List[Long]): Json =
    Json.fromDoubleOrNull(values.size.toDouble * 1e9 / values.sum.toDouble)
  private def selectedStages(explain: Json): List[String] = {
    val selected = for {
      winning <- explain.hcursor.downField("queryPlanner").get[Json]("winningPlan")
      executed <- explain.hcursor.downField("executionStats").get[Json]("executionStages")
    } yield List(winning, executed).flatMap(_.findAllByKey("stage")).flatMap(_.asString)
    selected.fold(error => fail(s"Missing selected explain plan: ${error.getMessage}"), identity)
  }
  private def measured(effect: IO[Unit], warmups: Int, samples: Int): IO[List[Long]] =
    List.fill(warmups)(effect).sequence_ *>
      List
        .fill(samples)(IO.monotonic.flatMap(start => effect *> IO.monotonic.map(end => (end - start).toNanos)))
        .sequence

  /** Raw samples of one round; "before" runs without the active-state index, "after" with it. */
  private final case class Round(
      beforeReads: List[Long],
      afterReads: List[Long],
      beforeWrites: List[Long],
      afterWrites: List[Long]
  ) {
    def writeRatio: Double = p95(afterWrites).toDouble / p95(beforeWrites).toDouble
  }

  private def footprint(database: mongo4cats.database.MongoDatabase[IO]): IO[Json] =
    MongoAccessEvaluationSupport
      .command(database, new Document("collStats", MongoCollections.InterviewSubjectCleanup))
      .flatMap(result => IO.fromEither(parse(result.toJson).leftMap(error => new AssertionError(error.getMessage))))

  private def indexBytes(footprint: Json): Long =
    footprint.hcursor.get[Long]("totalIndexSize").fold(error => fail(error.getMessage), identity)

  private def createActiveIndex(queue: MongoCollection[IO, Document]): IO[Unit] =
    queue
      .createIndex(
        Indexes.ascending("_id", "requestedAt"),
        new IndexOptions()
          .name(MongoInterviewCleanupSweepCodec.ActiveIndex)
          .partialFilterExpression(MongoInterviewCleanupSweepCodec.activeFilter)
      )
      .void

  private def writeWork(queue: MongoCollection[IO, Document]): IO[Unit] = {
    val activeId = new UUID(0L, (completedCount + 1).toLong).toString
    queue.updateOne(Filters.eq("_id", activeId), Updates.set("state", "ProducersFenced")).void *>
      queue.updateOne(Filters.eq("_id", activeId), Updates.set("state", "Pending")).void
  }

  test("paired sparse cleanup workload reduces examined records within write and index cost ceilings") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val through = new org.bson.BsonString(new UUID(0L, (completedCount + activeCount).toLong).toString)
      val sweep = MongoInterviewCleanupSweepCodec.Sweep(None, through, at.plusSeconds(1))
      val active = Document.parse(
        MongoInterviewCleanupSweepCodec
          .pageFilter(sweep)
          .toBsonDocument(classOf[Document], com.mongodb.MongoClientSettings.getDefaultCodecRegistry)
          .toJson
      )
      val legacy = Document.parse(active.toJson)
      val _ = legacy.getList("$and", classOf[Document]).get(0).put("state", new Document("$ne", "Complete"))
      val flush = fixture.client.getDatabase("admin").flatMap { admin =>
        MongoAccessEvaluationSupport.command(admin, new Document("fsync", 1).append("lock", false)).void
      }
      def explain(query: Document, hint: Option[String]): IO[Json] = {
        val command = new Document("find", MongoCollections.InterviewSubjectCleanup)
          .append("filter", query)
          .append("sort", new Document("_id", 1))
          .append("limit", 32)
        hint.foreach(value => { val _ = command.append("hint", value) })
        MongoAccessEvaluationSupport
          .command(fixture.database, new Document("explain", command).append("verbosity", "executionStats"))
          .flatMap(value => IO.fromEither(parse(value.toJson).leftMap(error => new AssertionError(error.getMessage))))
      }

      def read(queue: MongoCollection[IO, Document], query: Document, hint: String): IO[Unit] =
        queue
          .find(query)
          .sort(Sorts.ascending("_id"))
          .hint(hint)
          .limit(32)
          .all
          .flatMap(rows => IO(assertEquals(rows.size, 32)))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- (1 to completedCount + activeCount)
          .grouped(500)
          .toList
          .traverse_(batch => queue.insertMany(batch.map(row(_, at)).toList).void)
        _ <- queue.dropIndex(MongoInterviewCleanupSweepCodec.ActiveIndex)
        _ <- flush
        beforePreparedFootprint <- footprint(fixture.database)
        beforePlan <- explain(legacy, Some("_id_"))
        _ <- createActiveIndex(queue)
        _ <- flush
        afterPreparedFootprint <- footprint(fixture.database)
        afterPlan <- explain(active, Some(MongoInterviewCleanupSweepCodec.ActiveIndex))
        unhintedPlan <- explain(active, None)
        indexPresent <- IO.ref(true)
        setIndex = (wanted: Boolean) =>
          indexPresent.get.flatMap { present =>
            if (present == wanted) IO.unit
            else if (wanted) createActiveIndex(queue) *> indexPresent.set(true)
            else queue.dropIndex(MongoInterviewCleanupSweepCodec.ActiveIndex).void *> indexPresent.set(false)
          }
        phase = (withIndex: Boolean) =>
          setIndex(withIndex) *> (for {
            reads <- measured(
              read(
                queue,
                if (withIndex) active else legacy,
                if (withIndex) MongoInterviewCleanupSweepCodec.ActiveIndex else "_id_"
              ),
              readWarmup,
              readSamplesPerRound
            )
            writes <- measured(writeWork(queue), warmup, writeSamplesPerRound)
          } yield (reads, writes))
        resourcesStart <- fixture.sampleResources
        roundResults <- (0 until rounds).toList.traverse { round =>
          // Alternate which state goes first so monotonic drift cannot favour either side.
          if (round % 2 == 0)
            phase(false).flatMap((br, bw) => phase(true).map((ar, aw) => Round(br, ar, bw, aw)))
          else phase(true).flatMap((ar, aw) => phase(false).map((br, bw) => Round(br, ar, bw, aw)))
        }
        resourcesEnd <- fixture.sampleResources
        // Final physical footprints after the whole write workload, without and with the index.
        _ <- setIndex(false)
        _ <- flush
        beforeFootprint <- footprint(fixture.database)
        _ <- setIndex(true)
        _ <- flush
        afterFootprint <- footprint(fixture.database)
        beforeReads = roundResults.flatMap(_.beforeReads)
        afterReads = roundResults.flatMap(_.afterReads)
        beforeWrites = roundResults.flatMap(_.beforeWrites)
        afterWrites = roundResults.flatMap(_.afterWrites)
        roundRatios = roundResults.map(_.writeRatio)
        medianRatio = median(roundRatios)
        pooledRatio = p95(afterWrites).toDouble / p95(beforeWrites).toDouble
        beforeIndexBytes = indexBytes(beforeFootprint)
        afterIndexBytes = indexBytes(afterFootprint)
        report = Json.obj(
          "environment" -> Json.obj(
            "mongo" -> Json.fromString("8.0.32"),
            "java" -> Json.fromString(System.getProperty("java.version")),
            "concurrency" -> Json.fromInt(1),
            "completedRows" -> Json.fromInt(completedCount),
            "activeRows" -> Json.fromInt(activeCount),
            "rounds" -> Json.fromInt(rounds),
            "warmupPerPhase" -> Json.fromInt(warmup),
            "writePairSamplesPerPhase" -> Json.fromInt(writeSamplesPerRound),
            "readSamplesPerPhase" -> Json.fromInt(readSamplesPerRound),
            "errors" -> Json.fromInt(0),
            "mongoBoundary" -> Json.fromString("dedicated replica set"),
            "storageMeasurement" -> Json.fromString(
              "native fsync(lock=false) before each prepared/end-workload collStats; no compaction"
            ),
            "writeGate" -> Json.fromString(
              s"write-pair p95 pooled over alternating-order rounds, with/without index <= $writeRatioCeiling"
            ),
            "wiredTigerCacheBytes" -> Json.fromLong(256L * 1024L * 1024L),
            "mongoMemoryLimitBytes" -> Json.fromLong(3L * 1024L * 1024L * 1024L),
            "mongoTmpfsCapacityBytes" -> Json.fromLong(2L * 1024L * 1024L * 1024L),
            "resourceBoundary" -> Json.fromString(
              "Mongo container boundary samples include warmup/probe/background activity; memory peaks between samples unknown"
            )
          ),
          "resourcesStart" -> resourcesStart,
          "resourcesEnd" -> resourcesEnd,
          "rounds" -> Json.fromValues(roundResults.zipWithIndex.map { (round, index) =>
            Json.obj(
              "round" -> Json.fromInt(index),
              "firstState" -> Json.fromString(if (index % 2 == 0) "without-index" else "with-index"),
              "beforeWritePairP95Nanos" -> Json.fromLong(p95(round.beforeWrites)),
              "afterWritePairP95Nanos" -> Json.fromLong(p95(round.afterWrites)),
              "writeRatio" -> Json.fromDoubleOrNull(round.writeRatio),
              "beforeWritePairSamplesNanos" -> Json.fromValues(round.beforeWrites.map(Json.fromLong)),
              "afterWritePairSamplesNanos" -> Json.fromValues(round.afterWrites.map(Json.fromLong))
            )
          }),
          "before" -> Json.obj(
            "indexBytes" -> Json.fromLong(beforeIndexBytes),
            "preparedFootprint" -> beforePreparedFootprint,
            "workloadFootprint" -> beforeFootprint,
            "readP95Nanos" -> Json.fromLong(p95(beforeReads)),
            "readP99Nanos" -> Json.fromLong(p99(beforeReads)),
            "readMeasuredThroughput" -> throughput(beforeReads),
            "writePairP95Nanos" -> Json.fromLong(p95(beforeWrites)),
            "writePairP99Nanos" -> Json.fromLong(p99(beforeWrites)),
            "writePairMeasuredThroughput" -> throughput(beforeWrites),
            "explain" -> beforePlan
          ),
          "after" -> Json.obj(
            "indexBytes" -> Json.fromLong(afterIndexBytes),
            "preparedFootprint" -> afterPreparedFootprint,
            "workloadFootprint" -> afterFootprint,
            "readP95Nanos" -> Json.fromLong(p95(afterReads)),
            "readP99Nanos" -> Json.fromLong(p99(afterReads)),
            "readMeasuredThroughput" -> throughput(afterReads),
            "writePairP95Nanos" -> Json.fromLong(p95(afterWrites)),
            "writePairP99Nanos" -> Json.fromLong(p99(afterWrites)),
            "writePairMeasuredThroughput" -> throughput(afterWrites),
            "explain" -> afterPlan,
            "unhintedExplain" -> unhintedPlan
          ),
          "writeRegressionRatio" -> Json.fromDoubleOrNull(pooledRatio),
          "medianPerRoundWriteRatio" -> Json.fromDoubleOrNull(medianRatio)
        )
        _ <- IO.blocking {
          val directory = Paths.get(".local/data/cleanup-access-evaluation")
          val _ = Files.createDirectories(directory)
          val _ = Files.writeString(directory.resolve("paired.json"), report.spaces2)
        }
        _ <- IO {
          assertEquals(beforePlan.hcursor.downField("executionStats").get[Long]("nReturned"), Right(32L))
          assertEquals(afterPlan.hcursor.downField("executionStats").get[Long]("nReturned"), Right(32L))
          assert(
            beforePlan.hcursor.downField("executionStats").get[Long]("totalDocsExamined").exists(_ >= completedCount)
          )
          assert(afterPlan.hcursor.downField("executionStats").get[Long]("totalDocsExamined").exists(_ <= activeCount))
          assert(!selectedStages(afterPlan).contains("SORT"))
          assert(
            unhintedPlan.hcursor.downField("executionStats").get[Long]("totalDocsExamined").exists(_ <= activeCount)
          )
          assert(!selectedStages(unhintedPlan).contains("SORT"))
          assert(afterIndexBytes <= beforeIndexBytes * 1.25)
        }
        perRound = roundResults
          .map(round => f"${p95(round.beforeWrites) / 1e6}%.3f->${p95(round.afterWrites) / 1e6}%.3f")
          .mkString(", ")
        _ <- IO.println(
          s"Cleanup paired workload: read p95 ${p95(beforeReads)} -> ${p95(afterReads)} ns; pooled write pair p95 ${p95(beforeWrites)} -> ${p95(afterWrites)} ns; pooled ratio $pooledRatio; median per-round ratio $medianRatio; per-round write pair p95 ms [$perRound]; index bytes $beforeIndexBytes -> $afterIndexBytes"
        )
        _ <- IO(
          assert(
            pooledRatio <= writeRatioCeiling,
            s"Measured write p95 regression exceeded 10%: pooled ratio $pooledRatio (median per-round $medianRatio); paired.json retains raw samples"
          )
        )
        selected <- new MongoInterviewSubjectCleanup(fixture.database).pendingPage(None, at.plusSeconds(1)).value
        _ <- IO(assertEquals(selected.toOption.map(_.entries.size), Some(32)))
      } yield ()
    }
  }
}
