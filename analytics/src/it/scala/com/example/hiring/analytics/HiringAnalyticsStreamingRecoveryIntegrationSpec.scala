package com.example.hiring.analytics

import cats.effect.IO
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.streaming.*
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, encode, lit}
import org.apache.spark.sql.types.*
import org.bson.Document

import java.time.Instant
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Actual Delta/Mongo recovery; broker admission, Spark checkpoint ownership and process restart have separate proofs.
  */
final class HiringAnalyticsStreamingRecoveryIntegrationSpec extends AnalyticsMongoIntegrationSuite {
  private def resource = HiringAnalyticsRecoveryTestSupport.resource(mongoEndpoint)

  override val munitIOTimeout: FiniteDuration = 15.minutes

  test("reconstructed coordinator converges after Bronze, Silver, Gold and committed Mongo boundary failures") {
    resource
      .use { runtime =>
        Vector("bronze", "silver", "gold", "mongo").traverse_ { boundary =>
          for {
            harness <- runtime.harness("stream-" + boundary)
            frame <- harness.parsed()
            once = new AtomicBoolean(true)
            sink = new DeltaWriter[IO] {
              override def merge(source: DataFrame, path: String, condition: String) =
                harness.writer.merge(source, path, condition) *> IO.defer {
                  val selected = (boundary == "bronze" && path == harness.paths.bronze) ||
                    (boundary == "silver" && path == harness.paths.silver)
                  if (selected && once.compareAndSet(true, false)) IO.raiseError(InjectedFailure) else IO.unit
                }
              override def mergeWhenFresh(source: DataFrame, path: String, condition: String, at: () => Instant) =
                harness.writer.mergeWhenFresh(source, path, condition, at)
              override def withExpiry(frame: DataFrame, at: Instant, days: Int) =
                harness.writer.withExpiry(frame, at, days)
            }
            publication = new DelegatingPublisher(harness.publisher) {
              override def publish(
                  reservation: AnalyticsReportReservation,
                  report: AnalyticsReportOutput,
                  expiry: Instant
              ) =
                if (boundary == "gold") IO.defer {
                  if (once.compareAndSet(true, false)) IO.raiseError(InjectedFailure)
                  else super.publish(reservation, report, expiry)
                }
                else
                  super.publish(reservation, report, expiry) *> IO.defer {
                    if (boundary == "mongo" && once.compareAndSet(true, false)) IO.raiseError(InjectedFailure)
                    else IO.unit
                  }
            }
            failed <- harness.process(frame, sink, publication).attempt
            _ = assertEquals(failed.left.toOption, Some(InjectedFailure), boundary)
            pending <- harness.journal.load(harness.preparation().identity)
            _ = assert(pending.exists(_.terminalOutcome.isEmpty), boundary)
            originalCandidate = pending.flatMap(_.latestDecision).flatMap(_.candidateWatermark)
            _ = assert(originalCandidate.nonEmpty, boundary)
            before <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
            _ = assertEquals(before, None, boundary)
            // Each call creates fresh journal, stage and coordinator objects over the same durable sinks.
            recovered <- harness.process(frame)
            duplicate <- harness.process(frame)
            _ = assertEquals(recovered.outcome, StreamingTerminalOutcome.Published, boundary)
            _ = assertEquals(duplicate, recovered, boundary)
            watermark <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
            _ = assertEquals(watermark, originalCandidate, boundary)
            bronze <- harness.count(harness.paths.bronze)
            silver <- harness.count(harness.paths.silver)
            _ = assertEquals(bronze, 12L, boundary)
            _ = assertEquals(silver, 12L, boundary)
            progress <- harness.count(harness.paths.streamingProgress)
            _ = assertEquals(progress, 1L, boundary)
            published <- harness.sync(
              _.getCollection("analytics_report_snapshots").find(new Document("_id", "current")).first()
            )
            _ = assertEquals(published.getString("state"), "Published", boundary)
            completed <- harness.journal.load(harness.preparation().identity)
            receipt <- harness.publisher.publicationReceipt(completed.get.latestDecision.get.publicationReservation)
            _ = assertEquals(receipt, AnalyticsReportPublicationReceipt.CurrentGeneration, boundary)
          } yield ()
        }
      }
      .unsafeRunSync()
  }

  test("a completed deletion generation with unchanged marker view forces a fresh pinned publication") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-generation")
          frame <- harness.parsed()
          once = new AtomicBoolean(true)
          publication = new DelegatingPublisher(harness.publisher) {
            override def publish(
                reservation: AnalyticsReportReservation,
                report: AnalyticsReportOutput,
                expiry: Instant
            ) =
              IO.defer {
                if (once.compareAndSet(true, false)) harness.sync { database =>
                  database
                    .getCollection("analytics_report_control")
                    .updateOne(
                      new Document("_id", "analytics-report"),
                      new Document("$inc", new Document("generation", 1L))
                    )
                }.void
                else IO.unit
              } *> super.publish(reservation, report, expiry)
          }
          result <- harness.process(frame, publication = publication)
          _ = assertEquals(result.outcome, StreamingTerminalOutcome.Published)
          state <- harness.journal.load(harness.preparation().identity)
          _ = assertEquals(state.flatMap(_.latestDecision).map(_.revision), Some(1L))
          _ = assertEquals(state.flatMap(_.latestDecision).map(_.publicationReservation.generation), Some(1L))
          revisions <- harness.execution(
            harness.spark.read
              .format("delta")
              .load(harness.paths.streamingDecisions)
              .select("publicationGeneration")
              .collect()
              .map(_.getLong(0))
              .toSet
          )
          _ = assertEquals(revisions, Set(0L, 1L))
          silver <- harness.count(harness.paths.silver)
          _ = assertEquals(silver, 12L)
        } yield ()
      }
      .unsafeRunSync()
  }

  test("mixed new facts and a newer preexisting duplicate preserve only the original eligible watermark on retry") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-mixed-duplicate-recovery")
          freshTime = harness.at.minusSeconds(600L).truncatedTo(ChronoUnit.MICROS)
          duplicateTime = harness.at.plusSeconds(15L).truncatedTo(ChronoUnit.MICROS)
          frame <- harness.parsed(eventTimes = duplicateTime +: Vector.fill(11)(freshTime))
          _ <- harness.lock.resource(harness.paths.root).use { _ =>
            harness.maintenance.validateHmacConfigurationLocked *> harness.execution
              .either {
                val existing = frame.filter(col("partition") === 0 && col("offset") === 0L)
                val valid = OperationalEventTransforms.validEvents(existing)
                val markers = AnalyticsSubjectPrivacy.emptyMarkers(frame)
                OperationalEventTransforms.silver(valid, harness.keys, markers)
              }
              .flatMap(silver =>
                harness.writer.merge(
                  harness.writer.withExpiry(silver, harness.at, 30),
                  harness.paths.silver,
                  "target.eventId = source.eventId"
                )
              )
          }
          once = new AtomicBoolean(true)
          sink = new DeltaWriter[IO] {
            override def merge(source: DataFrame, path: String, condition: String) =
              harness.writer.merge(source, path, condition) *> IO.defer {
                if (path == harness.paths.silver && once.compareAndSet(true, false)) IO.raiseError(InjectedFailure)
                else IO.unit
              }
            override def mergeWhenFresh(source: DataFrame, path: String, condition: String, at: () => Instant) =
              harness.writer.mergeWhenFresh(source, path, condition, at)
            override def withExpiry(frame: DataFrame, at: Instant, days: Int) =
              harness.writer.withExpiry(frame, at, days)
          }
          failed <- harness.process(frame, sink).attempt
          _ = assertEquals(failed.left.toOption, Some(InjectedFailure))
          before <- harness.journal.load(harness.preparation().identity)
          expected = Some(freshTime.minusSeconds(86400L))
          _ = assertEquals(before.flatMap(_.latestDecision).flatMap(_.candidateWatermark), expected)
          recovered <- harness.process(frame)
          _ = assertEquals(recovered.candidateWatermark, expected)
          watermark <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
          _ = assertEquals(watermark, expected)
          facts <- harness.count(harness.paths.silver)
          _ = assertEquals(facts, 12L)
        } yield ()
      }
      .unsafeRunSync()
  }

  test("changed deletion markers keep actual publication hidden and do not advance watermark") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-marker")
          frame <- harness.parsed()
          once = new AtomicBoolean(true)
          publication = new DelegatingPublisher(harness.publisher) {
            override def reservePinned(run: RunId, fingerprint: RangeFingerprint, at: Instant) =
              super.reservePinned(run, fingerprint, at).flatTap { _ =>
                IO.defer {
                  if (once.compareAndSet(true, false)) harness.sync { database =>
                    database
                      .getCollection("analytics_erasure_requests")
                      .insertOne(
                        new Document("_id", harness.subjects.head)
                          .append("state", "Pending")
                      )
                    database
                      .getCollection("analytics_report_control")
                      .updateOne(
                        new Document("_id", "analytics-report"),
                        new Document("$set", new Document("generation", 1L).append("state", "Hidden"))
                      )
                  }.void
                  else IO.unit
                }
              }
          }
          result <- harness.process(frame, publication = publication)
          _ = assertEquals(result.outcome, StreamingTerminalOutcome.ErasurePending)
          watermark <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
          _ = assertEquals(watermark, None)
          snapshots <- harness.sync(_.getCollection("analytics_report_snapshots").countDocuments())
          _ = assertEquals(snapshots, 0L)
          oldToken = AnalyticsTestSubjectPseudonymizer.tokenValue(harness.keys, harness.subjects.head)
          forbidden <- harness.execution(
            harness.spark.read
              .format("delta")
              .load(harness.paths.silver)
              .filter(col("subjectToken") === oldToken)
              .count()
          )
          _ = assertEquals(forbidden, 0L)
          retry <- harness.process(frame)
          _ = assertEquals(retry, result)
        } yield ()
      }
      .unsafeRunSync()
  }

  test("malformed source writes real sanitized quarantine and cannot publish or advance watermark") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-quality")
          frame <- harness.parsed(malformed = true)
          result <- harness.process(frame)
          _ = assertEquals(result.outcome, StreamingTerminalOutcome.QualityBlocked)
          malformed <- harness.count(harness.paths.quarantine)
          _ = assertEquals(malformed, 1L)
          watermark <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
          _ = assertEquals(watermark, None)
          snapshots <- harness.sync(_.getCollection("analytics_report_snapshots").countDocuments())
          _ = assertEquals(snapshots, 0L)
        } yield ()
      }
      .unsafeRunSync()
  }
  test("closed-day, future, suppressed and idle batches retain their actual report and watermark boundaries") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-calendar-admission")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          initial <- timedFrame(harness, 0L, observed)
          first <- processAt(harness, initial, 0L, observed)
          originalWatermark = Some(observed.minusSeconds(86400L))
          _ = assertEquals(first.candidateWatermark, originalWatermark)
          before <- snapshot(harness)
          closedAt = originalWatermark.get.truncatedTo(ChronoUnit.DAYS).minusSeconds(1L)
          arrival = observed.plusSeconds(60L)
          closed <- timedFrame(harness, 1L, closedAt, skill = "ClosedOnly")
          closedResult <- processAt(harness, closed, 1L, arrival)
          _ = assertEquals(closedResult.outcome, StreamingTerminalOutcome.Published)
          _ = assertEquals(closedResult.candidateWatermark, None)
          afterClosed <- snapshot(harness)
          _ = assertEquals(skillPostingRows(afterClosed), skillPostingRows(before))
          _ = assertEquals(afterClosed.get("funnel"), before.get("funnel"))
          late <- harness.execution(
            harness.spark.read
              .format("delta")
              .load(harness.paths.lateFacts)
              .select("ingestedAt", "expiresAt", "admissionReason")
              .distinct()
              .collect()
              .toVector
          )
          _ = assertEquals(late.size, 1)
          _ = assertEquals(late.head.getTimestamp(0).toInstant, arrival)
          _ = assertEquals(late.head.getTimestamp(1).toInstant, arrival.plusSeconds(30L * 86400L))
          _ = assertEquals(late.head.getString(2), "CLOSED_DAY")
          _ <- processAt(harness, closed, 1L, arrival.plusSeconds(3600L))
          lateCount <- harness.count(harness.paths.lateFacts)
          silverCount <- harness.count(harness.paths.silver)
          _ = assertEquals(lateCount, 12L)
          _ = assertEquals(silverCount, 12L)
          afterClosedWatermark <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
          _ = assertEquals(afterClosedWatermark, originalWatermark)
          futureObservation = observed.plusSeconds(120L)
          future <- timedFrame(harness, 2L, futureObservation.plusSeconds(301L), skill = "FutureOnly")
          futureResult <- processAt(harness, future, 2L, futureObservation)
          _ = assertEquals(futureResult.outcome, StreamingTerminalOutcome.QualityBlocked)
          afterFuture <- snapshot(harness)
          _ = assertEquals(afterFuture, afterClosed)
          quarantined <- harness.count(harness.paths.quarantine)
          _ = assertEquals(quarantined, 12L)
          skewObservation = observed.plusSeconds(180L)
          skew <- timedFrame(harness, 3L, skewObservation.plusSeconds(299L), skill = "SkewVisible")
          skewResult <- processAt(harness, skew, 3L, skewObservation)
          cappedWatermark = Some(skewObservation.minusSeconds(86400L))
          _ = assertEquals(skewResult.candidateWatermark, cappedWatermark)
          suppressed <- timedFrame(
            harness,
            4L,
            observed.plusSeconds(240L),
            skill = "SuppressedOnly",
            distinctSubjects = 1
          )
          suppressedResult <- processAt(harness, suppressed, 4L, observed.plusSeconds(240L))
          _ = assertEquals(suppressedResult.candidateWatermark, None)
          afterSuppressed <- snapshot(harness)
          skills = afterSuppressed
            .getList("skillPostingActivity", classOf[Document])
            .asScala
            .map(_.getString("skill"))
            .toSet
          _ = assert(!skills.contains("suppressedonly"))
          idle <- harness.execution(suppressed.filter(lit(false)))
          idleResult <- processAt(harness, idle, 5L, observed.plusSeconds(300L), records = 0)
          _ = assertEquals(idleResult.candidateWatermark, None)
          finalWatermark <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
          _ = assertEquals(finalWatermark, cappedWatermark)
          afterIdle <- snapshot(harness)
          _ = assertEquals(skillPostingRows(afterIdle), skillPostingRows(afterSuppressed))
        } yield ()
      }
      .unsafeRunSync()
  }

  test("open application windows update provisionally and publish the shared time-to-hire formulas") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-open-hiring-lifecycle")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          created <- timedFrame(harness, 0L, observed, eventType = "APPLICATION_CREATED")
          first <- processAt(harness, created, 0L, observed)
          _ = assertEquals(first.outcome, StreamingTerminalOutcome.Published)
          provisional <- snapshot(harness)
          _ = assertEquals(
            provisional.getList("funnel", classOf[Document]).asScala.map(_.getLong("created").longValue()).sum,
            12L
          )
          _ = assert(!provisional.containsKey("timeToHire"))
          hiredAt = observed.plusSeconds(3600L)
          hired <- timedFrame(harness, 1L, hiredAt, eventType = "APPLICATION_STATUS_CHANGED")
          second <- processAt(harness, hired, 1L, hiredAt)
          _ = assertEquals(second.outcome, StreamingTerminalOutcome.Published)
          complete <- snapshot(harness)
          time = complete.get("timeToHire", classOf[Document])
          _ = assertEquals(time.getLong("eligibleCount").longValue(), 12L)
          _ = Vector("p50Hours", "p75Hours", "p90Hours", "p95Hours")
            .foreach(field => assertEquals(time.getDouble(field).doubleValue(), 1.0))
          funnel = complete.getList("funnel", classOf[Document]).asScala
          _ = assertEquals(funnel.map(_.getLong("created").longValue()).sum, 12L)
          _ = assertEquals(funnel.map(_.getLong("hired").longValue()).sum, 12L)
          facts <- harness.count(harness.paths.silver)
          late <- harness.count(harness.paths.lateFacts)
          _ = assertEquals(facts, 24L)
          _ = assertEquals(late, 0L)
          _ = assertEquals(second.candidateWatermark, Some(hiredAt.minusSeconds(86400L)))
        } yield ()
      }
      .unsafeRunSync()
  }

  private def snapshot(harness: Harness): IO[Document] =
    harness.sync(_.getCollection("analytics_report_snapshots").find(new Document("_id", "current")).first())

  private def skillPostingRows(snapshot: Document): Vector[AnalyticsSkillPostingDayOutput] =
    snapshot
      .getList("skillPostingActivity", classOf[Document])
      .asScala
      .toVector
      .map(row =>
        AnalyticsSkillPostingDayOutput(
          row.getDate("day").toInstant,
          row.getString("skill"),
          row.getLong("postings").longValue()
        )
      )
      .sortBy(row => (row.day.toEpochMilli, row.skill, row.postings))

  private def processAt(
      harness: Harness,
      frame: DataFrame,
      index: Long,
      observed: Instant,
      records: Int = 12
  ): IO[StreamingCoordinatorResult] =
    harness.lock.resource(harness.paths.root).use { _ =>
      for {
        _ <- harness.maintenance.validateHmacConfigurationLocked
        store = harness.journal
        prior <- store.latestWatermark(harness.preparation().identity.lineage)
        preparation = harness
          .preparation(index)
          .copy(
            observedAt = observed,
            priorWatermark = prior,
            inputFingerprint = RangeFingerprint
              .from(AnalyticsDigest.sha256Hex(s"calendar-source-$index-$records".getBytes("UTF-8")))
              .toOption
              .get,
            sourceEndOffsets = (0 until 3).toVector
              .map(p => StreamingPartitionEndOffset.from(harness.topic, p, index * 4L + records / 3L).toOption.get),
            deliveredOffsets =
              if (records == 0) Vector.empty
              else
                (0 until 3).toVector.map(p =>
                  StreamingPartitionSummary
                    .from(harness.topic, p, index * 4L, index * 4L + records / 3L - 1L, records / 3L)
                    .toOption
                    .get
                )
          )
        result <- harness
          .streamingStages(frame)
          .use(stages =>
            new StreamingBatchCoordinator[IO](store, harness.markers, stages, harness.checkpoint(store), IO.unit)
              .process(preparation)
          )
      } yield result
    }

  private def timedFrame(
      harness: Harness,
      batch: Long,
      occurred: Instant,
      eventType: String = "JOB_CREATED",
      skill: String = "Scala",
      distinctSubjects: Int = 12
  ): IO[DataFrame] = harness.execution {
    val rows = (0 until 12).map { n =>
      val application = AnalyticsOperationalEventFixtures.id(s"calendar-application-$n")
      val aggregate =
        if (eventType == "JOB_CREATED") AnalyticsOperationalEventFixtures.id(s"calendar-job-$batch-$n")
        else application
      val aggregateType = if (eventType == "JOB_CREATED") "Job" else "Application"
      val actor = harness.subjects(n % distinctSubjects)
      val job = AnalyticsOperationalEventFixtures.id("calendar-job")
      val eventId = AnalyticsOperationalEventFixtures.id(s"calendar-event-$batch-$n")
      val payload = eventType match {
        case "JOB_CREATED" =>
          io.circe.Json.obj(
            "job" -> io.circe.Json.obj(
              "jobId" -> io.circe.Json.fromString(aggregate),
              "status" -> io.circe.Json.fromString("Open"),
              "skills" -> io.circe.Json.arr(io.circe.Json.fromString(skill))
            )
          )
        case "APPLICATION_CREATED" | "APPLICATION_STATUS_CHANGED" =>
          val identity = io.circe.Json.obj(
            "applicationId" -> io.circe.Json.fromString(application),
            "candidateId" -> io.circe.Json.fromString(actor),
            "jobId" -> io.circe.Json.fromString(job)
          )
          if (eventType == "APPLICATION_CREATED")
            identity.deepMerge(
              io.circe.Json.obj("status" -> io.circe.Json.fromString("Created"))
            )
          else
            identity.deepMerge(
              io.circe.Json.obj(
                "previousStatus" -> io.circe.Json.fromString("Interview"),
                "newStatus" -> io.circe.Json.fromString("Hired")
              )
            )
        case unsupported => fail(s"Unsupported timed fixture event: $unsupported")
      }
      val raw =
        s"""{"eventId":"$eventId","eventType":"$eventType","occurredAt":"$occurred","aggregateType":"$aggregateType","aggregateId":"$aggregate","actorId":"$actor","payload":${payload.noSpaces}}"""
      Row(harness.topic, n % 3, batch * 4L + n / 3L, raw)
    }
    val schema = StructType(
      Vector(
        StructField("topic", StringType, false),
        StructField("partition", IntegerType, false),
        StructField("offset", LongType, false),
        StructField("value", StringType, true)
      )
    )
    val source = harness.spark
      .createDataFrame(rows.asJava, schema)
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", lit(Timestamp.from(occurred)))
    OperationalEventTransforms.parseKafkaRecords(source)
  }
}
