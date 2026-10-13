package com.example.hiring.analytics

import cats.effect.IO
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.streaming.*
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.StreamingCalendarFixtures.*
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Actual Delta/Mongo recovery; broker admission, Spark checkpoint ownership and process restart have separate proofs.
  */
final class HiringAnalyticsStreamingRecoveryIntegrationSpec extends SharedRecoveryRuntimeSuite {

  override val munitIOTimeout: FiniteDuration = 15.minutes

  test(
    "HCS-02.4 reconstructed coordinator converges after Bronze, Silver, Gold and committed Mongo boundary failures"
  ) {
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

  test(
    "HCS-10.6 HAL-12.6 a completed deletion generation with unchanged marker view forces a fresh pinned publication"
  ) {
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

  test(
    "HCS-02.5 mixed new facts and a newer preexisting duplicate preserve only the original eligible watermark on retry"
  ) {
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

  test("HCS-10.5 changed deletion markers keep actual publication hidden and do not advance watermark") {
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

  test("HCS-06.3 malformed source writes real sanitized quarantine and cannot publish or advance watermark") {
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
  test(
    "HCS-03.1 HCS-04.1 HCS-05.2 closed-day, future, suppressed and idle batches retain their actual report and watermark boundaries"
  ) {
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

  test("HCS-04.2 HCS-05.1 open application windows update provisionally and publish the shared time-to-hire formulas") {
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

  private def control(harness: Harness): IO[Document] =
    harness.sync(_.getCollection("analytics_report_control").find(new Document("_id", "analytics-report")).first())

  private def revision(harness: Harness): IO[Long] =
    control(harness).map(_.getLong("lastPublishedRevision").longValue())

  private def storedSkills(harness: Harness, path: String): IO[Vector[String]] =
    harness.execution(
      harness.spark.read
        .format("delta")
        .load(path)
        .select("jobSkills")
        .collect()
        .toVector
        .flatMap(_.getSeq[String](0))
        .distinct
    )

  test("HCS-03.2 events at the last microsecond of a UTC day and at midnight fall in different daily windows") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-utc-day-boundary")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          midnight = observed.truncatedTo(ChronoUnit.DAYS)
          lastOfDay = midnight.minus(1L, ChronoUnit.MICROS)
          before <- timedFrame(harness, 0L, lastOfDay)
          after <- timedFrame(harness, 1L, midnight)
          first <- processAt(harness, before, 0L, observed)
          second <- processAt(harness, after, 1L, observed)
          _ = assertEquals(first.outcome, StreamingTerminalOutcome.Published)
          _ = assertEquals(second.outcome, StreamingTerminalOutcome.Published)
          report <- snapshot(harness)
          days = skillPostingRows(report).filter(_.skill == "scala").map(row => row.day -> row.postings).toMap
          _ = assertEquals(days.get(midnight.minusSeconds(86400L)), Some(12L), days.toString)
          _ = assertEquals(days.get(midnight), Some(12L), days.toString)
        } yield ()
      }
      .unsafeRunSync()
  }

  test("HCS-03.3 an idle batch moves neither the watermark nor the report content") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-idle-batch")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          initial <- timedFrame(harness, 0L, observed)
          _ <- processAt(harness, initial, 0L, observed)
          lineage = harness.preparation().identity.lineage
          watermark <- harness.journal.latestWatermark(lineage)
          publishedRevision <- revision(harness)
          reports <- snapshot(harness)
          empty <- harness.execution(initial.filter(lit(false)))
          idle <- processAt(harness, empty, 1L, observed.plusSeconds(60L), records = 0)
          _ = assertEquals(idle.candidateWatermark, None)
          after <- harness.journal.latestWatermark(lineage)
          _ = assertEquals(after, watermark)
          afterRevision <- revision(harness)
          _ = assertEquals(afterRevision, publishedRevision + 1L, "idle batch publication revision")
          current <- snapshot(harness)
          _ = assertEquals(skillPostingRows(current), skillPostingRows(reports))
        } yield ()
      }
      .unsafeRunSync()
  }

  test("HCS-06.1 a conflicting repeat of a published event quarantines and leaves report and watermark unchanged") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-conflicting-repeat")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          initial <- timedFrame(harness, 0L, observed)
          _ <- processAt(harness, initial, 0L, observed)
          lineage = harness.preparation().identity.lineage
          watermark <- harness.journal.latestWatermark(lineage)
          publishedRevision <- revision(harness)
          quarantinedBefore <- harness.count(harness.paths.quarantine)
          conflicting <- timedFrame(harness, 1L, observed, skill = "Rust", eventKey = Some(0L))
          result <- processAt(harness, conflicting, 1L, observed.plusSeconds(60L))
          quarantinedAfter <- harness.count(harness.paths.quarantine)
          _ = assertEquals(result.candidateWatermark, None, result.toString)
          _ = assertEquals(quarantinedAfter, quarantinedBefore + 12L, "one quarantine row per conflicting event")
          skills <- storedSkills(harness, harness.paths.silver)
          _ = assertEquals(skills, Vector("Scala"))
          after <- harness.journal.latestWatermark(lineage)
          _ = assertEquals(after, watermark)
          afterRevision <- revision(harness)
          _ = assertEquals(afterRevision, publishedRevision)
        } yield ()
      }
      .unsafeRunSync()
  }

  test("HCS-06.2 a conflicting repeat of a retained late fact quarantines and keeps the stored late fact") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("stream-conflicting-late-fact")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          initial <- timedFrame(harness, 0L, observed)
          _ <- processAt(harness, initial, 0L, observed)
          closedAt = observed.truncatedTo(ChronoUnit.DAYS).minus(5L, ChronoUnit.DAYS)
          late <- timedFrame(harness, 1L, closedAt, skill = "ClosedOnly")
          _ <- processAt(harness, late, 1L, observed.plusSeconds(60L))
          retained <- harness.count(harness.paths.lateFacts)
          _ = assertEquals(retained, 12L)
          lineage = harness.preparation().identity.lineage
          watermark <- harness.journal.latestWatermark(lineage)
          quarantinedBefore <- harness.count(harness.paths.quarantine)
          conflicting <- timedFrame(harness, 2L, closedAt, skill = "Rust", eventKey = Some(1L))
          _ <- processAt(harness, conflicting, 2L, observed.plusSeconds(120L))
          quarantinedAfter <- harness.count(harness.paths.quarantine)
          _ = assertEquals(quarantinedAfter, quarantinedBefore + 12L, "one quarantine row per conflicting event")
          skills <- storedSkills(harness, harness.paths.lateFacts)
          _ = assertEquals(skills, Vector("ClosedOnly"))
          retainedAfter <- harness.count(harness.paths.lateFacts)
          _ = assertEquals(retainedAfter, 12L)
          after <- harness.journal.latestWatermark(lineage)
          _ = assertEquals(after, watermark)
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
}
