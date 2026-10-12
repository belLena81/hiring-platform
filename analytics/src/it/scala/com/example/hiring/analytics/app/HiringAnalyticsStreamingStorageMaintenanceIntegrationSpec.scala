package com.example.hiring.analytics.app

import cats.effect.IO
import cats.syntax.all.*
import com.example.hiring.analytics.AnalyticsTestSubjectPseudonymizer
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.SharedRecoveryRuntimeSuite
import com.example.hiring.analytics.StreamingCalendarFixtures.*
import com.example.hiring.analytics.ShortRetentionFixtures.*
import com.example.hiring.analytics.app.StreamingMaintenanceFixtures.*
import com.example.hiring.analytics.domain.StreamingBatchId
import com.example.hiring.analytics.service.streaming.StreamingCheckpointBatch
import org.apache.spark.sql.functions.{array_contains, col}
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*

/** Idle-time maintenance over a real Delta and Mongo lakehouse: retention, erasure upkeep and progress pruning. */
final class HiringAnalyticsStreamingStorageMaintenanceIntegrationSpec extends SharedRecoveryRuntimeSuite {
  override val munitIOTimeout: FiniteDuration = 30.minutes

  private def revision(harness: Harness): IO[Long] =
    harness
      .sync(_.getCollection("analytics_report_control").find(new Document("_id", "analytics-report")).first())
      .map(_.getLong("lastPublishedRevision").longValue())

  private def subjectRows(harness: Harness, path: String, subject: String): IO[Long] = {
    val token = AnalyticsTestSubjectPseudonymizer.tokenValue(harness.keys, subject)
    harness.execution {
      harness.spark.read
        .format("delta")
        .load(path)
        .filter(col("subjectToken") === token || array_contains(col("subjectTokens"), token))
        .count()
    }
  }

  /** Bounded polling: Delta only vacuums files whose removal is older than the one-second safety horizon. */
  private def reclaimUntil(
      maintenance: Maintenance,
      done: IO[Boolean],
      attempts: Int = 60
  ): IO[Unit] =
    done.flatMap { finished =>
      if (finished) IO.unit
      else if (attempts == 0) IO.raiseError(new AssertionError("physical reclamation did not complete in time"))
      else maintenance.reclaim *> IO.sleep(1.second) *> reclaimUntil(maintenance, done, attempts - 1)
    }

  test("HCS-11.1 maintenance thirty-one days later expires Bronze, Silver and late facts and reclaims their files") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("maintenance-expiry")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        lineage = harness.preparation().identity.lineage
        current <- timedFrame(harness, 0L, observed)
        _ <- processAt(harness, current, 0L, observed)
        closedAt = observed.truncatedTo(ChronoUnit.DAYS).minus(5L, ChronoUnit.DAYS)
        late <- timedFrame(harness, 1L, closedAt, skill = "ClosedOnly")
        _ <- processAt(harness, late, 1L, observed.plusSeconds(60L))
        tables = Vector(harness.paths.bronze, harness.paths.silver, harness.paths.lateFacts)
        before <- tables.traverse(table => tableFiles(table).map(table -> _)).map(_.toMap)
        counts <- tables.traverse(harness.count)
        _ = assert(counts.forall(_ > 0L), s"every table starts populated: $counts")
        progress <- harness.count(harness.paths.streamingProgress)
        maintenance = StreamingMaintenanceFixtures.maintenance(
          harness,
          shortRetention(harness.operational),
          progressRetention = 365.days,
          logs = shiftedLogs(32L)
        )
        _ <- maintenance.run(lineage, observed.plus(31L, ChronoUnit.DAYS))
        remaining <- tables.traverse(harness.count)
        _ = assertEquals(remaining, Vector.fill(3)(0L), "expired rows are deleted")
        _ = assert(tables.forall(table => before(table)._1.nonEmpty), "the tables held data files before maintenance")
        _ <- reclaimUntil(
          maintenance,
          tables
            .traverse(table => tableFiles(table).map(after => after._1.intersect(before(table)._1).isEmpty))
            .map(_.forall(identity))
        )
        after <- tables.traverse(table => tableFiles(table).map(table -> _)).map(_.toMap)
        _ = tables.foreach { table =>
          assertEquals(after(table)._1.intersect(before(table)._1), Set.empty[String], s"data files of $table")
          assertEquals(after(table)._2.intersect(Set("_delta_log/00000000000000000000.json")), Set.empty[String], table)
        }
        control <- harness.count(harness.paths.streamingProgress)
        _ = assertEquals(control, progress, "control tables are intact")
      } yield ()
    }
  }

  test("HCS-11.2 maintenance with an active deletion marker purges the subject and does not refresh the report") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("maintenance-marker")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        lineage = harness.preparation().identity.lineage
        frame <- timedFrame(harness, 0L, observed)
        _ <- processAt(harness, frame, 0L, observed)
        published <- revision(harness)
        deleted = harness.subjects(2)
        _ <- harness.sync(
          _.getCollection("analytics_erasure_requests").insertOne(
            new Document("_id", deleted).append("state", "Pending")
          )
        )
        _ <- subjectRows(harness, harness.paths.silver, deleted).map(rows => assertEquals(rows, 1L))
        maintenance = StreamingMaintenanceFixtures.maintenance(harness, harness.operational, 7.days)
        _ <- maintenance.run(lineage, Instant.now())
        silver <- subjectRows(harness, harness.paths.silver, deleted)
        bronze <- subjectRows(harness, harness.paths.bronze, deleted)
        _ = assertEquals((silver, bronze), (0L, 0L))
        remaining <- harness.count(harness.paths.silver)
        _ = assertEquals(remaining, 11L)
        after <- revision(harness)
        _ = assertEquals(after, published, "the report is not republished while a marker is active")
      } yield ()
    }
  }

  test("HCS-11.3 progress older than sixty seconds is pruned while unfinished and checkpoint-retained batches stay") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("maintenance-pruning")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        lineage = harness.preparation().identity.lineage
        arrival = (index: Long) => observed.plusSeconds(index * 3600L)
        _ <- (0L until 4L).toVector.traverse_ { index =>
          timedFrame(harness, index, arrival(index)).flatMap(processAt(harness, _, index, arrival(index)))
        }
        failOnce = new AtomicBoolean(true)
        unfinishedFrame <- timedFrame(harness, 4L, arrival(4L))
        interrupted <- processAt(
          harness,
          unfinishedFrame,
          4L,
          arrival(4L),
          observer = Some(new StreamingDurableBoundaryObserver[IO] {
            override def completed(
                boundary: StreamingDurableBoundary,
                identity: com.example.hiring.analytics.domain.StreamingBatchIdentity
            ) =
              IO.whenA(boundary == StreamingDurableBoundary.BronzeCommitted && failOnce.compareAndSet(true, false))(
                IO.raiseError(InjectedFailure)
              )
          })
        ).attempt
        _ = assertEquals(interrupted.left.toOption, Some(InjectedFailure))
        completions <- harness.execution {
          harness.spark.read
            .format("delta")
            .load(harness.paths.streamingProgress)
            .select("batchId", "completedAt")
            .collect()
            .toVector
            .flatMap(row => Option(row.getTimestamp(1)).map(row.getLong(0) -> _.toInstant))
            .toMap
        }
        olderThanRetention = completions(2L)
        _ = assert(completions(3L).isAfter(olderThanRetention.plusSeconds(1L)), s"batches finish apart: $completions")
        at = olderThanRetention.plusSeconds(61L)
        retained = Vector(StreamingCheckpointBatch(StreamingBatchId.from(1L).toOption.get, Map.empty, committed = true))
        maintenance = StreamingMaintenanceFixtures.maintenance(harness, harness.operational, 60.seconds)
        _ <- maintenance.run(lineage, at, retained)
        kept <- (0L to 4L).toVector.traverse(index =>
          harness.journal.load(harness.preparation(index).identity).map(index -> _.isDefined)
        )
        _ = assertEquals(
          kept.toMap,
          Map(0L -> false, 1L -> true, 2L -> false, 3L -> true, 4L -> true),
          "pruned: old finished batches; kept: checkpoint-retained 1, recent and latest 3, unfinished 4"
        )
      } yield ()
    }
  }
}
