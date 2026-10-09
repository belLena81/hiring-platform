package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Row
import org.bson.Document

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Composes the production replay service, Delta stages, Mongo journal, report publisher and lakehouse lock. */
final class HiringAnalyticsLateFactReplayRecoveryIntegrationSpec extends AnalyticsMongoIntegrationSuite {
  private def resource = HiringAnalyticsRecoveryTestSupport.resource(mongoEndpoint)

  override val munitIOTimeout: FiniteDuration = 15.minutes

  private def seed(harness: Harness): IO[(AnalyticsLateFactReplayRequest, Instant, Instant)] = {
    val observed = Instant.now().minusSeconds(25L * 86400L).truncatedTo(ChronoUnit.MICROS)
    val expiry = observed.plusSeconds(30L * 86400L)
    val request = AnalyticsLateFactReplayRequest
      .from("selected-old-day", (0 until 12).toVector.map(n => (harness.topic, n % 3, (n / 3).toLong)))
      .toOption
      .get
    harness.lock
      .resource(harness.paths.root)
      .use { _ =>
        harness.maintenance.validateHmacConfigurationLocked *> harness
          .execution {
            val rows = (0 until 12).map { n =>
              val event = s"late-event-$n"
              val token = AnalyticsTestSubjectPseudonymizer.tokenValue(harness.keys, harness.subjects(n))
              Row(
                event,
                AnalyticsDigest.sha256Hex(event.getBytes("UTF-8")),
                "APPLICATION_CREATED",
                harness.topic,
                n % 3,
                (n / 3).toLong,
                Timestamp.from(observed.minusSeconds(86400L)),
                "Application",
                s"application-$n",
                s"application-$n",
                "job-1",
                "Created",
                Vector("Scala"),
                token,
                Vector(token),
                "CLOSED_DAY",
                Timestamp.from(observed),
                Timestamp.from(expiry)
              )
            }
            harness.spark.createDataFrame(rows.asJava, AnalyticsTableSchemas.struct(AnalyticsTableSchemas.lateFacts))
          }
          .flatMap(frame =>
            harness.writer.merge(
              frame,
              harness.paths.lateFacts,
              "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
            )
          )
      }
      .as((request, observed, expiry))
  }

  test("replay reconstructs every real adapter after merged facts and publishes exactly once with original expiry") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("replay-merged-recovery")
          seeded <- seed(harness)
          (request, ingestedAt, expiresAt) = seeded
          once = new AtomicBoolean(true)
          interrupted = new DelegatingPublisher(harness.publisher) {
            override def publish(
                reservation: AnalyticsReportReservation,
                report: AnalyticsReportOutput,
                expiry: Instant
            ) = IO.defer {
              if (once.compareAndSet(true, false)) IO.raiseError(InjectedFailure)
              else super.publish(reservation, report, expiry)
            }
          }
          failed <- harness.replay(request, interrupted).attempt
          _ = assertEquals(failed.left.toOption, Some(InjectedFailure))
          pending <- harness.replayJournal.load(request.requestId)
          _ = assertEquals(pending.map(_.progress), Some(AnalyticsLateFactReplayProgress.FactsMerged))
          recovered <- harness.replay(request)
          repeat <- harness.replay(request)
          _ = assertEquals(recovered, AnalyticsLateFactReplayOutcome.Published)
          _ = assertEquals(repeat, AnalyticsLateFactReplayOutcome.AlreadyPublished)
          count <- harness.count(harness.paths.silver)
          _ = assertEquals(count, 12L)
          times <- harness.execution(
            harness.spark.read
              .format("delta")
              .load(harness.paths.silver)
              .select("ingestedAt", "expiresAt")
              .distinct()
              .collect()
              .toVector
          )
          _ = assertEquals(times.size, 1)
          _ = assertEquals(times.head.getTimestamp(0).toInstant, ingestedAt)
          _ = assertEquals(times.head.getTimestamp(1).toInstant, expiresAt)
          complete <- harness.replayJournal.load(request.requestId)
          receipt <- harness.publisher.publicationReceipt(complete.get.reservation)
          _ = assertEquals(receipt, AnalyticsReportPublicationReceipt.CurrentGeneration)
          report <- harness.sync(
            _.getCollection("analytics_report_snapshots").find(new Document("_id", "current")).first()
          )
          _ = assertEquals(report.getList("funnel", classOf[Document]).get(0).getLong("created").longValue(), 12L)
          independent <- harness.execution(
            !DeltaTable.isDeltaTable(harness.spark, harness.paths.streamingProgress) &&
              !DeltaTable.isDeltaTable(harness.spark, harness.paths.streamingDecisions)
          )
          _ = assert(independent, "explicit replay cannot create streaming progress or decisions")
        } yield ()
      }
      .unsafeRunSync()
  }

  test("committed Mongo publication recovers before selected facts must still exist") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("replay-receipt-recovery")
          seeded <- seed(harness)
          request = seeded._1
          once = new AtomicBoolean(true)
          interrupted = new DelegatingPublisher(harness.publisher) {
            override def publish(
                reservation: AnalyticsReportReservation,
                report: AnalyticsReportOutput,
                expiry: Instant
            ) =
              super.publish(reservation, report, expiry) *> IO.defer {
                if (once.compareAndSet(true, false)) IO.raiseError(InjectedFailure) else IO.unit
              }
          }
          failed <- harness.replay(request, interrupted).attempt
          _ = assertEquals(failed.left.toOption, Some(InjectedFailure))
          pending <- harness.replayJournal.load(request.requestId)
          _ = assertEquals(pending.map(_.progress), Some(AnalyticsLateFactReplayProgress.FactsMerged))
          _ <- harness.lock
            .resource(harness.paths.root)
            .use(_ =>
              harness.execution {
                DeltaTable.forPath(harness.spark, harness.paths.lateFacts).delete()
              }
            )
          missing <- harness.count(harness.paths.lateFacts)
          _ = assertEquals(missing, 0L)
          recovered <- harness.replay(request)
          _ = assertEquals(recovered, AnalyticsLateFactReplayOutcome.Published)
          published <- harness.replayJournal.load(request.requestId)
          _ = assertEquals(published.map(_.progress), Some(AnalyticsLateFactReplayProgress.Published))
          count <- harness.count(harness.paths.silver)
          _ = assertEquals(count, 12L)
          attempts <- harness.sync(_.getCollection("analytics_report_runs").countDocuments())
          _ = assertEquals(attempts, 1L)
        } yield ()
      }
      .unsafeRunSync()
  }

  test("deletion generation changes after Gold while empty markers cannot refresh the replay reservation") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("replay-generation-race")
          seeded <- seed(harness)
          request = seeded._1
          once = new AtomicBoolean(true)
          raced = new DelegatingPublisher(harness.publisher) {
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
          rejected <- harness.replay(request, raced)
          _ = assertEquals(rejected, AnalyticsLateFactReplayOutcome.ErasurePending)
          original <- harness.replayJournal.load(request.requestId)
          _ = assertEquals(original.map(_.publicationAttempt), Some(0))
          _ = assertEquals(original.map(_.reservation.generation), Some(0L))
          snapshots <- harness.sync(_.getCollection("analytics_report_snapshots").countDocuments())
          _ = assertEquals(snapshots, 0L)
          resumed <- harness.replay(request)
          _ = assertEquals(resumed, AnalyticsLateFactReplayOutcome.Published)
          fresh <- harness.replayJournal.load(request.requestId)
          _ = assertEquals(fresh.map(_.publicationAttempt), Some(1))
          _ = assertEquals(fresh.map(_.reservation.generation), Some(1L))
          reservations <- harness.sync(_.getCollection("analytics_report_runs").countDocuments())
          _ = assertEquals(reservations, 2L)
          facts <- harness.count(harness.paths.silver)
          _ = assertEquals(facts, 12L)
        } yield ()
      }
      .unsafeRunSync()
  }

  test("actual Mongo deletion marker rejects a selected retained fact before replay writes") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("replay-deleted-selection")
          seeded <- seed(harness)
          request = seeded._1
          _ <- harness.sync { database =>
            database
              .getCollection("analytics_erasure_requests")
              .insertOne(
                new Document("_id", harness.subjects.head)
                  .append("state", "Pending")
              )
          }
          rejected <- harness.replay(request).attempt
          _ = assertEquals(rejected.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
          recorded <- harness.replayJournal.load(request.requestId)
          _ = assertEquals(recorded, None)
          facts <- harness.count(harness.paths.silver)
          _ = assertEquals(facts, 0L)
          snapshots <- harness.sync(_.getCollection("analytics_report_snapshots").countDocuments())
          _ = assertEquals(snapshots, 0L)
        } yield ()
      }
      .unsafeRunSync()
  }
}
