package com.example.hiring.analytics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLateFactReplayJournal
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.mongodb.client.MongoClients
import munit.FunSuite
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.duration.*

final class MongoAnalyticsLateFactReplayJournalIntegrationSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes
  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  private final class Mongo extends GenericContainer[Mongo](DockerImageName.parse(image))

  test(
    "durable journal pins attempts, rejects conflicts, survives restart, and compacts only completed coordinate details"
  ) {
    val container = new Mongo()
      .withExposedPorts(27017)
      .withCommand("mongod", "--bind_ip_all")
      .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
    container.start()
    val uri = s"mongodb://${container.getHost}:${container.getMappedPort(27017)}"
    val client = AnalyticsMongo4catsTestSupport.client(uri)
    val sync = MongoClients.create(uri)
    try {
      val name = s"late_replay_${UUID.randomUUID().toString.replace('-', '_')}"
      val database = AnalyticsMongo4catsTestSupport.database(client, name)
      val root = s"file:///tmp/$name/lakehouse"
      def journal = new MongoAnalyticsLateFactReplayJournal[IO](database, AnalyticsTestOperationalConfig.streams, root)
      val request = AnalyticsLateFactReplayRequest.from("selected", Vector(("hiring-events", 0, 1L))).toOption.get
      val unfinished = AnalyticsLateFactReplayRequest.from("unfinished", Vector(("hiring-events", 0, 2L))).toOption.get
      val now = Instant.parse("2026-10-01T12:00:00Z")
      def reservation(
          value: AnalyticsLateFactReplayRequest,
          attempt: Int,
          generation: Long = 0L
      ): AnalyticsReportReservation = {
        val identity = AnalyticsLateFactReplayService.reservationIdentityFor(value, attempt).toOption.get
        AnalyticsReportReservation(identity._1, identity._2, generation, attempt.toLong + 1L)
      }
      def checked[A](step: String)(effect: IO[A]): IO[A] =
        effect.adaptError { case cause => new IllegalStateException(s"replay journal step failed: $step", cause) }
      val result = (for {
        _ <- checked("ensure indexes")(journal.ensureIndexes)
        _ <- checked("ensure indexes")(journal.ensureIndexes)
        first <- checked("initial prepare")(journal.prepare(request, reservation(request, 0), now))
        _ <- IO.blocking {
          val persisted = sync
            .getDatabase(name)
            .getCollection(MongoAnalyticsLateFactReplayJournal.CollectionName)
            .find(new Document("requestId", "selected"))
            .first()
          assert(persisted.get("preparedAt").isInstanceOf[java.util.Date], "preparedAt must persist as BSON Date")
          assert(persisted.get("updatedAt").isInstanceOf[java.util.Date], "updatedAt must persist as BSON Date")
        }
        preparedDependencies <- checked("prepared dependencies")(journal.activePublicationRunIds)
        _ = assertEquals(preparedDependencies, Set(reservation(request, 0).runId))
        duplicate <- checked("duplicate prepare")(
          journal.prepare(request, reservation(request, 0), now.plusSeconds(1L))
        )
        _ = assertEquals(duplicate, first)
        changedReservation <- journal.prepare(request, reservation(request, 0, 1L), now).attempt
        _ = assertEquals(changedReservation.left.toOption, Some(AnalyticsError.LateFactReplayRequestConflict))
        changedSelection = AnalyticsLateFactReplayRequest
          .from("selected", Vector(("hiring-events", 0, 3L)))
          .toOption
          .get
        conflict <- journal.prepare(changedSelection, reservation(changedSelection, 0), now).attempt
        _ = assertEquals(conflict.left.toOption, Some(AnalyticsError.LateFactReplayRequestConflict))
        _ <- checked("initial facts merged")(journal.markFactsMerged(request, now))
        _ <- checked("idempotent facts merged")(journal.markFactsMerged(request, now))
        mergedDependencies <- checked("merged dependencies")(journal.activePublicationRunIds)
        _ = assertEquals(mergedDependencies, Set(reservation(request, 0).runId))
        next <- checked("advance attempt")(
          journal.advancePublicationAttempt(request, 0, reservation(request, 1, 1L), now)
        )
        retryAdvance <- checked("retry advance")(
          journal.advancePublicationAttempt(request, 0, reservation(request, 1, 1L), now)
        )
        _ = assertEquals(retryAdvance, next)
        _ = assertEquals(next.progress, AnalyticsLateFactReplayProgress.Prepared)
        loaded <- checked("restart load")(journal.load(request.requestId))
        _ = assertEquals(loaded, Some(next))
        _ <- checked("replacement facts merged")(journal.markFactsMerged(request, now))
        _ <- checked("published")(journal.markPublished(request, now))
        _ <- checked("published")(journal.markPublished(request, now))
        _ <- checked("unfinished prepare")(journal.prepare(unfinished, reservation(unfinished, 0), now))
        retainedDependencies <- checked("retained dependencies")(journal.activePublicationRunIds)
        _ = assertEquals(retainedDependencies, Set(reservation(request, 1, 1L).runId, reservation(unfinished, 0).runId))
        compacted <- checked("compaction")(journal.compactCompleted(now.plusSeconds(32L * 86400L)))
        _ = assertEquals(compacted, 1L)
        compactedDependencies <- checked("compacted dependencies")(journal.activePublicationRunIds)
        _ = assertEquals(compactedDependencies, Set(reservation(unfinished, 0).runId))
        published <- checked("published load")(journal.load(request.requestId))
        pending <- checked("unfinished load")(journal.load(unfinished.requestId))
        _ = assertEquals(published.map(_.progress), Some(AnalyticsLateFactReplayProgress.Published))
        _ = assertEquals(published.map(_.selectionDigest), Some(request.selectionDigest))
        _ = assertEquals(pending.map(_.progress), Some(AnalyticsLateFactReplayProgress.Prepared))
        reuse <- journal.prepare(changedSelection, reservation(changedSelection, 0), now).attempt
        _ = assertEquals(reuse.left.toOption, Some(AnalyticsError.LateFactReplayRequestConflict))
      } yield ()).unsafeRunSync()
      val records = sync.getDatabase(name).getCollection(MongoAnalyticsLateFactReplayJournal.CollectionName)
      assertEquals(records.countDocuments(), 2L)
      val completed = records.find(new Document("requestId", "selected")).first()
      val pending = records.find(new Document("requestId", "unfinished")).first()
      assert(!completed.containsKey("coordinates"))
      assert(pending.containsKey("coordinates"))
      assert(!completed.containsKey("expiresAt"))
      assertEquals(completed.getLong("generation").longValue(), 1L)
      assertEquals(completed.getInteger("publicationAttempt").intValue(), 1)
      assert(completed.get("publishedAt").isInstanceOf[java.util.Date])
      result
    } finally {
      sync.close()
      AnalyticsMongo4catsTestSupport.close(client)
      container.stop()
    }
  }
}
