package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.StreamingIngestionFixtures.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.{AnalyticsBatchSettings, AnalyticsConfigFixtures}
import com.example.hiring.analytics.domain.RangeFingerprint
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{AnalyticsPublication, AnalyticsRunOutcome}
import com.example.hiring.testing.KafkaShortRetention
import com.mongodb.client.MongoClients
import munit.CatsEffectSuite
import org.bson.Document

import java.time.Instant
import java.util.Base64
import scala.concurrent.duration.*

/** The one-shot batch reads an exact committed Kafka range, skipping transaction markers, and reports expiry. */
final class HiringAnalyticsBatchKafkaRangeIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 20.minutes

  private val RunId = "kafka-range-run"

  private def batchSettings(
      fixture: StreamingIngestionFixtures.Fixture,
      start: Long,
      endExclusive: Long
  ): IO[AnalyticsBatchSettings] = {
    val manifest = fixture.namespace.manifest
    val key = Base64.getEncoder.encodeToString(Array.fill[Byte](32)(29))
    val packaged = new String(getClass.getClassLoader.getResourceAsStream("application.conf").readAllBytes(), "UTF-8")
    val overrides =
      s"""
         |analytics {
         |  mongo { uri = "${manifest.mongoUri}", database = "${fixture.databaseName}" }
         |  spark { master = "local[2]", local-directory = "${fixture.sparkTemp}" }
         |  kafka {
         |    bootstrap-servers = "${manifest.kafkaBootstrap}"
         |    username = "analytics_reader"
         |    password = "${manifest.readerPassword}"
         |    topic = "${fixture.namespace.events}"
         |    security-protocol = "SASL_PLAINTEXT"
         |    allow-plaintext = true
         |  }
         |  lakehouse { root = "${fixture.root.resolve("lakehouse").toUri}" }
         |  hmac { secret-base64 = "$key", key-id = "hmac-v1" }
         |  batch { run-id = "$RunId", partition = 0, start-offset = $start, end-offset-exclusive = $endExclusive }
         |}
         |""".stripMargin
    IO.fromEither(AnalyticsConfigFixtures.batch(packaged + overrides))
  }

  private def run(settings: AnalyticsBatchSettings): IO[AnalyticsPublication] =
    AppModule.batch[IO](settings).use(program => program)

  /** Each event is its own transaction, so every committed record is followed by one transaction marker offset. */
  private def singleEventTransactions(count: Int): Vector[Transaction] =
    (0 until count).toVector.map(index => Transaction(index until index + 1))

  private def runDocument(fixture: StreamingIngestionFixtures.Fixture): IO[Document] = IO.blocking {
    val sync = MongoClients.create(fixture.namespace.manifest.mongoUri)
    try
      sync
        .getDatabase(fixture.databaseName)
        .getCollection("analytics_report_runs")
        .find(new Document("_id", RunId))
        .first()
    finally sync.close()
  }

  test("HAL-01.1 a committed range of one hundred events with transaction markers is read twice with the same result") {
    fixture.use { fixture =>
      for {
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), singleEventTransactions(100))
        settings <- batchSettings(fixture, 0L, 200L)
        first <- run(settings).timeout(10.minutes)
        second <- run(settings).timeout(10.minutes)
        streamSettings <- StreamingIngestionFixtures.settings(fixture, 13)
        counts <- stored(fixture, streamSettings)
      } yield {
        List(first, second).foreach { publication =>
          assertEquals(publication.bronzeRecords, 100L)
          assertEquals(publication.validRecords, 100L)
          assertEquals(publication.quarantinedRecords, 0L)
          assertEquals(publication.outcome, AnalyticsRunOutcome.Published)
        }
        assertEquals(counts.silver, 100L, "Silver holds one row per distinct event")
        assertEquals(counts.bronze, 100L, "the repeated run does not duplicate Bronze")
      }
    }
  }

  test("HAL-01.2 the stored snapshot belongs to the run whose reservation carries the published range fingerprint") {
    fixture.use { fixture =>
      for {
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), singleEventTransactions(12))
        settings <- batchSettings(fixture, 0L, 24L)
        publication <- run(settings).timeout(10.minutes)
        snapshot <- reportSnapshot(fixture)
        reservation <- runDocument(fixture)
      } yield {
        assertEquals(publication.outcome, AnalyticsRunOutcome.Published)
        assertEquals(snapshot.map(_.getString("runId")), Some(RunId))
        assertEquals(
          reservation.getString("rangeFingerprint"),
          RangeFingerprint.ofSha256(s"${fixture.namespace.events}:0:0:24").value
        )
      }
    }
  }

  test("HAL-11.1 a range that starts below the earliest offset after retention is reported expired") {
    fixture.use { fixture =>
      for {
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), singleEventTransactions(6))
        settings <- batchSettings(fixture, 0L, 12L)
        _ <- KafkaShortRetention.probe(fixture.namespace.manifest).use(_.expireCurrentRecords(fixture.namespace.events))
        outcome <- run(settings).timeout(10.minutes).attempt
      } yield outcome match {
        case Left(AnalyticsError.ExpiredOffsetRange(topic, partition, requested, earliest)) =>
          assertEquals(topic, fixture.namespace.events)
          assertEquals((partition, requested), (0, 0L))
          assert(earliest > 0L, s"earliest available offset moved past the range: $earliest")
        case other => fail(s"expected an expired range, got $other")
      }
    }
  }
}
