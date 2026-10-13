package com.example.hiring.analytics

import cats.effect.{Clock, IO}
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.ShortRetentionFixtures.*
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsErasureStores
import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.{AccountSubjectId, RangeFingerprint, RunId}
import com.example.hiring.analytics.service.erasure.*
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, UUID}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*

/** A committed account deletion as the erasure worker finds it, and the worker composed over a disposable lakehouse. */
private[analytics] object ErasureRequestFixtures {
  val Unused = KafkaConnection("unused:9092")

  final class Request(
      val harness: Harness,
      val stores: MongoAnalyticsErasureStores[IO],
      val subject: String,
      val receiptId: String
  ) {
    val id: AccountSubjectId = AnalyticsErasureWorkerTestSupport.accountSubjectId(subject)
    val runId: RunId = AnalyticsErasureWorkerTestSupport.runId("analytics-erasure-" + subject)
    val fingerprint: RangeFingerprint = AnalyticsErasureWorkerTestSupport.fingerprint("erasure:" + subject)
    def document(collection: String): IO[Document] =
      harness.sync(_.getCollection(collection).find(new Document("_id", subject)).first())
  }

  /** The durable rows a committed account deletion leaves for the worker, with its recorded producer registrations. */
  def seedRequest(
      harness: Harness,
      runtime: Runtime,
      subject: String,
      transactionalIds: Vector[String],
      at: Instant
  ): IO[Request] = {
    val receiptId = UUID.randomUUID().toString
    harness
      .sync { db =>
        db.getCollection("analytics_erasure_requests")
          .insertOne(
            new Document("_id", subject)
              .append("state", "Pending")
              .append("fencingVersion", 1)
              .append("producerRegistry", true)
              .append("requestedAt", Date.from(at))
              .append("receiptId", receiptId)
          )
        db.getCollection("outbox_subject_fences").insertOne(new Document("_id", subject).append("deleted", true))
        db.getCollection("users").insertOne(new Document("_id", subject).append("accountStatus", "Deleted"))
        db.getCollection("hiring_migration_ledger")
          .insertOne(new Document("_id", "003_event_outbox_subject_references").append("state", "Complete"))
        db.createCollection("producer_registrations")
        db.getCollection("hiring_migration_ledger")
          .insertOne(
            new Document("_id", "012_attributable_producer_registrations")
              .append("state", "Complete")
              .append("version", Long.box(1L))
          )
        transactionalIds.foreach { transactionalId =>
          db.getCollection("producer_registrations")
            .insertOne(
              new Document("_id", s"$subject:$transactionalId")
                .append("subjectId", subject)
                .append("transactionalId", transactionalId)
                .append("kind", "Operational")
                .append("state", "Active")
                .append("registeredAt", Date.from(at))
            )
        }
        db.getCollection("analytics_report_control")
          .updateOne(
            new Document("_id", "analytics-report"),
            new Document("$set", new Document("state", "Hidden").append("generation", 1L))
          )
      }
      .as(
        new Request(
          harness,
          AnalyticsErasureWorkerTestSupport.stores(runtime.client, harness.database),
          subject,
          receiptId
        )
      )
  }

  def worker(
      request: Request,
      clock: Clock[IO],
      retention: KafkaRetention[IO],
      fencer: TransactionalProducerFencer[IO] = (_, _) => IO.unit,
      fencerKafka: KafkaConnection = Unused
  ): AnalyticsErasureWorker[IO] =
    AnalyticsErasureWorkerTestSupport.worker(
      request.harness.spark,
      request.harness.database,
      request.stores,
      Unused,
      fencerKafka,
      "hiring.operational-events",
      request.harness.paths,
      request.harness.keys,
      request.harness.publisher,
      clock = clock,
      leaseDuration = 60.days,
      producerFencer = fencer,
      kafkaRetention = Some(retention),
      operational = shortRetention(request.harness.operational),
      deltaLogs = shiftedLogs(32L)
    )

  def fixedRetention(passed: AtomicBoolean): KafkaRetention[IO] = new KafkaRetention[IO] {
    private val barrier = KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 0L)).toOption.get
    override def capture(): IO[KafkaRetentionBarrier] = IO.pure(barrier)
    override def retentionPassed(value: KafkaRetentionBarrier): IO[Boolean] = IO(passed.get())
  }

  def claimAt(request: Request, at: Instant) =
    request.stores.queue.claim(at, at.plus(1L, ChronoUnit.DAYS), 1).map(_.head)

  def processing(
      request: Request,
      at: Instant,
      worker: AnalyticsErasureWorker[IO],
      claim: ErasureClaim
  ): IO[ErasureClaimOutcome] =
    request.harness.publisher.reserve(request.runId, request.fingerprint, at).flatMap(worker.process(claim, _))
}
