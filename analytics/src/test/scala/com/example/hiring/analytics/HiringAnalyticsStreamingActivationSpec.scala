package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.SparkHiringAnalyticsStream
import com.example.hiring.analytics.config.{AnalyticsStreamingSettings, KafkaConnection}
import com.example.hiring.analytics.domain.StreamingActivationIdentity
import com.example.hiring.analytics.domain.{StreamingBatchId, StreamingBatchIdentity, StreamingLineage}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{AnalyticsLakehouseLock, AnalyticsStreamingRegistry}
import com.example.hiring.analytics.service.streaming.{StreamingActivationGate, StreamingCheckpointAcknowledgement}

import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite

final class HiringAnalyticsStreamingActivationSpec extends CatsEffectSuite {
  private val settings = AnalyticsStreamingSettings
    .fromHocon("""
    |analytics.streaming {
    |  stream-id = "hiring-events"
    |  activation-grant-id = "grant-2026-09"
    |  checkpoint-location = "file:///var/lib/hiring-analytics/checkpoints/hiring-events"
    |  trigger-interval = 10 seconds
    |  max-offsets-per-trigger = 1000
    |  maximum-replay-records = 1000
    |  initial-offsets = [{ partition = 0, offset = 0 }]
    |}
    |""".stripMargin)
    .toOption
    .get

  test("missing activation authorization prevents lock ownership and Spark query startup") {
    Ref.of[IO, Boolean](false).flatMap { ownerAcquired =>
      val lock = new AnalyticsLakehouseLock[IO] {
        override def resource(root: String): Resource[IO, Unit] =
          Resource.make(ownerAcquired.set(true))(_ => IO.unit)
      }
      val gate = new StreamingActivationGate[IO] {
        override def requireAuthorized(identity: StreamingActivationIdentity, grantId: String): IO[java.time.Instant] =
          IO.raiseError(AnalyticsError.InvalidConfiguration("activation authorization is absent"))
      }
      val topic = com.example.hiring.analytics.domain.AnalyticsTopic.from("hiring.events").toOption.get
      val stream = new SparkHiringAnalyticsStream[IO](
        null,
        null,
        null,
        KafkaConnection("localhost:9092"),
        topic,
        settings,
        gate,
        AnalyticsStreamingRegistry.allowUnregistered[IO],
        lock,
        "/lakehouse",
        new StreamingCheckpointAcknowledgement[IO] {
          override def callbackMayAcknowledge(identity: StreamingBatchIdentity): IO[Unit] = IO.unit
          override def reconcile(
              lineage: StreamingLineage,
              checkpointedBatchIds: Set[StreamingBatchId],
              checkpointEstablished: Boolean
          ): IO[Unit] = IO.unit
        },
        (_, _, _, _) => IO.unit,
        () => IO.pure("cluster-id" -> "topic-id")
      )

      stream.resource.use(_ => IO.unit).attempt.flatMap { result =>
        ownerAcquired.get.map { acquired =>
          assert(result.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
          assert(!acquired)
        }
      }
    }
  }
}
