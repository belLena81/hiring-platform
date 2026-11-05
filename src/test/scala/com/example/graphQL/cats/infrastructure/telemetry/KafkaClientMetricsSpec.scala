package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.IO
import munit.CatsEffectSuite
import org.apache.kafka.common.errors.{ProducerFencedException, RecordTooLargeException, TimeoutException}

/** KFH-07: Kafka client instruments, their snapshot semantics and their closed attribute sets. */
final class KafkaClientMetricsSpec extends CatsEffectSuite {
  private val Lag = "hiring.kafka.consumer.lag"
  private val Duration = "hiring.kafka.producer.send.duration"
  private val Failures = "hiring.kafka.producer.send.failures"
  private val Rebalances = "hiring.kafka.consumer.rebalances"

  private def exported(name: String)(use: KafkaClientMetrics => IO[Unit]): IO[List[TelemetryCapture.Point]] =
    TelemetryCapture.resource.use { capture =>
      KafkaClientMetrics.resource(capture.otel4s.meterProvider).use { metrics =>
        use(metrics) *> capture.collectMetrics.map(TelemetryCapture.points(_).filter(_.name == name))
      }
    }

  test("KFH-07.1 five records behind the consumer report a lag of 5 for that partition") {
    exported(Lag)(_.recordLag("group-a", "hiring.operational-events", Map(0 -> 5L))).map { points =>
      assertEquals(points.map(_.value), List(5.0))
      assertEquals(points.head.unit, "{record}")
      assertEquals(
        points.head.attributes,
        Map(
          "hiring.consumer.group" -> "group-a",
          "messaging.destination.name" -> "hiring.operational-events",
          "messaging.kafka.partition" -> "0"
        )
      )
    }
  }

  test("KFH-07.1 a new snapshot replaces the old one and dropped partitions leave no series") {
    exported(Lag) { metrics =>
      metrics.recordLag("g", "t", Map(0 -> 5L, 1 -> 7L)) *>
        metrics.recordLag("g", "t", Map(0 -> 2L, 1 -> 7L)) *>
        metrics.dropLag("g", "t", Set(1))
    }.map(points =>
      assertEquals(points.map(p => p.attributes("messaging.kafka.partition") -> p.value), List("0" -> 2.0))
    )
  }

  test("KFH-07.1 lag series of different groups stay independent") {
    exported(Lag) { metrics =>
      metrics.recordLag("g1", "t", Map(0 -> 1L)) *> metrics.recordLag("g2", "t", Map(0 -> 9L)) *>
        metrics.recordLag("g1", "t", Map.empty)
    }.map(points => assertEquals(points.map(p => p.attributes("hiring.consumer.group") -> p.value), List("g2" -> 9.0)))
  }

  test("KFH-07.1 lag is read from the client's records-lag metric per partition and nothing else") {
    val group = "consumer-fetch-manager-metrics"
    val lag = KafkaClientMetrics.ClientLag.fromClientMetrics(
      List(
        ("records-lag", group, Map("topic" -> "t", "partition" -> "0"), java.lang.Double.valueOf(5.0)),
        ("records-lag", group, Map("topic" -> "t", "partition" -> "1"), java.lang.Double.valueOf(Double.NaN)),
        ("records-lag-max", group, Map("topic" -> "t", "partition" -> "0"), java.lang.Double.valueOf(99.0)),
        ("records-lag", group, Map("topic" -> "t"), java.lang.Double.valueOf(3.0)),
        ("records-lag", "other-metrics", Map("topic" -> "t", "partition" -> "2"), java.lang.Double.valueOf(4.0)),
        ("records-lag", group, Map("topic" -> "u", "partition" -> "3"), java.lang.Double.valueOf(-1.0))
      )
    )
    assertEquals(lag, Map("t" -> Map(0 -> 5L), "u" -> Map(3 -> 0L)))
  }

  test("KFH-07.2 send failures are labelled by a closed error class, never by message or cause") {
    exported(Failures) { metrics =>
      List[Throwable](
        new TimeoutException("secret broker host"),
        new ProducerFencedException("fenced"),
        new RecordTooLargeException("key 1234"),
        new IllegalStateException("candidate resume text"),
        new RuntimeException("wrapped", new TimeoutException("cause is ignored"))
      ).foldLeft(IO.unit)((done, error) => done *> metrics.timedSend(IO.raiseError[Unit](error)).attempt.void)
    }.map { points =>
      assert(points.forall(_.attributes.keySet == Set("error.type")), clue(points))
      assertEquals(
        points.map(p => p.attributes("error.type") -> p.value).toMap,
        Map(
          "TimeoutException" -> 1.0,
          "ProducerFencedException" -> 1.0,
          "RecordTooLargeException" -> 1.0,
          "other" -> 2.0
        )
      )
      assert(
        points.forall(p =>
          KafkaClientMetrics.ErrorTypes(p.attributes("error.type")) || p.attributes("error.type") == "other"
        )
      )
    }
  }

  test("KFH-07.2 every send is timed once whether it succeeds or fails") {
    exported(Duration) { metrics =>
      metrics.timedSend(IO.unit) *> metrics.timedSend(IO.raiseError[Unit](new TimeoutException("x"))).attempt.void
    }.map { points =>
      assertEquals(points.map(_.value), List(2.0))
      assertEquals(points.head.unit, "s")
      assertEquals(points.head.attributes, Map.empty[String, String])
    }
  }

  test("KFH-07.2 rebalance counts carry only the group and a fixed kind") {
    exported(Rebalances) { metrics =>
      metrics.rebalanced("g", KafkaRebalanceKind.Assigned, 3) *>
        metrics.rebalanced("g", KafkaRebalanceKind.Revoked, 1) *>
        metrics.rebalanced("g", KafkaRebalanceKind.Revoked, 0)
    }.map { points =>
      assert(points.forall(_.attributes.keySet == Set("hiring.consumer.group", "kind")), clue(points))
      assertEquals(points.map(p => p.attributes("kind") -> p.value).toMap, Map("assigned" -> 3.0, "revoked" -> 1.0))
    }
  }

  test("KFH-07.2 the no-op metrics run the send unchanged") {
    KafkaClientMetrics.noop.timedSend(IO.pure(41).map(_ + 1)).map(assertEquals(_, 42))
  }
}
