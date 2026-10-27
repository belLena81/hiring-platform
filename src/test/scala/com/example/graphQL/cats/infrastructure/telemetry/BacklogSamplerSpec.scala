package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.{MetricExporter, PeriodicMetricReader}
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import java.time.Instant
import java.util.Collection
import com.example.graphQL.cats.service.{Diagnostics, FailureReason, LogEvent, LogField, LogFields}
import munit.CatsEffectSuite
import org.typelevel.otel4s.oteljava.OtelJava
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Backlog gauges report durable queue state, and report nothing when the state could not be observed. */
final class BacklogSamplerSpec extends CatsEffectSuite {
  private val interval = 15.seconds

  private final class ScriptedSource(outcomes: Ref[IO, List[IO[BacklogSnapshot]]]) extends BacklogSource {
    override def sample(now: Instant): IO[BacklogSnapshot] =
      outcomes.modify {
        case head :: tail if tail.nonEmpty => (tail, head)
        case head :: _                     => (List(head), head)
        case Nil                           => (Nil, IO.raiseError(new IllegalStateException("no script")))
      }.flatten
  }

  private def sampler(
      embedding: List[IO[BacklogSnapshot]],
      outbox: List[IO[BacklogSnapshot]] = List(IO.pure(BacklogSnapshot(0, None))),
      diagnostics: Diagnostics[IO] = Diagnostics.noop[IO]
  ): IO[BacklogSampler] =
    (Ref.of[IO, List[IO[BacklogSnapshot]]](embedding), Ref.of[IO, List[IO[BacklogSnapshot]]](outbox)).flatMapN {
      (e, o) =>
        BacklogSampler.create(
          List(
            BacklogCapability.EmbeddingWork -> new ScriptedSource(e),
            BacklogCapability.OperationalEventOutbox -> new ScriptedSource(o)
          ),
          interval,
          diagnostics
        )
    }

  private val backlog = BacklogSnapshot(7, Some(90.seconds))

  test("a sampled backlog is observed with its count, oldest age and sample age") {
    TestControl.executeEmbed(for {
      s <- sampler(List(IO.pure(backlog)))
      _ <- s.sampleAll
      _ <- IO.sleep(5.seconds)
      observed <- s.observations
    } yield {
      val embedding = observed.find(_.capability == BacklogCapability.EmbeddingWork).get
      assertEquals(embedding.snapshot, backlog)
      assertEquals(embedding.sampleAge, 5.seconds)
      assert(embedding.fresh)
    })
  }

  test("an empty backlog is observed as zero pending with no oldest age") {
    TestControl.executeEmbed(for {
      s <- sampler(List(IO.pure(BacklogSnapshot(0, None))))
      _ <- s.sampleAll
      observed <- s.observations
    } yield {
      val embedding = observed.find(_.capability == BacklogCapability.EmbeddingWork).get
      assertEquals(embedding.snapshot, BacklogSnapshot(0, None))
      assert(embedding.fresh)
    })
  }

  test("a failed sample makes the capability unobserved instead of keeping or zeroing the last value") {
    TestControl.executeEmbed(for {
      s <- sampler(List(IO.pure(backlog), IO.raiseError(new RuntimeException("mongo down"))))
      _ <- s.sampleAll
      before <- s.observations
      _ <- s.sampleAll
      after <- s.observations
    } yield {
      assertEquals(
        before.map(_.capability).toSet,
        Set(BacklogCapability.EmbeddingWork, BacklogCapability.OperationalEventOutbox)
      )
      assertEquals(after.map(_.capability), List(BacklogCapability.OperationalEventOutbox))
    })
  }

  test("a sample that exceeds the interval is a failed sample and does not block the other capability") {
    TestControl.executeEmbed(for {
      s <- sampler(List(IO.never))
      _ <- s.sampleAll
      observed <- s.observations
    } yield assertEquals(observed.map(_.capability), List(BacklogCapability.OperationalEventOutbox)))
  }

  test("a sample older than the freshness window is no longer fresh") {
    TestControl.executeEmbed(for {
      s <- sampler(List(IO.pure(backlog)))
      _ <- s.sampleAll
      _ <- IO.sleep(interval * 3)
      atLimit <- s.observations
      _ <- IO.sleep(1.second)
      expired <- s.observations
    } yield {
      assert(atLimit.forall(_.fresh))
      assert(expired.forall(!_.fresh))
    })
  }

  test("the background sampler refreshes on the interval and stops when its resource is released") {
    TestControl.executeEmbed(for {
      calls <- Ref.of[IO, Int](0)
      source = new BacklogSource {
        def sample(now: Instant): IO[BacklogSnapshot] =
          calls.updateAndGet(_ + 1).map(n => BacklogSnapshot(n.toLong, None))
      }
      s <- BacklogSampler.create(List(BacklogCapability.EmbeddingWork -> source), interval, Diagnostics.noop[IO])
      _ <- s.background.use(_ => IO.sleep(interval * 2 + 1.second))
      stopped <- calls.get
      _ <- IO.sleep(interval * 4)
      later <- calls.get
    } yield {
      assertEquals(stopped, 3)
      assertEquals(later, 3)
    })
  }

  private final case class Point(name: String, unit: String, value: Double, attributes: Map[String, String])

  /** Registers the gauges on a real SDK meter provider and collects what one export would send. */
  private def exported(s: BacklogSampler): IO[List[Point]] = exportedObservations(s.observations)

  private def exportedObservations(observations: IO[List[BacklogObservation]]): IO[List[Point]] =
    IO.defer {
      val collected = new java.util.concurrent.ConcurrentLinkedQueue[MetricData]()
      val exporter = new MetricExporter {
        override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
          AggregationTemporality.CUMULATIVE
        override def `export`(metrics: Collection[MetricData]): CompletableResultCode = {
          collected.addAll(metrics)
          CompletableResultCode.ofSuccess()
        }
        override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
        override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
      }
      val provider = SdkMeterProvider
        .builder()
        .registerMetricReader(PeriodicMetricReader.create(exporter))
        .build()
      val sdk = OpenTelemetrySdk.builder().setMeterProvider(provider).build()
      OtelJava
        .fromJOpenTelemetry[IO](sdk: OpenTelemetry)
        .flatMap { otel =>
          BacklogMetrics.registerObservations(otel.meterProvider, observations).use { _ =>
            IO.blocking(provider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS)).void
          }
        }
        .map { _ =>
          collected.asScala.toList.flatMap { metric =>
            val points: List[(Double, Map[String, String])] =
              (metric.getLongGaugeData.getPoints.asScala.toList.map(p => (p.getValue.toDouble, p.getAttributes)) ++
                metric.getDoubleGaugeData.getPoints.asScala.toList.map(p => (p.getValue, p.getAttributes)))
                .map((v, attrs) => (v, attrs.asMap.asScala.map((k, value) => k.getKey -> value.toString).toMap))
            points.map((v, attrs) => Point(metric.getName, metric.getUnit, v, attrs))
          }
        }
    }

  test("exported gauges carry only the capability label, names and units, and an empty backlog has no oldest age") {
    for {
      s <- sampler(List(IO.pure(BacklogSnapshot(0, None))), List(IO.pure(backlog)))
      _ <- s.sampleAll
      points <- exported(s)
    } yield {
      assert(points.forall(_.attributes.keySet == Set("hiring.capability")), clue(points))
      assertEquals(
        points.map(_.attributes("hiring.capability")).toSet,
        Set("embedding_work", "operational_event_outbox")
      )
      val pending = points.filter(_.name == "hiring.backlog.pending")
      assert(pending.forall(_.unit == "{item}"))
      assertEquals(
        pending.map(p => p.attributes("hiring.capability") -> p.value).toMap,
        Map("embedding_work" -> 0.0, "operational_event_outbox" -> 7.0)
      )
      val oldest = points.filter(_.name == "hiring.backlog.oldest_pending_age")
      assert(oldest.forall(_.unit == "s"))
      assertEquals(
        oldest.map(p => p.attributes("hiring.capability") -> p.value),
        List("operational_event_outbox" -> 90.0)
      )
      assert(points.filter(_.name == "hiring.backlog.sample_age").forall(_.unit == "s"))
    }
  }

  test("a failed sample exports no backlog value for that capability") {
    for {
      s <- sampler(List(IO.raiseError(new RuntimeException("mongo down"))), List(IO.pure(backlog)))
      _ <- s.sampleAll
      points <- exported(s)
    } yield assertEquals(
      points.map(_.attributes("hiring.capability")).toSet,
      Set("operational_event_outbox")
    )
  }

  test("a stale sample exports its sample age but no pending count or oldest age") {
    val stale = BacklogObservation(BacklogCapability.EmbeddingWork, backlog, 46.seconds, fresh = false)
    exportedObservations(IO.pure(List(stale))).map { points =>
      assertEquals(points.map(_.name).toSet, Set("hiring.backlog.sample_age"))
      assertEquals(points.map(_.value), List(46.0))
    }
  }

  test("a capability recovers to observed after a failed sample") {
    TestControl.executeEmbed(for {
      s <- sampler(
        List(IO.pure(backlog), IO.raiseError(new RuntimeException("down")), IO.pure(BacklogSnapshot(2, None)))
      )
      _ <- s.sampleAll
      _ <- s.sampleAll
      during <- s.observations
      _ <- s.sampleAll
      after <- s.observations
    } yield {
      assert(!during.exists(_.capability == BacklogCapability.EmbeddingWork))
      assertEquals(
        after.find(_.capability == BacklogCapability.EmbeddingWork).map(_.snapshot),
        Some(BacklogSnapshot(2, None))
      )
    })
  }

  test("one failing source stays unobserved across ticks while the other stays observed") {
    TestControl.executeEmbed(for {
      s <- sampler(List(IO.raiseError(new RuntimeException("down"))), List(IO.pure(backlog)))
      views <- (1 to 3).toList.traverse(_ => s.sampleAll >> IO.sleep(interval) >> s.observations)
    } yield views.foreach { observed =>
      assertEquals(observed.map(_.capability), List(BacklogCapability.OperationalEventOutbox))
      assert(observed.forall(_.fresh))
    })
  }

  test("a slow source does not delay or stale the other source") {
    TestControl.executeEmbed(for {
      s <- sampler(List(IO.sleep(10.seconds).as(backlog)), List(IO.pure(backlog)))
      _ <- s.sampleAll
      observed <- s.observations
    } yield {
      val outbox = observed.find(_.capability == BacklogCapability.OperationalEventOutbox).get
      val embedding = observed.find(_.capability == BacklogCapability.EmbeddingWork).get
      // The outbox sample was taken when its own read finished, not after waiting for the slow source.
      assertEquals(outbox.sampleAge, 10.seconds)
      assertEquals(embedding.sampleAge, 0.seconds)
    })
  }

  private def capturingDiagnostics(events: Ref[IO, List[(LogEvent, Map[LogField, String])]]): Diagnostics[IO] =
    new Diagnostics[IO] {
      def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
        events.update((event, fields) :: _)
    }

  test("a failure is reported once per episode with a bounded reason and no error text, and again after recovery") {
    TestControl.executeEmbed(for {
      events <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      s <- sampler(
        List(
          IO.raiseError(new RuntimeException("secret mongo uri")),
          IO.raiseError(new RuntimeException("secret mongo uri")),
          IO.pure(backlog),
          IO.never
        ),
        diagnostics = capturingDiagnostics(events)
      )
      _ <- s.sampleAll
      _ <- s.sampleAll
      afterEpisode <- events.get
      _ <- s.sampleAll
      _ <- s.sampleAll
      all <- events.get
    } yield {
      def fieldsOf(events: List[(LogEvent, Map[LogField, String])]) = events.reverse.map(_._2)
      assert((afterEpisode ++ all).forall(_._1 == LogEvent.BacklogSampleFailed))
      assertEquals(
        fieldsOf(afterEpisode),
        List(Map(LogField.Worker -> "embedding_work", LogField.Reason -> FailureReason.InternalError.reason))
      )
      assertEquals(
        fieldsOf(all),
        List(
          Map(LogField.Worker -> "embedding_work", LogField.Reason -> FailureReason.InternalError.reason),
          Map(LogField.Worker -> "embedding_work", LogField.Reason -> FailureReason.DeadlineExceeded.reason)
        )
      )
      // Only bounded values: never the error message or any identifier.
      assert(all.forall(_._2.values.forall(v => LogFields.validPublic(LogField.Reason, v) || v == "embedding_work")))
      assert(all.forall(_._2.values.forall(!_.contains("secret"))))
    })
  }
}
