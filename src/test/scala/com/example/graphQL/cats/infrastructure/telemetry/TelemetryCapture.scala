package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.{IO, Resource}
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.{InstrumentType, SdkMeterProvider}
import io.opentelemetry.sdk.metrics.`export`.{CollectionRegistration, MetricReader}
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.`export`.{SimpleSpanProcessor, SpanExporter}
import org.typelevel.otel4s.oteljava.OtelJava

import java.util.Collection
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** An in-memory OpenTelemetry SDK for asserting what is exported: finished spans and metric points. */
final class TelemetryCapture private (
    val sdk: OpenTelemetrySdk,
    val otel4s: OtelJava[IO],
    reader: CollectingReader,
    metrics: ConcurrentLinkedQueue[MetricData],
    spans: ConcurrentLinkedQueue[SpanData]
) {
  def java: OpenTelemetry = sdk

  /** Forces a metric collection and returns every metric exported by that collection. */
  def collectMetrics: IO[List[MetricData]] =
    IO.blocking {
      metrics.clear()
      metrics.addAll(reader.collect())
      metrics.asScala.toList
    }

  def finishedSpans: List[SpanData] = spans.asScala.toList
}

/** Collects on demand. */
final class CollectingReader(@annotation.unused sink: ConcurrentLinkedQueue[MetricData]) extends MetricReader {
  @volatile private var registration: Option[CollectionRegistration] = None
  override def register(collectionRegistration: CollectionRegistration): Unit = registration = Some(
    collectionRegistration
  )
  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE
  override def forceFlush(): CompletableResultCode = CompletableResultCode.ofSuccess()
  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
  def collect(): Collection[MetricData] =
    registration.fold(java.util.List.of[MetricData]())(_.collectAllMetrics())
}

object TelemetryCapture {
  final case class Point(name: String, unit: String, value: Double, attributes: Map[String, String])

  def resource: Resource[IO, TelemetryCapture] =
    Resource
      .make(IO.delay {
        val metrics = new ConcurrentLinkedQueue[MetricData]()
        val spans = new ConcurrentLinkedQueue[SpanData]()
        // A pull reader instead of PeriodicMetricReader: the periodic one records SDK self-telemetry with the
        // implicit current context, which throws when fiber context tracking is off.
        val reader = new CollectingReader(metrics)
        val spanExporter = new SpanExporter {
          override def `export`(collection: Collection[SpanData]): CompletableResultCode = {
            val _ = spans.addAll(collection)
            CompletableResultCode.ofSuccess()
          }
          override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
          override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
        }
        val meterProvider = SdkMeterProvider
          .builder()
          .registerMetricReader(reader)
          .build()
        val tracerProvider =
          SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build()
        val sdk = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).setTracerProvider(tracerProvider).build()
        (sdk, reader, metrics, spans)
      }) { case (sdk, _, _, _) => IO.blocking(sdk.close()) }
      .evalMap { case (sdk, reader, metrics, spans) =>
        OtelJava.fromJOpenTelemetry[IO](sdk).map(otel => new TelemetryCapture(sdk, otel, reader, metrics, spans))
      }

  def points(metrics: List[MetricData]): List[Point] =
    metrics.flatMap { metric =>
      val raw: List[(Double, io.opentelemetry.api.common.Attributes)] =
        metric.getLongGaugeData.getPoints.asScala.toList.map(p => (p.getValue.toDouble, p.getAttributes)) ++
          metric.getDoubleGaugeData.getPoints.asScala.toList.map(p => (p.getValue, p.getAttributes)) ++
          metric.getLongSumData.getPoints.asScala.toList.map(p => (p.getValue.toDouble, p.getAttributes)) ++
          metric.getDoubleSumData.getPoints.asScala.toList.map(p => (p.getValue, p.getAttributes)) ++
          metric.getHistogramData.getPoints.asScala.toList.map(p => (p.getCount.toDouble, p.getAttributes))
      raw.map { (value, attributes) =>
        Point(
          metric.getName,
          metric.getUnit,
          value,
          attributes.asMap.asScala.map((key, v) => key.getKey -> v.toString).toMap
        )
      }
    }
}
