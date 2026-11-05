package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import org.apache.kafka.common.errors.{
  AuthorizationException,
  BrokerNotAvailableException,
  DisconnectException,
  InvalidProducerEpochException,
  InvalidTopicException,
  NetworkException,
  NotLeaderOrFollowerException,
  ProducerFencedException,
  RecordTooLargeException,
  SaslAuthenticationException,
  SerializationException,
  TimeoutException,
  TopicAuthorizationException,
  UnknownTopicOrPartitionException
}
import org.apache.kafka.common.KafkaException
import org.typelevel.otel4s.{Attribute, Attributes}
import org.typelevel.otel4s.metrics.{BucketBoundaries, MeterProvider}

import java.util.concurrent.TimeUnit

/** The reason a consumer group member moved partitions. */
enum KafkaRebalanceKind(val label: String) {
  case Assigned extends KafkaRebalanceKind("assigned")
  case Revoked extends KafkaRebalanceKind("revoked")
}

/** What the Kafka runtimes report about their clients. Implementations never see record keys, values or ids. */
trait KafkaClientMetrics {

  /** Runs one producer send, recording its duration and, when it fails, one failure with a closed `error.type`. */
  def timedSend[A](send: IO[A]): IO[A]

  /** Counts partitions assigned to or revoked from one consumer group member. */
  def rebalanced(group: String, kind: KafkaRebalanceKind, partitions: Int): IO[Unit]

  /** Counts one offset commit that was dropped because the group rebalanced; the records are redelivered. */
  def commitDropped(group: String): IO[Unit]

  /** Replaces every lag series of one group and topic with the latest client snapshot. */
  def recordLag(group: String, topic: String, lagByPartition: Map[Int, Long]): IO[Unit]

  /** Stops reporting the lag of partitions the member no longer owns. */
  def dropLag(group: String, topic: String, partitions: Set[Int]): IO[Unit]
}

/** Kafka client instruments (meter `hiring-platform`). Attributes come only from closed sets: the configured consumer
  * group and topic, the assigned partition number, a fixed rebalance kind and the allow-listed exception class.
  *
  *   - `hiring.kafka.consumer.lag` `{record}` (observable gauge): `hiring.consumer.group`,
  *     `messaging.destination.name`, `messaging.kafka.partition`. Read from the Kafka client's own `records-lag` metric
  *     snapshot, so a scrape never calls the broker or takes the consumer lock.
  *   - `hiring.kafka.producer.send.duration` `s` (histogram): one observation per send, success or failure.
  *   - `hiring.kafka.producer.send.failures` `{failure}` (counter): `error.type`, one of
  *     [[KafkaClientMetrics.ErrorTypes]] or `other`.
  *   - `hiring.kafka.consumer.rebalances` `{rebalance}` (counter): `hiring.consumer.group`, `kind` (`assigned` or
  *     `revoked`); one count per partition moved. `revoked` is counted when a partition's record stream finalizes,
  *     which also happens on a consumer restart or shutdown, so it can overcount rebalance revocations at each such
  *     event (fs2-kafka exposes no revocation listener in this stream shape).
  *   - `hiring.kafka.consumer.commits.dropped` `{commit}` (counter): `hiring.consumer.group`; an offset commit lost to
  *     a rebalance, whose records are redelivered and absorbed by the durable receipt.
  */
object KafkaClientMetrics {
  private val MeterName = "hiring-platform"
  val OtherErrorType = "other"

  /** The only exception classes that may label a failure; the outermost exception decides, never its causes. */
  private val allowed: Map[String, String] = List[Class[?]](
    classOf[TimeoutException],
    classOf[DisconnectException],
    classOf[NetworkException],
    classOf[NotLeaderOrFollowerException],
    classOf[BrokerNotAvailableException],
    classOf[ProducerFencedException],
    classOf[InvalidProducerEpochException],
    classOf[RecordTooLargeException],
    classOf[SerializationException],
    classOf[InvalidTopicException],
    classOf[UnknownTopicOrPartitionException],
    classOf[TopicAuthorizationException],
    classOf[AuthorizationException],
    classOf[SaslAuthenticationException],
    classOf[KafkaException],
    classOf[java.util.concurrent.TimeoutException]
  ).map(c => c.getName -> c.getSimpleName).toMap

  val ErrorTypes: Set[String] = allowed.values.toSet

  /** The `error.type` of a failure: the outermost exception's simple name when allow-listed, else `other`. */
  def errorType(error: Throwable): String = allowed.getOrElse(error.getClass.getName, OtherErrorType)

  val noop: KafkaClientMetrics = new KafkaClientMetrics {
    def timedSend[A](send: IO[A]): IO[A] = send
    def rebalanced(group: String, kind: KafkaRebalanceKind, partitions: Int): IO[Unit] = IO.unit
    def commitDropped(group: String): IO[Unit] = IO.unit
    def recordLag(group: String, topic: String, lagByPartition: Map[Int, Long]): IO[Unit] = IO.unit
    def dropLag(group: String, topic: String, partitions: Set[Int]): IO[Unit] = IO.unit
  }

  private[telemetry] final case class LagSeries(group: String, topic: String, partition: Int)

  private[telemetry] def lagAttributes(series: LagSeries): Attributes =
    Attributes(
      Attribute("hiring.consumer.group", series.group),
      Attribute("messaging.destination.name", series.topic),
      Attribute("messaging.kafka.partition", series.partition.toLong)
    )

  private[telemetry] def sendFailureAttributes(error: Throwable): Attributes =
    Attributes(Attribute("error.type", errorType(error)))

  private[telemetry] def rebalanceAttributes(group: String, kind: KafkaRebalanceKind): Attributes =
    Attributes(Attribute("hiring.consumer.group", group), Attribute("kind", kind.label))

  /** Instruments are registered once; every callback and recorder swallows its own failures. */
  def resource(meterProvider: MeterProvider[IO]): Resource[IO, KafkaClientMetrics] =
    for {
      meter <- Resource.eval(meterProvider.get(MeterName))
      lag <- Resource.eval(Ref.of[IO, Map[LagSeries, Long]](Map.empty))
      duration <- Resource.eval(
        meter
          .histogram[Double]("hiring.kafka.producer.send.duration")
          .withUnit("s")
          .withDescription("Duration of one Kafka producer send (one publication wave transaction).")
          .withExplicitBucketBoundaries(
            BucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
          )
          .create
      )
      failures <- Resource.eval(
        meter
          .counter[Long]("hiring.kafka.producer.send.failures")
          .withUnit("{failure}")
          .withDescription("Failed Kafka producer sends by allow-listed exception class (`other` for anything else).")
          .create
      )
      rebalances <- Resource.eval(
        meter
          .counter[Long]("hiring.kafka.consumer.rebalances")
          .withUnit("{rebalance}")
          .withDescription("Partitions assigned to or revoked from this consumer group member.")
          .create
      )
      dropped <- Resource.eval(
        meter
          .counter[Long]("hiring.kafka.consumer.commits.dropped")
          .withUnit("{commit}")
          .withDescription("Offset commits dropped because the consumer group rebalanced.")
          .create
      )
      _ <- meter
        .observableGauge[Long]("hiring.kafka.consumer.lag")
        .withUnit("{record}")
        .withDescription(
          "Records behind the log end per assigned partition, from the Kafka client's records-lag metric."
        )
        .createWithCallback(measurement =>
          lag.get
            .flatMap(_.toList.traverse_((series, value) => measurement.record(value, lagAttributes(series))))
            .handleError(_ => ())
        )
    } yield new KafkaClientMetrics {
      def timedSend[A](send: IO[A]): IO[A] =
        IO.monotonic.flatMap { start =>
          send.guaranteeCase { outcome =>
            IO.monotonic
              .flatMap(end => duration.record((end - start).toUnit(TimeUnit.SECONDS)))
              .handleError(_ => ()) *> (outcome match {
              case cats.effect.Outcome.Errored(error) =>
                failures.inc(sendFailureAttributes(error)).handleError(_ => ())
              case _ => IO.unit
            })
          }
        }

      def rebalanced(group: String, kind: KafkaRebalanceKind, partitions: Int): IO[Unit] =
        if (partitions <= 0) IO.unit
        else rebalances.add(partitions.toLong, rebalanceAttributes(group, kind)).handleError(_ => ())

      def commitDropped(group: String): IO[Unit] =
        dropped.inc(Attributes(Attribute("hiring.consumer.group", group))).handleError(_ => ())

      def recordLag(group: String, topic: String, lagByPartition: Map[Int, Long]): IO[Unit] =
        lag.update { current =>
          current.filterNot((series, _) => series.group == group && series.topic == topic) ++
            lagByPartition.map((partition, value) => LagSeries(group, topic, partition) -> value.max(0L))
        }

      def dropLag(group: String, topic: String, partitions: Set[Int]): IO[Unit] =
        lag.update(
          _.filterNot((series, _) => series.group == group && series.topic == topic && partitions(series.partition))
        )
    }

  /** Records lag per `(topic, partition)` from the client's `records-lag` metric values. */
  object ClientLag {
    private val MetricGroup = "consumer-fetch-manager-metrics"
    private val MetricName = "records-lag"

    /** `metrics` is the (name, group, tags, value) view of the Kafka client metrics; only per-partition `records-lag`
      * entries with a finite numeric value count. The result is topic to partition to lag.
      */
    def fromClientMetrics(
        metrics: Iterable[(String, String, Map[String, String], Any)]
    ): Map[String, Map[Int, Long]] =
      metrics.iterator
        .collect {
          case (MetricName, MetricGroup, tags, value: Number) if value.doubleValue.isFinite =>
            for {
              topic <- tags.get("topic")
              partition <- tags.get("partition").flatMap(_.toIntOption)
            } yield (topic, partition, math.max(0.0, value.doubleValue).toLong)
        }
        .flatten
        .toList
        .groupMap(_._1)(row => row._2 -> row._3)
        .view
        .mapValues(_.toMap)
        .toMap
  }
}
