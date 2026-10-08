package com.example.hiring.analytics.adapter.kafka

import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
import com.example.hiring.analytics.service.erasure.TransactionalProducerFencer

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import org.apache.kafka.clients.admin.Admin

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

/** Confirms broker-side producer fencing before deletion passes its Kafka replay barrier. */
object KafkaProducerFencer {
  def apply[F[_]: Async](driverExecution: SparkBlockingExecution[F]): TransactionalProducerFencer[F] =
    new TransactionalProducerFencer[F] {
      override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): F[Unit] =
        fenceAfterSubmission[F](connection, transactionalIds, driverExecution)(_ => Async[F].unit)
    }

  /** Test seam signals after the AdminClient has submitted the request and before awaiting its result. */
  private[analytics] def fenceAfterSubmission[F[_]: Async](
      connection: KafkaConnection,
      transactionalIds: Vector[String],
      driverExecution: SparkBlockingExecution[F]
  )(afterSubmission: org.apache.kafka.common.KafkaFuture[Void] => F[Unit]): F[Unit] =
    if (transactionalIds.isEmpty) Async[F].unit
    else
      (KafkaConnection.preflight[F](connection) *> Async[F].fromEither(
        KafkaClientProperties.clientProperties(connection)
      )).flatMap { clientProperties =>
        val properties = new Properties()
        properties.put("bootstrap.servers", connection.bootstrapServers)
        clientProperties.foreach { case (key, value) => properties.setProperty(key, value) }
        Resource
          .make(driverExecution.blocking(Admin.create(properties)))(admin =>
            driverExecution.blocking(admin.close(Duration.ofSeconds(5))).void
          )
          .use { admin =>
            for {
              result <- driverExecution.blocking(admin.fenceProducers(transactionalIds.distinct.asJava).all())
              _ <- afterSubmission(result)
              _ <- driverExecution.blocking(result.get()).void
            } yield ()
          }
      }
}
