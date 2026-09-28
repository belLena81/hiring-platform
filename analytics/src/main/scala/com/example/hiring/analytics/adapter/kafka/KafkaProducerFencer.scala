package com.example.hiring.analytics.adapter.kafka

import com.example.hiring.analytics.config.KafkaConnection

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import org.apache.kafka.clients.admin.Admin

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

private[analytics] trait TransactionalProducerFencer[F[_]] {
  def fence(connection: KafkaConnection, transactionalIds: Vector[String]): F[Unit]
}

/** Confirms broker-side producer fencing before deletion passes its Kafka replay barrier. */
object KafkaProducerFencer {
  def apply[F[_]: Async]: TransactionalProducerFencer[F] = new TransactionalProducerFencer[F] {
    override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): F[Unit] =
      fenceAfterSubmission[F](connection, transactionalIds)(_ => Async[F].unit)
  }

  /** Test seam signals after the AdminClient has submitted the request and before awaiting its result. */
  private[analytics] def fenceAfterSubmission[F[_]: Async](
      connection: KafkaConnection,
      transactionalIds: Vector[String]
  )(afterSubmission: org.apache.kafka.common.KafkaFuture[Void] => F[Unit]): F[Unit] =
    if (transactionalIds.isEmpty) Async[F].unit
    else
      Async[F].fromEither(KafkaClientProperties.clientProperties(connection)).flatMap { clientProperties =>
        val properties = new Properties()
        properties.put("bootstrap.servers", connection.bootstrapServers)
        clientProperties.foreach { case (key, value) => properties.setProperty(key, value) }
        Resource
          .make(Async[F].blocking(Admin.create(properties)))(admin =>
            Async[F].blocking(admin.close(Duration.ofSeconds(5))).void
          )
          .use { admin =>
            for {
              result <- Async[F].blocking(admin.fenceProducers(transactionalIds.distinct.asJava).all())
              _ <- afterSubmission(result)
              _ <- Async[F].blocking(result.get()).void
            } yield ()
          }
      }
}
