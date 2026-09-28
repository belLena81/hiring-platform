package com.example.hiring.analytics.adapter.kafka
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.spark.KafkaConnection

import cats.effect.{IO, Resource}
import org.apache.kafka.clients.admin.Admin

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

private[analytics] trait TransactionalProducerFencer {
  def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit]
}

/** Confirms broker-side producer fencing before deletion passes its Kafka replay barrier. */
object KafkaProducerFencer extends TransactionalProducerFencer {
  override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] =
    fenceAfterSubmission(connection, transactionalIds)(_ => IO.unit)

  /** Test seam signals after the AdminClient has submitted the request and before awaiting its result. */
  private[analytics] def fenceAfterSubmission(
      connection: KafkaConnection,
      transactionalIds: Vector[String]
  )(afterSubmission: org.apache.kafka.common.KafkaFuture[Void] => IO[Unit]): IO[Unit] =
    if (transactionalIds.isEmpty) IO.unit
    else {
      val properties = new Properties()
      properties.put("bootstrap.servers", connection.bootstrapServers)
      KafkaConnection.clientProperties(connection).foreach { case (key, value) => properties.setProperty(key, value) }
      Resource
        .make(IO.blocking(Admin.create(properties)))(admin => IO.blocking(admin.close(Duration.ofSeconds(5))).void)
        .use { admin =>
          for {
            result <- IO.blocking(admin.fenceProducers(transactionalIds.distinct.asJava).all())
            _ <- afterSubmission(result)
            _ <- IO.blocking(result.get()).void
          } yield ()
        }
    }
}
