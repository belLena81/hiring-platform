package com.example.hiring.analytics

import cats.effect.IO
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
    if (transactionalIds.isEmpty) IO.unit
    else IO.blocking {
      val properties = new Properties()
      properties.put("bootstrap.servers", connection.bootstrapServers)
      KafkaConnection.clientProperties(connection).foreach { case (key, value) => properties.setProperty(key, value) }
      val admin = Admin.create(properties)
      try admin.fenceProducers(transactionalIds.distinct.asJava).all().get()
      finally admin.close(Duration.ofSeconds(5))
    }.void
}
