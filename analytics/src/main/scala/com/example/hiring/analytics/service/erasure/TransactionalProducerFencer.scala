package com.example.hiring.analytics.service.erasure

import com.example.hiring.analytics.config.KafkaConnection

trait TransactionalProducerFencer[F[_]] {
  def fence(connection: KafkaConnection, transactionalIds: Vector[String]): F[Unit]
}
