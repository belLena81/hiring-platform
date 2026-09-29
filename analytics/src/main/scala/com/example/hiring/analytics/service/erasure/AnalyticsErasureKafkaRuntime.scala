package com.example.hiring.analytics.service.erasure

import com.example.hiring.analytics.config.KafkaConnection

/** Kafka capabilities needed by the erasure lifecycle after the application wires their implementations. */
final case class AnalyticsErasureKafkaRuntime[F[_]](
    fencerConnection: KafkaConnection,
    producerFencer: TransactionalProducerFencer[F],
    retention: KafkaRetention[F]
)
