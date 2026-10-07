package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.producer.MkProducer

private[kafka] object GuardedTransactionalProducer {

  /** fs2-kafka installs its close finalizer after initTransactions; retain ownership until that succeeds. */
  def resource(
      settings: TransactionalProducerSettings[IO, String, Array[Byte]],
      factory: MkProducer[IO]
  ): Resource[IO, TransactionalKafkaProducer.WithoutOffsets[IO, String, Array[Byte]]] =
    Resource
      .make(Ref.of[IO, Option[KafkaByteProducer]](None)) { pending =>
        pending
          .getAndSet(None)
          .flatMap(
            _.traverse_(producer =>
              IO.blocking(
                producer.close(java.time.Duration.ofMillis(settings.producerSettings.closeTimeout.toMillis))
              )
            )
          )
      }
      .flatMap { pending =>
        given MkProducer[IO] = new MkProducer[IO] {
          def apply[G[_]](value: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] =
            factory(value).flatTap(producer => pending.set(Some(producer)))
        }
        // Mask the finalizer handoff so cancellation cannot leave both owners responsible for closing.
        Resource
          .make(
            TransactionalKafkaProducer.resource(settings).allocated.flatTap(_ => pending.set(None))
          )(_._2)
          .map(_._1)
      }

}
