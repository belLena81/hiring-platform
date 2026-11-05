package com.example.graphQL.cats.infrastructure.kafka

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.service.port.{InterviewPublisherRole, ProducerTransactionalId, RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import fs2.kafka.*
import fs2.kafka.producer.MkProducer

/** Explicit operator action; initializing the same transactional ID confirms fencing of the previous epoch. The id is a
  * known prefix followed by a worker id; every producer lifetime of that id is a separate generation in the registry.
  */
object KafkaProducerGenerationRetirement {
  val PublisherPrefix: String = ProducerTransactionalId.PublisherPrefix

  def canonicalPrefix(id: String): Either[RepositoryError, String] =
    ProducerTransactionalId.parse(id).map(_._1).leftMap(_ => RepositoryError.InvalidStoredData)

  def credentials(config: KafkaConfig, id: String): Either[RepositoryError, (String, String)] =
    canonicalPrefix(id).flatMap { prefix =>
      val configured = prefix match {
        case PublisherPrefix => config.publisher.saslUsername -> config.publisher.saslPassword
        case InterviewPublisherRole.Orchestrator.transactionalIdPrefix =>
          config.interview.orchestratorUsername -> config.interview.orchestratorPassword
        case _ => config.interview.workerUsername -> config.interview.workerPassword
      }
      configured match {
        case (Some(user), Some(password)) => Right(user -> password)
        case _                            => Left(RepositoryError.InvalidStoredData)
      }
    }

  def fence(config: KafkaConfig, id: String): RepositoryF[IO, Unit] =
    EitherT.fromEither[IO](credentials(config, id)).flatMap { case (user, password) =>
      val settings = ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
        .withBootstrapServers(config.bootstrapServers)
        .withProperties(
          KafkaClientSettings.security(Some(user), Some(password), config.saslSecurityProtocol, config.saslMechanism)
        )
      RepositoryF.fromFEither(
        GuardedTransactionalProducer
          .resource(TransactionalProducerSettings(id, settings), MkProducer.mkProducerForSync[IO])
          .use(_ => IO.unit)
          .attempt
          .map(_.leftMap(_ => RepositoryError.Unavailable))
      )
    }
}
