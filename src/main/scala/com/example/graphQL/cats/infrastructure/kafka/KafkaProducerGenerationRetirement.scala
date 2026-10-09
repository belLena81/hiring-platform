package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.service.port.{InterviewPublisherRole, RepositoryIO, RepositoryError}
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import java.util.UUID

/** Explicit operator action; initializing the same transactional ID confirms fencing of the previous epoch. */
object KafkaProducerGenerationRetirement {
  val PublisherPrefix = "hiring-publisher-"
  private val Prefixes = PublisherPrefix :: InterviewPublisherRole.values.toList.map(_.transactionalIdPrefix)

  def canonicalPrefix(id: String): Either[RepositoryError, String] =
    Prefixes.find(id.startsWith).toRight(RepositoryError.InvalidStoredData).flatMap { prefix =>
      val suffix = id.substring(prefix.length)
      Either
        .catchNonFatal(UUID.fromString(suffix))
        .leftMap(_ => RepositoryError.InvalidStoredData)
        .flatMap(uuid => Either.cond(uuid.toString == suffix, prefix, RepositoryError.InvalidStoredData))
    }

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

  def fence(config: KafkaConfig, id: String): RepositoryIO[Unit] =
    RepositoryIO.fromEither(credentials(config, id)).flatMap { case (user, password) =>
      val settings = ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
        .withBootstrapServers(config.bootstrapServers)
        .withProperties(KafkaClientSettings.security(Some(user), Some(password), config.saslSecurityProtocol))
      RepositoryIO.fromIOEither(
        GuardedTransactionalProducer
          .resource(TransactionalProducerSettings(id, settings), MkProducer.mkProducerForSync[IO])
          .use(_ => IO.unit)
          .attempt
          .map(_.leftMap(_ => RepositoryError.Unavailable))
      )
    }
}
