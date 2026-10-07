package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.service.port.{RepositoryIO, RepositoryError}
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import java.util.UUID

/** Explicit operator action; initializing the same transactional ID confirms fencing of the previous epoch. */
object KafkaProducerGenerationRetirement {
  def credentials(config: KafkaConfig, id: String): Either[RepositoryError, (String, String)] = {
    val choices = List(
      ("hiring-publisher-", config.publisher.saslUsername, config.publisher.saslPassword),
      ("hiring-interview-orchestrator-", config.interview.orchestratorUsername, config.interview.orchestratorPassword),
      ("hiring-interview-worker-", config.interview.workerUsername, config.interview.workerPassword)
    )
    choices.find { case (prefix, _, _) => id.startsWith(prefix) }.toRight(RepositoryError.InvalidStoredData).flatMap {
      case (prefix, Some(user), Some(password)) =>
        val suffix = id.substring(prefix.length)
        Either
          .catchNonFatal(UUID.fromString(suffix))
          .leftMap(_ => RepositoryError.InvalidStoredData)
          .flatMap(uuid => Either.cond(uuid.toString == suffix, user -> password, RepositoryError.InvalidStoredData))
      case _ => Left(RepositoryError.InvalidStoredData)
    }
  }

  def fence(config: KafkaConfig, id: String): RepositoryIO[Unit] =
    RepositoryIO.fromEither(credentials(config, id)).flatMap { case (user, password) =>
      val settings = OperationalEventKafkaRuntime
        .saslProperties(Some(user), Some(password), config.saslSecurityProtocol)
        .foldLeft(
          ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
            .withBootstrapServers(config.bootstrapServers)
        ) { case (current, (key, value)) => current.withProperty(key, value) }
      RepositoryIO.fromIOEither(
        GuardedTransactionalProducer
          .resource(TransactionalProducerSettings(id, settings), MkProducer.mkProducerForSync[IO])
          .use(_ => IO.unit)
          .attempt
          .map(_.leftMap(_ => RepositoryError.Unavailable))
      )
    }
}
