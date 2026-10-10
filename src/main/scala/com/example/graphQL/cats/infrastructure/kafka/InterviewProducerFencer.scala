package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.port.{InterviewPublisherFencer, RepositoryIO}
import com.example.graphQL.cats.service.RepositoryError
import org.apache.kafka.clients.admin.Admin

import java.time.Duration
import scala.jdk.CollectionConverters.*

object InterviewProducerFencer {
  private val CloseTimeoutSeconds = 5L
  def resource(
      bootstrapServers: String,
      username: String,
      password: String,
      protocol: KafkaSaslSecurityProtocol,
      diagnostics: Diagnostics
  ): Resource[IO, InterviewPublisherFencer] = {
    val acquire = IO.blocking(
      Admin.create(KafkaClientSettings.clientProperties(bootstrapServers, Some(username), Some(password), protocol))
    )
    Resource.make(acquire)(admin => IO.blocking(admin.close(Duration.ofSeconds(CloseTimeoutSeconds)))).map { admin =>
      new InterviewPublisherFencer {
        override def fence(transactionalIds: Vector[String]): RepositoryIO[Unit] =
          RepositoryIO
            .fromEither(
              InterviewSubjectCleanup
                .validateProducerIds(transactionalIds)
                .leftMap(_ => RepositoryError.InvalidStoredData)
            )
            .flatMap { ids =>
              if (ids.isEmpty) ().pure[RepositoryIO]
              else
                RepositoryIO.fromIOEither(
                  IO.uncancelable { _ =>
                    IO.fromCompletableFuture(
                      IO.blocking(admin.fenceProducers(ids.asJava).all().toCompletionStage.toCompletableFuture)
                    ).void
                  }.attempt
                    .flatMap {
                      case Right(_)    => IO.pure(Right(()))
                      case Left(error) =>
                        diagnostics
                          .emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
                          .as(Left(RepositoryError.Unavailable))
                    }
                )
            }
      }
    }
  }
}
