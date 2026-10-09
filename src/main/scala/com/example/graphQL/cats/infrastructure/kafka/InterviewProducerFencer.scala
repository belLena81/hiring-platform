package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.port.{InterviewPublisherFencer, RepositoryIO, RepositoryError}
import org.apache.kafka.clients.admin.Admin
import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

object InterviewProducerFencer {
  private val RequestTimeoutMillis = 10000
  private val CloseTimeoutSeconds = 5L
  def resource(
      bootstrapServers: String,
      username: String,
      password: String,
      protocol: KafkaSaslSecurityProtocol,
      diagnostics: Diagnostics
  ): Resource[IO, InterviewPublisherFencer] = {
    val acquire = IO.blocking {
      val properties = new Properties()
      val _ = properties.setProperty("bootstrap.servers", bootstrapServers)
      val _ = properties.setProperty("request.timeout.ms", RequestTimeoutMillis.toString)
      val _ = properties.setProperty("default.api.timeout.ms", RequestTimeoutMillis.toString)
      KafkaClientSettings
        .security(Some(username), Some(password), protocol)
        .foreach { case (key, value) => val _ = properties.setProperty(key, value) }
      Admin.create(properties)
    }
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
              if (ids.isEmpty) RepositoryIO.fromEither(Right(()))
              else
                RepositoryIO.fromIOEither(
                  IO.uncancelable { _ =>
                    IO.blocking(admin.fenceProducers(ids.asJava).all())
                      .flatMap(result => IO.blocking(result.get()))
                      .void
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
