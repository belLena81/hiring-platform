package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, LogEvent, LogField, LogFields, ProbeResult}
import com.example.graphQL.cats.service.Diagnostics.*
import com.mongodb.{ConnectionString, MongoClientSettings, MongoSecurityException, MongoSocketException, MongoTimeoutException}
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients, MongoDatabase}
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

object MongoDatabaseProbe {
  def effectiveSettings(uri: String): MongoClientSettings =
    MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(uri))
      .applyToConnectionPoolSettings { builder =>
        val _ = builder.minSize(0).maxSize(10).maxWaitTime(2, TimeUnit.SECONDS)
      }
      .applyToClusterSettings { builder =>
        val _ = builder.serverSelectionTimeout(2, TimeUnit.SECONDS)
      }
      .applyToSocketSettings { builder =>
        val _ = builder.connectTimeout(2, TimeUnit.SECONDS).readTimeout(2, TimeUnit.SECONDS)
      }
      .build()

  def clientResource(uri: String): Resource[IO, MongoClient] =
    Resource.make(IO.blocking(MongoClients.create(effectiveSettings(uri))))(client => IO.blocking(client.close()))

  def connectionMetadata(uri: String, database: String): Map[LogField, String] =
    Map(LogField.MongoHosts -> new ConnectionString(uri).getHosts.asScala.take(4).mkString(","),
      LogField.MongoDatabase -> database)

  def resource(uri: String, database: String, diagnostics: Diagnostics = Diagnostics.noop): Resource[IO, DatabaseProbe] =
    clientResource(uri).map(client => fromDatabase(client.getDatabase(database), connectionMetadata(uri, database), diagnostics))

  def fromDatabase(
      database: MongoDatabase,
      metadata: Map[LogField, String],
      diagnostics: Diagnostics = Diagnostics.noop
  ): DatabaseProbe = new DatabaseProbe {
    override def check: IO[ProbeResult] = check(None)

    override def check(requestId: Option[String]): IO[ProbeResult] =
      PublisherBridge.first(database.runCommand(new Document("ping", 1)))
        .map {
          case Some(_) => (ProbeResult.Ready, Map.empty[LogField, String])
          case None => (ProbeResult.Unavailable, Map(LogField.Reason -> "EMPTY_RESULT"))
        }
        .handleError { error =>
          val (result, reason) = error match {
            case _: MongoSecurityException => (ProbeResult.AuthenticationFailed, "AUTHENTICATION_FAILED")
            case _: MongoTimeoutException => (ProbeResult.Unavailable, "DATABASE_TIMEOUT")
            case _: MongoSocketException => (ProbeResult.Unavailable, "DATABASE_NETWORK")
            case _ => (ProbeResult.Unavailable, "DATABASE_ERROR")
          }
          (result, LogFields.failure(error) + (LogField.Reason -> reason))
        }
        .timeoutTo(2.seconds, IO.pure((ProbeResult.Unavailable,
          LogFields.failure(new java.util.concurrent.TimeoutException()) + (LogField.Reason -> "PROBE_TIMEOUT"))))
        .timed.flatMap { case (elapsed, (result, failure)) =>
          val fields = metadata ++ failure ++ Map(LogField.DurationMs -> elapsed.toMillis.toString,
            LogField.Outcome -> "NOT_READY")
          (if (result == ProbeResult.Ready) IO.unit
          else diagnostics.emit(LogEvent.MongoProbeFailed, requestId, fields = fields)).as(result)
        }
  }
}
