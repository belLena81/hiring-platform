package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.application.{DatabaseProbe, ProbeResult}
import com.mongodb.{ConnectionString, MongoClientSettings, MongoSecurityException}
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients}
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

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

  private[mongo] def clientResource(uri: String): Resource[IO, MongoClient] =
    Resource.make(IO.blocking(MongoClients.create(effectiveSettings(uri))))(client => IO.blocking(client.close()))

  def resource(uri: String, database: String): Resource[IO, DatabaseProbe] =
    clientResource(uri).map { client =>
      new DatabaseProbe {
        override def check: IO[ProbeResult] =
          PublisherBridge.first(client.getDatabase(database).runCommand(new Document("ping", 1)))
            .map {
              case Some(_) => ProbeResult.Ready
              case None => ProbeResult.Unavailable
            }
            .handleError {
              case _: MongoSecurityException => ProbeResult.AuthenticationFailed
              case _ => ProbeResult.Unavailable
            }
            .timeoutTo(2.seconds, IO.pure(ProbeResult.Unavailable))
      }
    }
}
