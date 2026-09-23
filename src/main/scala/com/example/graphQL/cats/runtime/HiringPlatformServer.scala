package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{Host, Port}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import org.http4s.{HttpApp, Response, Status}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.noop.NoOpLogger
import scala.concurrent.duration.*

object HiringPlatformServer {
  def resource(
      host: Host,
      port: Port,
      app: HttpApp[IO],
      diagnostics: Diagnostics = Diagnostics.noop
  ): Resource[IO, Server] =
    EmberServerBuilder
      .default[IO]
      .withHost(host)
      .withPort(port)
      .withHttpApp(app)
      .withLogger(NoOpLogger[IO])
      .withShutdownTimeout(10.seconds)
      .withIdleTimeout(10.seconds)
      .withRequestHeaderReceiveTimeout(5.seconds)
      .withMaxHeaderSize(8192)
      .withMaxConnections(64)
      .withErrorHandler { case error =>
        diagnostics
          .emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
          .as(Response[IO](Status.InternalServerError))
      }
      .build
}
