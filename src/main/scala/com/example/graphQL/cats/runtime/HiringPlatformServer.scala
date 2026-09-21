package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{Host, Port}
import org.http4s.{HttpApp, Response, Status}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.StructuredLogger
import scala.concurrent.duration.*

object HiringPlatformServer {
  def resource(
      host: Host,
      port: Port,
      app: HttpApp[IO],
      logger: StructuredLogger[IO] = NoOpLogger[IO]
  ): Resource[IO, Server] =
    EmberServerBuilder.default[IO]
      .withHost(host)
      .withPort(port)
      .withHttpApp(app)
      .withLogger(logger)
      .withShutdownTimeout(10.seconds)
      .withIdleTimeout(10.seconds)
      .withRequestHeaderReceiveTimeout(5.seconds)
      .withMaxHeaderSize(8192)
      .withMaxConnections(64)
      .withErrorHandler { case error =>
        logger.error(error)("unhandled").as(Response[IO](Status.InternalServerError))
      }
      .build
}
