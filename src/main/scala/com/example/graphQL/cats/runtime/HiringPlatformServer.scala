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
      host: String,
      port: Int,
      app: HttpApp[IO],
      logger: StructuredLogger[IO] = NoOpLogger[IO]
  ): Resource[IO, Server] =
    for {
      address <- Resource.eval(IO.fromOption(Host.fromString(host))(new IllegalArgumentException("Invalid bind address")))
      bindPort <- Resource.eval(IO.fromOption(Port.fromInt(port))(new IllegalArgumentException("Invalid bind port")))
      server <- Resource.make(
        EmberServerBuilder.default[IO]
          .withHost(address)
          .withPort(bindPort)
          .withHttpApp(app)
          .withLogger(logger)
          .withShutdownTimeout(10.seconds)
          .withIdleTimeout(10.seconds)
          .withRequestHeaderReceiveTimeout(5.seconds)
          .withMaxHeaderSize(8192)
          .withMaxConnections(64)
          .withErrorHandler(_ => IO.pure(Response[IO](Status.InternalServerError)))
          .withOnWriteFailure((_, _, _) => IO.unit)
          .withConnectionErrorHandler { case _ => IO.unit }
          .build.allocated
      ) { case (_, release) => release }
    } yield server._1
}
