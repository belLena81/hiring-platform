package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{Host, Port}
import com.example.graphQL.cats.api.http.{Admission, HiringApiRoutes}
import com.example.graphQL.cats.application.{DatabaseProbe, Diagnostics, HealthService}
import org.http4s.{Response, Status}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.noop.NoOpLogger
import scala.concurrent.duration.*

object HiringPlatformServer {
  def resource(host: String, port: Int, probe: DatabaseProbe, diagnostics: Diagnostics): Resource[IO, Server] =
    for {
      address <- Resource.eval(IO.fromOption(Host.fromString(host))(new IllegalArgumentException("Invalid bind address")))
      bindPort <- Resource.eval(IO.fromOption(Port.fromInt(port))(new IllegalArgumentException("Invalid bind port")))
      admission <- Resource.eval(Admission.create)
      routes = new HiringApiRoutes(new HealthService(probe, diagnostics), diagnostics, admission)
      server <- Resource.make(
        EmberServerBuilder.default[IO]
          .withHost(address)
          .withPort(bindPort)
          .withHttpApp(routes.app)
          .withLogger(NoOpLogger[IO])
          .withShutdownTimeout(10.seconds)
          .withIdleTimeout(10.seconds)
          .withRequestHeaderReceiveTimeout(5.seconds)
          .withMaxHeaderSize(8192)
          .withMaxConnections(64)
          .withErrorHandler(_ => IO.pure(Response[IO](Status.InternalServerError)))
          .withOnWriteFailure((_, _, _) => IO.unit)
          .withConnectionErrorHandler { case _ => IO.unit }
          .build.allocated
      ) { case (_, release) => admission.close *> release }
    } yield server._1
}
