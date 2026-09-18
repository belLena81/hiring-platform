package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{Host, Port}
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.graphql.HiringGraphQLServices
import com.example.graphQL.cats.api.http.{Admission, HiringApiRoutes}
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService}
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import com.example.graphQL.cats.config.JwtAuthConfig
import org.http4s.{Response, Status}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.noop.NoOpLogger
import scala.concurrent.duration.*

object HiringPlatformServer {
  def resource(
      host: String,
      port: Int,
      probe: DatabaseProbe,
      diagnostics: Diagnostics,
      admissionPermits: Int,
      hiring: Option[HiringGraphQLServices] = None,
      jwtAuth: Option[JwtAuthConfig] = None,
      userAuthenticator: Option[UserAuthenticator[IO]] = None,
      ensureHiringReady: IO[Boolean] = IO.pure(true)
  ): Resource[IO, Server] =
    for {
      address <- Resource.eval(IO.fromOption(Host.fromString(host))(new IllegalArgumentException("Invalid bind address")))
      bindPort <- Resource.eval(IO.fromOption(Port.fromInt(port))(new IllegalArgumentException("Invalid bind port")))
      admission <- Resource.eval(Admission.create(admissionPermits))
      authenticate = (userAuthenticator, jwtAuth) match {
        case (Some(users), Some(config)) =>
          new JwtActorAuthenticator(config, users, IO.realTimeInstant).authenticate
        case _ =>
          (_: org.http4s.Request[IO]) => IO.pure(None)
      }
      routes = new HiringApiRoutes(new HealthService(probe, diagnostics), diagnostics, admission, hiring, authenticate, ensureHiringReady)
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
