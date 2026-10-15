package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Ref, Resource}
import com.comcast.ip4s.{Host, Port}
import com.example.graphQL.cats.api.graphql.TestGraphQLSupport
import com.example.graphQL.cats.api.http.HiringApiRoutes
import com.example.graphQL.cats.config.RateLimitConfig
import com.example.graphQL.cats.service.{
  DatabaseProbe,
  Diagnostics,
  HealthService,
  LogEvent,
  LogField,
  LogFields,
  ProbeResult,
  ReadinessComponent
}
import org.http4s.server.Server
import io.circe.Json
import io.circe.parser.parse
import java.net.{InetSocketAddress, Socket, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class HiringPlatformServerSpec extends CatsEffectSuite {
  private lazy val client = HttpClient
    .newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .connectTimeout(Duration.ofSeconds(2))
    .build()

  private val cleanupBound = 6.seconds
  private val shutdownBound = 14.seconds
  private val AdmissionPermits = 16
  private val authRateLimit = RateLimitConfig(60, 100, 1000)

  private def probe(result: IO[ProbeResult]): DatabaseProbe[IO] = new DatabaseProbe[IO] {
    def check(requestId: Option[String]): IO[ProbeResult] = result
  }

  private def server(
      host: String,
      port: Int,
      database: DatabaseProbe[IO],
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      degraded: Option[IO[List[ReadinessComponent]]] = None
  ): Resource[IO, Server] =
    for {
      dependencies <- TestGraphQLSupport.dependencies(
        TestGraphQLSupport.emptyServices,
        _ => IO.pure(Right(None)),
        hiringReady,
        authRateLimit
      )
      app <- Resource.eval(
        new HiringApiRoutes(
          new HealthService(database, Diagnostics.noop[IO], degraded),
          Diagnostics.noop[IO],
          dependencies
        ).httpApp(HiringApiRoutes.HttpConfig(AdmissionPermits, 5.seconds))
      )
      server <- HiringPlatformServer.resource(
        Host.fromString(host).getOrElse(fail(s"Invalid test host: $host")),
        Port.fromInt(port).getOrElse(fail(s"Invalid test port: $port")),
        app,
        Diagnostics.noop[IO]
      )
    } yield server

  test("P1-AC02 unexpected application failures return 500 and are logged") {
    val failure = new IllegalStateException("synthetic server failure")
    for {
      emitted <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          IO.defer(emitted.update(_ :+ (event, fields)))
      }
      failingApp = org.http4s.HttpApp[IO](_ => IO.raiseError[org.http4s.Response[IO]](failure))
      _ <- HiringPlatformServer
        .resource(
          Host.fromString("127.0.0.1").get,
          Port.fromInt(0).get,
          failingApp,
          diagnostics
        )
        .use { server =>
          request(server.address.getPort, "/failure").map { response =>
            assertEquals(response.statusCode(), 500)
          } *> emitted.get.map { events =>
            assertEquals(events, Vector(LogEvent.RuntimeFailed -> LogFields.failure(failure)))
          }
        }
    } yield ()
  }

  private def request(port: Int, path: String, body: Option[String] = None): IO[HttpResponse[String]] =
    IO.fromCompletableFuture(IO {
      val builder = HttpRequest
        .newBuilder(URI.create(s"http://127.0.0.1:$port$path"))
        .timeout(Duration.ofSeconds(8))
      val httpRequest = body
        .fold(builder.GET()) { payload =>
          builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(payload))
        }
        .build()
      client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
    })

  private def json(response: HttpResponse[String]): Json =
    parse(response.body()).toOption.getOrElse(fail("Expected a JSON response"))

  private def assertCorrelation(response: HttpResponse[String]): Unit = {
    val header = response.headers().firstValue("X-Request-ID")
    assert(header.isPresent)
    assert(scala.util.Try(java.util.UUID.fromString(header.get())).isSuccess)
  }

  test("ARR-07.1 the served readiness lists a degraded publisher with 200 and a lost database with 503") {
    val degraded = Some(IO.pure(List(ReadinessComponent.KafkaPublisher, ReadinessComponent.OutboxAge)))
    for {
      served <- server("127.0.0.1", 0, probe(IO.pure(ProbeResult.Ready)), degraded = degraded).use { running =>
        request(running.address.getPort, "/ready")
      }
      lost <- server("127.0.0.1", 0, probe(IO.pure(ProbeResult.Unavailable)), degraded = degraded).use { running =>
        request(running.address.getPort, "/ready")
      }
    } yield {
      assertEquals(served.statusCode(), 200)
      assertEquals(
        json(served),
        Json.obj(
          "status" -> Json.fromString("READY"),
          "degraded" -> Json.arr(Json.fromString("kafka-publisher"), Json.fromString("outbox-age"))
        )
      )
      assertEquals(lost.statusCode(), 503)
      assertEquals(json(lost), Json.obj("status" -> Json.fromString("NOT_READY")))
    }
  }

  test("P1-AC02/04 live health, readiness and GraphQL use the served contract") {
    for {
      checks <- Ref.of[IO, Int](0)
      _ <- server("127.0.0.1", 0, probe(checks.update(_ + 1).as(ProbeResult.Ready))).use { server =>
        val port = server.address.getPort
        for {
          health <- request(port, "/health")
          graphqlPing <- request(port, "/graphql", Some("""{"query":"{ __typename }"}"""))
          healthChecks <- checks.get
          ready <- request(port, "/ready")
          totalChecks <- checks.get
          graphiql <- request(port, "/graphiql")
        } yield {
          assertEquals(health.statusCode(), 200)
          assertEquals(json(health).hcursor.get[String]("status"), Right("UP"))
          assertEquals(graphqlPing.statusCode(), 200)
          assertEquals(json(graphqlPing).hcursor.downField("data").get[String]("__typename"), Right("Query"))
          assertEquals(healthChecks, 0)
          assertEquals(ready.statusCode(), 200)
          assertEquals(json(ready).hcursor.get[String]("status"), Right("READY"))
          assertEquals(totalChecks, 1)
          assertEquals(graphiql.statusCode(), 404)
          List(health, graphqlPing, ready).foreach(assertCorrelation)
        }
      }
    } yield ()
  }

  test("P1-AC04 live database failure keeps health up, GraphQL at 200, and recovers") {
    for {
      result <- Ref.of[IO, ProbeResult](ProbeResult.Unavailable)
      _ <- server("127.0.0.1", 0, probe(result.get)).use { server =>
        val port = server.address.getPort
        for {
          health <- request(port, "/health")
          ready <- request(port, "/ready")
          graphql <- request(port, "/graphql", Some("""{"query":"{ __typename }"}"""))
          _ <- result.set(ProbeResult.Ready)
          recovered <- request(port, "/ready")
        } yield {
          assertEquals(health.statusCode(), 200)
          assertEquals(ready.statusCode(), 503)
          assertEquals(json(ready).hcursor.get[String]("status"), Right("NOT_READY"))
          assertEquals(graphql.statusCode(), 200)
          assertEquals(json(graphql).hcursor.downField("data").get[String]("__typename"), Right("Query"))
          assertEquals(recovered.statusCode(), 200)
        }
      }
    } yield ()
  }

  private def resetConnection(port: Int, path: String, entered: Deferred[IO, Unit]): IO[Unit] = {
    val body = """{"query":"{ me { id } }"}"""
    val wire =
      if (path == "/ready") s"GET /ready HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n\r\n"
      else
        s"POST /graphql HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nContent-Type: application/json\r\nContent-Length: ${body.getBytes(StandardCharsets.UTF_8).length}\r\n\r\n$body"
    Resource.make(IO.blocking(new Socket()))(socket => IO.blocking(socket.close())).use { socket =>
      IO.blocking {
        socket.connect(new InetSocketAddress("127.0.0.1", port), 2000)
        socket.setSoLinger(true, 0)
        socket.getOutputStream.write(wire.getBytes(StandardCharsets.UTF_8))
        socket.getOutputStream.flush()
      } *> entered.get.timeout(cleanupBound)
    }
  }

  List("/ready", "/graphql").foreach { path =>
    test(
      s"P1-AC06 socket RST on $path: deadline-driven cleanup within two seconds plus four-second harness tolerance"
    ) {
      for {
        entered <- Deferred[IO, Unit]
        finalized <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        calls <- Ref.of[IO, Int](0)
        blocking = calls.getAndUpdate(_ + 1).flatMap {
          case 0 => (entered.complete(()).void *> IO.never[ProbeResult]).onCancel(finalized.complete(()).void)
          case _ => IO.pure(ProbeResult.Ready)
        }
        // GraphQL no longer calls the database probe, so a stuck GraphQL request blocks in the hiring-readiness check.
        fake = if (path == "/ready") probe(blocking) else probe(IO.pure(ProbeResult.Ready))
        // The per-request hiring-readiness check is memoized and is not cancelled with the request, so the GraphQL
        // variant releases it explicitly instead of waiting for a cancellation.
        stuckHiringReady = entered.complete(()).void *> release.get.as(ProbeResult.Ready)
        hiringReady = if (path == "/graphql") stuckHiringReady else IO.pure(ProbeResult.Ready)
        _ <- server("127.0.0.1", 0, fake, hiringReady).use { server =>
          for {
            _ <- resetConnection(server.address.getPort, path, entered)
            resetAt <- IO.monotonic
            _ <- if (path == "/ready") finalized.get.timeout(cleanupBound) else release.complete(()).void
            elapsed <- IO.monotonic.map(_ - resetAt)
            response <- request(server.address.getPort, "/ready")
            count <- calls.get
          } yield {
            assert(elapsed < cleanupBound, clues(elapsed))
            assertEquals(response.statusCode(), 200)
            assertEquals(json(response).hcursor.get[String]("status"), Right("READY"))
            assertEquals(count, if (path == "/ready") 2 else 0)
          }
        }
      } yield ()
    }
  }

  test("P1-AC02 releasing the server permits immediate rebind of the same port") {
    val ready = probe(IO.pure(ProbeResult.Ready))
    for {
      port <- server("127.0.0.1", 0, ready).use { server =>
        request(server.address.getPort, "/health").map { response =>
          assertEquals(response.statusCode(), 200)
          server.address.getPort
        }
      }
      _ <- server("127.0.0.1", port, ready).use { server =>
        request(server.address.getPort, "/health").map(response => assertEquals(response.statusCode(), 200))
      }
    } yield ()
  }

  test("P1-AC02 bind failure releases an already acquired dependency and leaves the original server usable") {
    val ready = probe(IO.pure(ProbeResult.Ready))
    for {
      acquired <- Ref.of[IO, Int](0)
      released <- Ref.of[IO, Int](0)
      dependency = Resource.make(acquired.update(_ + 1).as(ready))(_ => released.update(_ + 1))
      _ <- server("127.0.0.1", 0, ready).use { original =>
        for {
          result <- dependency
            .flatMap { database =>
              server("127.0.0.1", original.address.getPort, database)
            }
            .use(_ => IO.unit)
            .attempt
            .timeout(10.seconds)
          acquisitionCount <- acquired.get
          releaseCount <- released.get
          response <- request(original.address.getPort, "/health")
        } yield {
          assert(result.isLeft)
          assertEquals(acquisitionCount, 1)
          assertEquals(releaseCount, 1)
          assertEquals(response.statusCode(), 200)
        }
      }
    } yield ()
  }

  test("P1-AC02 acquisition failure after server creation releases server and outer dependency") {
    val ready = probe(IO.pure(ProbeResult.Ready))
    for {
      boundPort <- Deferred[IO, Int]
      released <- Ref.of[IO, Boolean](false)
      failure = new RuntimeException("synthetic acquisition failure")
      result <- (for {
        database <- Resource.make(IO.pure(ready))(_ => released.set(true))
        server <- server("127.0.0.1", 0, database)
        _ <- Resource.eval(boundPort.complete(server.address.getPort).void)
        _ <- Resource.eval(IO.raiseError[Unit](failure))
      } yield ()).use(_ => IO.unit).attempt
      dependencyReleased <- released.get
      port <- boundPort.get.timeout(cleanupBound)
      _ <- server("127.0.0.1", port, ready).use { server =>
        request(server.address.getPort, "/health").map(response => assertEquals(response.statusCode(), 200))
      }
    } yield {
      assertEquals(result, Left(failure))
      assert(dependencyReleased)
    }
  }

  test("P1-AC02 shutdown cleans the probe within 2s + 4s tolerance and releases the server within 10s + 4s tolerance") {
    for {
      entered <- Deferred[IO, Unit]
      finalized <- Deferred[IO, Unit]
      boundPort <- Deferred[IO, Int]
      shutdown <- Deferred[IO, Unit]
      fake = probe((entered.complete(()).void *> IO.never[ProbeResult]).onCancel(finalized.complete(()).void))
      _ <- server("127.0.0.1", 0, fake)
        .use { server =>
          boundPort.complete(server.address.getPort).void *> shutdown.get
        }
        .background
        .use { completion =>
          boundPort.get.timeout(cleanupBound).flatMap { port =>
            request(port, "/ready").attempt.background.use { _ =>
              for {
                _ <- entered.get.timeout(cleanupBound)
                _ <- shutdown.complete(())
                _ <- IO.both(
                  finalized.get.timeout(cleanupBound),
                  completion.flatMap(_.embedNever).timeout(shutdownBound)
                )
                canceled <- finalized.tryGet
                _ <- server("127.0.0.1", port, probe(IO.pure(ProbeResult.Ready))).use { restarted =>
                  request(restarted.address.getPort, "/health")
                    .map(response => assertEquals(response.statusCode(), 200))
                }
              } yield assertEquals(canceled, Some(()))
            }
          }
        }
    } yield ()
  }
}
