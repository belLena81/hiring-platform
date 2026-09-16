package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Ref, Resource}
import com.example.graphQL.cats.application.{DatabaseProbe, Diagnostics, ProbeResult}
import io.circe.Json
import io.circe.parser.parse
import java.net.{InetSocketAddress, Socket, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class FoundationServerSpec extends CatsEffectSuite {
  private lazy val client = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .connectTimeout(Duration.ofSeconds(2))
    .build()

  private val cleanupBound = 6.seconds
  private val shutdownBound = 14.seconds

  private def probe(result: IO[ProbeResult]): DatabaseProbe = new DatabaseProbe {
    def check: IO[ProbeResult] = result
  }

  private def request(port: Int, path: String, body: Option[String] = None): IO[HttpResponse[String]] =
    IO.fromCompletableFuture(IO {
      val builder = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path"))
        .timeout(Duration.ofSeconds(8))
      val httpRequest = body.fold(builder.GET()) { payload =>
        builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(payload))
      }.build()
      client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
    })

  private def json(response: HttpResponse[String]): Json =
    parse(response.body()).toOption.getOrElse(fail("Expected a JSON response"))

  private def assertCorrelation(response: HttpResponse[String]): Unit = {
    val header = response.headers().firstValue("X-Request-ID")
    assert(header.isPresent)
    assertEquals(UUID.fromString(header.orElseThrow()).toString, header.orElseThrow())
  }

  test("P1-AC02/04 live health, readiness and GraphQL use the served contract") {
    for {
      checks <- Ref.of[IO, Int](0)
      _ <- FoundationServer.resource("127.0.0.1", 0,
        probe(checks.update(_ + 1).as(ProbeResult.Ready)), Diagnostics.noop).use { server =>
        val port = server.address.getPort
        for {
          health <- request(port, "/health")
          graphqlHealth <- request(port, "/graphql", Some("""{"query":"{ health { status } }"}"""))
          healthChecks <- checks.get
          ready <- request(port, "/ready")
          graphqlReady <- request(port, "/graphql", Some("""{"query":"{ first: readiness { status } second: readiness { status } }"}"""))
          totalChecks <- checks.get
          graphiql <- request(port, "/graphiql")
        } yield {
          assertEquals(health.statusCode(), 200)
          assertEquals(json(health).hcursor.get[String]("status"), Right("UP"))
          assertEquals(graphqlHealth.statusCode(), 200)
          assertEquals(json(graphqlHealth).hcursor.downField("data").downField("health").get[String]("status"), Right("UP"))
          assertEquals(healthChecks, 0)
          assertEquals(ready.statusCode(), 200)
          assertEquals(json(ready).hcursor.get[String]("status"), Right("READY"))
          assertEquals(graphqlReady.statusCode(), 200)
          List("first", "second").foreach { alias =>
            assertEquals(json(graphqlReady).hcursor.downField("data").downField(alias).get[String]("status"), Right("READY"))
          }
          assertEquals(totalChecks, 2)
          assertEquals(graphiql.statusCode(), 404)
          List(health, graphqlHealth, ready, graphqlReady).foreach(assertCorrelation)
        }
      }
    } yield ()
  }

  test("P1-AC04 live database failure keeps health up, GraphQL at 200, and recovers") {
    for {
      result <- Ref.of[IO, ProbeResult](ProbeResult.Unavailable)
      _ <- FoundationServer.resource("127.0.0.1", 0, probe(result.get), Diagnostics.noop).use { server =>
        val port = server.address.getPort
        for {
          health <- request(port, "/health")
          ready <- request(port, "/ready")
          graphql <- request(port, "/graphql", Some("""{"query":"{ readiness { status } }"}"""))
          _ <- result.set(ProbeResult.Ready)
          recovered <- request(port, "/ready")
        } yield {
          assertEquals(health.statusCode(), 200)
          assertEquals(ready.statusCode(), 503)
          assertEquals(json(ready).hcursor.get[String]("status"), Right("NOT_READY"))
          assertEquals(graphql.statusCode(), 200)
          assertEquals(json(graphql).hcursor.downField("data").downField("readiness").get[String]("status"), Right("NOT_READY"))
          assertEquals(recovered.statusCode(), 200)
        }
      }
    } yield ()
  }

  private def resetConnection(port: Int, path: String, entered: Deferred[IO, Unit]): IO[Unit] = {
    val body = """{"query":"{ readiness { status } }"}"""
    val wire = if (path == "/ready") s"GET /ready HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n\r\n"
      else s"POST /graphql HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nContent-Type: application/json\r\nContent-Length: ${body.getBytes(StandardCharsets.UTF_8).length}\r\n\r\n$body"
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
    test(s"P1-AC06 socket RST on $path: deadline-driven cleanup within two seconds plus four-second harness tolerance") {
      for {
        entered <- Deferred[IO, Unit]
        finalized <- Deferred[IO, Unit]
        calls <- Ref.of[IO, Int](0)
        fake = probe(calls.getAndUpdate(_ + 1).flatMap {
          case 0 => (entered.complete(()).void *> IO.never[ProbeResult]).onCancel(finalized.complete(()).void)
          case _ => IO.pure(ProbeResult.Ready)
        })
        _ <- FoundationServer.resource("127.0.0.1", 0, fake, Diagnostics.noop).use { server =>
          for {
            _ <- resetConnection(server.address.getPort, path, entered)
            resetAt <- IO.monotonic
            _ <- finalized.get.timeout(cleanupBound)
            elapsed <- IO.monotonic.map(_ - resetAt)
            response <- request(server.address.getPort, "/ready")
            count <- calls.get
          } yield {
            assert(elapsed < cleanupBound, clues(elapsed))
            assertEquals(response.statusCode(), 200)
            assertEquals(json(response).hcursor.get[String]("status"), Right("READY"))
            assertEquals(count, 2)
          }
        }
      } yield ()
    }
  }

  test("P1-AC02 releasing the server permits immediate rebind of the same port") {
    val ready = probe(IO.pure(ProbeResult.Ready))
    for {
      port <- FoundationServer.resource("127.0.0.1", 0, ready, Diagnostics.noop).use { server =>
        request(server.address.getPort, "/health").map { response =>
          assertEquals(response.statusCode(), 200)
          server.address.getPort
        }
      }
      _ <- FoundationServer.resource("127.0.0.1", port, ready, Diagnostics.noop).use { server =>
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
      _ <- FoundationServer.resource("127.0.0.1", 0, ready, Diagnostics.noop).use { original =>
        for {
          result <- dependency.flatMap { database =>
            FoundationServer.resource("127.0.0.1", original.address.getPort, database, Diagnostics.noop)
          }.use(_ => IO.unit).attempt.timeout(10.seconds)
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
        server <- FoundationServer.resource("127.0.0.1", 0, database, Diagnostics.noop)
        _ <- Resource.eval(boundPort.complete(server.address.getPort).void)
        _ <- Resource.eval(IO.raiseError[Unit](failure))
      } yield ()).use(_ => IO.unit).attempt
      dependencyReleased <- released.get
      port <- boundPort.get.timeout(cleanupBound)
      _ <- FoundationServer.resource("127.0.0.1", port, ready, Diagnostics.noop).use { server =>
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
      _ <- FoundationServer.resource("127.0.0.1", 0, fake, Diagnostics.noop).use { server =>
        boundPort.complete(server.address.getPort).void *> shutdown.get
      }.background.use { completion =>
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
              _ <- FoundationServer.resource("127.0.0.1", port,
                probe(IO.pure(ProbeResult.Ready)), Diagnostics.noop).use { restarted =>
                request(restarted.address.getPort, "/health").map(response => assertEquals(response.statusCode(), 200))
              }
            } yield assertEquals(canceled, Some(()))
          }
        }
      }
    } yield ()
  }
}
