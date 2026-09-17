package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Ref, Resource}
import com.github.dockerjava.api.model.ExposedPort
import com.example.graphQL.cats.application.{DatabaseProbe, Diagnostics, HealthService, LogEvent, LogField, ProbeResult}
import com.example.graphQL.cats.api.http.{Admission, HiringApiRoutes}
import io.circe.Json
import munit.CatsEffectSuite
import org.bson.Document
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.circe.*
import org.typelevel.ci.CIString
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.*

class MongoDatabaseProbeIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"

  private final class Standalone extends GenericContainer[Standalone](DockerImageName.parse(image))

  private def container(auth: Boolean): Resource[IO, (Standalone, String)] = {
    val password = UUID.randomUUID().toString
    Resource.make(IO.blocking {
      val instance = new Standalone
      val _ = instance.withExposedPorts(27017)
        .withCommand("mongod", "--bind_ip_all")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
      if (auth) {
        val _ = instance.withEnv("MONGO_INITDB_ROOT_USERNAME", "foundation")
          .withEnv("MONGO_INITDB_ROOT_PASSWORD", password)
      }
      try {
        instance.start()
        (instance, password)
      } catch {
        case error: Throwable =>
          instance.stop()
          throw error
      }
    }) { case (instance, _) => IO.blocking(instance.stop()) }
  }

  private def uri(instance: Standalone): String =
    s"mongodb://${instance.getHost}:${instance.getMappedPort(27017)}"

  private def ready(probe: DatabaseProbe, remaining: Int = 30): IO[Unit] =
    probe.check.flatMap {
      case ProbeResult.Ready => IO.unit
      case _ if remaining > 0 => IO.sleep(200.millis) *> ready(probe, remaining - 1)
      case other => IO.raiseError(new AssertionError(s"Expected ready, received $other"))
    }

  test("standalone readiness, outage/recovery, retained synthetic fixture and resource close") {
    container(auth = false).use { case (instance, _) =>
      MongoDatabaseProbe.resource(uri(instance), "foundation").use { probe =>
        for {
          _ <- ready(probe)
          _ <- MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
            val database = client.getDatabase("foundation")
            for {
              hello <- PublisherBridge.first(database.runCommand(new Document("hello", 1)))
              _ <- IO(assert(hello.exists(document => !document.containsKey("setName"))))
              _ <- PublisherBridge.first(database.getCollection("retention").insertOne(
                new Document("_id", "foundation-fixture").append("value", "synthetic")
              ))
            } yield ()
          }
          _ <- Resource.make(
            IO.blocking(instance.getDockerClient.pauseContainerCmd(instance.getContainerId).exec())
          )(_ => IO.blocking(instance.getDockerClient.unpauseContainerCmd(instance.getContainerId).exec()).void).use { _ =>
            probe.check.flatMap(result => IO(assertEquals(result, ProbeResult.Unavailable)))
          }
          _ <- ready(probe)
          _ <- IO.blocking(instance.getDockerClient.stopContainerCmd(instance.getContainerId).exec())
          _ <- IO.blocking(instance.getDockerClient.startContainerCmd(instance.getContainerId).exec())
          restartedUri <- IO.blocking {
            val info = instance.getDockerClient.inspectContainerCmd(instance.getContainerId).exec()
            val port = info.getNetworkSettings.getPorts.getBindings.get(ExposedPort.tcp(27017))
              .headOption.map(_.getHostPortSpec).getOrElse(throw new AssertionError("Missing Mongo port"))
            s"mongodb://${instance.getHost}:$port"
          }
          _ <- MongoDatabaseProbe.resource(restartedUri, "foundation").use(restarted => ready(restarted))
          closedClient <- MongoDatabaseProbe.clientResource(restartedUri).use { client =>
            PublisherBridge.first(client.getDatabase("foundation").getCollection("retention")
              .find(new Document("_id", "foundation-fixture"))).flatMap { fixture =>
              IO(assert(fixture.exists(_.getString("value") == "synthetic"))).as(client)
            }
          }
          afterClose <- PublisherBridge.first(closedClient.getDatabase("foundation")
            .runCommand(new Document("ping", 1))).attempt
          _ <- IO(assert(afterClose.isLeft))
        } yield ()
      }
    }
  }

  test("authentication failure is classified and valid credentials recover readiness") {
    container(auth = true).use { case (instance, password) =>
      val address = s"${instance.getHost}:${instance.getMappedPort(27017)}"
      val valid = s"mongodb://foundation:$password@$address/?authSource=admin"
      val invalid = s"mongodb://foundation:incorrect@$address/?authSource=admin"
      MongoDatabaseProbe.resource(valid, "foundation").use { authenticated =>
        for {
          _ <- ready(authenticated)
          records <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
          diagnostics = new Diagnostics {
            def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
              records.update(_ :+ (event, id, fields))
          }
          requestId = Some("fe211944-7015-4e73-8dc1-000000000001")
          result <- MongoDatabaseProbe.resource(invalid, "foundation", diagnostics).use(_.check(requestId))
          captured <- records.get
          _ <- IO {
            assertEquals(result, ProbeResult.AuthenticationFailed)
            assertEquals(captured.size, 1)
            assert(captured.forall { case (event, id, fields) =>
              event == LogEvent.MongoProbeFailed && id == requestId &&
                fields.get(LogField.Reason).contains("AUTHENTICATION_FAILED") &&
                fields.get(LogField.ErrorType).contains("com.mongodb.MongoSecurityException") &&
                fields.get(LogField.DurationMs).flatMap(_.toLongOption).exists(_ >= 0)
            })
            assert(!captured.toString.contains("incorrect"))
            assert(!captured.toString.contains(password))
            assert(!captured.toString.contains("authSource"))
          }
          _ <- records.set(Vector.empty)
          _ <- MongoDatabaseProbe.resource(invalid, "foundation", diagnostics).use { rejected =>
            for {
              admission <- Admission.create(16)
              http = new HiringApiRoutes(new HealthService(rejected, diagnostics), diagnostics, admission).app
              response <- http(Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
                .withEntity(Json.obj("query" -> Json.fromString("{ readiness { status } }"))))
              body <- response.as[Json]
              correlated <- records.get
            } yield {
              val id = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
              assertEquals(response.status, Status.Ok)
              assertEquals(body.hcursor.downField("data").downField("readiness").get[String]("status"), Right("NOT_READY"))
              assertEquals(correlated.map(_._1), Vector(LogEvent.MongoProbeFailed, LogEvent.MongoAuthFailed,
                LogEvent.GraphQLCompleted, LogEvent.RequestCompleted))
              assert(id.nonEmpty)
              assert(correlated.forall(_._2 == id))
              assert(correlated.filter(_._1 == LogEvent.MongoProbeFailed).forall(_._3.get(LogField.ErrorType)
                .contains("com.mongodb.MongoSecurityException")))
            }
          }
          throwing = new Diagnostics {
            def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
              throw new IllegalStateException("synthetic-sink-secret")
          }
          unchanged <- MongoDatabaseProbe.resource(invalid, "foundation", throwing).use(_.check(requestId))
          _ <- IO(assertEquals(unchanged, ProbeResult.AuthenticationFailed))
          _ <- ready(authenticated)
        } yield ()
      }
    }
  }
}
