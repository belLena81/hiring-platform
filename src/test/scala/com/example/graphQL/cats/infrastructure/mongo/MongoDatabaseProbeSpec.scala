package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.application.{Diagnostics, HealthService, LogEvent, LogField, LogFields, ProbeResult}
import com.example.graphQL.cats.api.http.{Admission, HiringApiRoutes}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.circe.*
import org.typelevel.ci.CIString

import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

class MongoDatabaseProbeSpec extends CatsEffectSuite {
  test("HTTP readiness through the real Mongo adapter preserves correlation across equal timeout budgets") {
    cats.effect.Resource.fromAutoCloseable(IO.blocking(new ServerSocket(0))).use { socket =>
      for {
        events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
        sink = new Diagnostics {
          def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
            events.update(_ :+ ((event, id, fields)))
        }
        _ <- MongoDatabaseProbe.resource(s"mongodb://127.0.0.1:${socket.getLocalPort}", "foundation", sink).use { probe =>
          for {
            admission <- Admission.create
            http = new HiringApiRoutes(new HealthService(probe, sink), sink, admission).app
            measured <- http(Request[IO](Method.GET, Uri.unsafeFromString("/ready"))).timed
            (elapsed, response) = measured
            body <- response.as[Json]
            captured <- events.get
          } yield {
            val id = response.headers.get(CIString("X-Request-ID")).map(_.head.value)
            val serviceRecords = captured.filter(_._1 == LogEvent.MongoUnavailable)
            val adapterRecords = captured.filter(_._1 == LogEvent.MongoProbeFailed)
            assertEquals(response.status, Status.ServiceUnavailable)
            assertEquals(body.hcursor.get[String]("status"), Right("NOT_READY"))
            assert(id.nonEmpty)
            assert(captured.forall(_._2 == id))
            assertEquals(serviceRecords.size, 1)
            assert(serviceRecords.forall(_._3.get(LogField.Reason)
              .exists(Set("PROBE_TIMEOUT", "DATABASE_UNAVAILABLE").contains)))
            serviceRecords.foreach { case (_, _, fields) =>
              if (fields.get(LogField.Reason).contains("PROBE_TIMEOUT")) {
                assertEquals(fields.get(LogField.ErrorType), Some("java.util.concurrent.TimeoutException"))
                assert(fields.get(LogField.ErrorLocation).exists(LogFields.validPublic(LogField.ErrorLocation, _)))
              } else assertEquals(adapterRecords.size, 1)
            }
            assert(adapterRecords.size <= 1)
            assert(adapterRecords.forall(_._3.get(LogField.Reason)
              .exists(Set("PROBE_TIMEOUT", "DATABASE_TIMEOUT", "DATABASE_NETWORK").contains)))
            assert(captured.filter(_._1 == LogEvent.RequestCompleted).map(_._3.get(LogField.Status)) == Vector(Some("503")))
            assert(elapsed < 4.seconds, clues(elapsed))
          }
        }
      } yield ()
    }
  }

  test("connection metadata retains at most four parsed hosts without URI credentials or options") {
    IO {
      val fields = MongoDatabaseProbe.connectionMetadata(
        "mongodb://synthetic-user:synthetic-password@host1:27017,host2:27017,host3:27017,host4:27017,host5:27017/?appName=synthetic-option",
        "foundation"
      )
      assertEquals(fields.get(LogField.MongoHosts), Some("host1:27017,host2:27017,host3:27017,host4:27017"))
      assertEquals(fields.get(LogField.MongoDatabase), Some("foundation"))
      assert(!fields.toString.contains("synthetic"))
    }
  }

  test("failed ping emits one correlated safe classification and duration; sink failures preserve unavailable") {
    cats.effect.Resource.fromAutoCloseable(IO.blocking(new ServerSocket(0))).use { socket =>
      for {
        events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
        sink = new Diagnostics {
          def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
            events.update(_ :+ ((event, id, fields))) *> IO.raiseError(new IllegalStateException("synthetic-sink-secret"))
        }
        id = Some("fe211944-7015-4e73-8dc1-000000000001")
        result <- MongoDatabaseProbe.resource(
          s"mongodb://synthetic-user:synthetic-password@127.0.0.1:${socket.getLocalPort}/?appName=synthetic-option",
          "foundation", sink).use(_.check(id))
        captured <- events.get
      } yield {
        assertEquals(result, ProbeResult.Unavailable)
        assertEquals(captured.size, 1)
        assert(captured.forall { case (event, requestId, fields) =>
          event == LogEvent.MongoProbeFailed && requestId == id &&
            fields.get(LogField.Reason).exists(Set("PROBE_TIMEOUT", "DATABASE_TIMEOUT", "DATABASE_NETWORK").contains) &&
            fields.get(LogField.ErrorType).exists(LogFields.validPublic(LogField.ErrorType, _)) &&
            fields.get(LogField.DurationMs).flatMap(_.toLongOption).exists(_ >= 0) &&
            fields.get(LogField.MongoHosts).contains(s"127.0.0.1:${socket.getLocalPort}") &&
            fields.get(LogField.MongoDatabase).contains("foundation")
        })
        assert(!captured.toString.contains("synthetic"))
      }
    }
  }
  test("URI options cannot relax pool and driver timeout budgets") {
    IO {
      val settings = MongoDatabaseProbe.effectiveSettings(
        "mongodb://127.0.0.1:27017/?maxPoolSize=200&minPoolSize=20&connectTimeoutMS=90000&socketTimeoutMS=0&serverSelectionTimeoutMS=90000&waitQueueTimeoutMS=90000"
      )
      assertEquals(settings.getConnectionPoolSettings.getMaxSize, 10)
      assertEquals(settings.getConnectionPoolSettings.getMinSize, 0)
      assertEquals(settings.getConnectionPoolSettings.getMaxWaitTime(TimeUnit.MILLISECONDS), 2000L)
      assertEquals(settings.getSocketSettings.getConnectTimeout(TimeUnit.MILLISECONDS), 2000)
      assertEquals(settings.getSocketSettings.getReadTimeout(TimeUnit.MILLISECONDS), 2000)
      assertEquals(settings.getHeartbeatSocketSettings.getConnectTimeout(TimeUnit.MILLISECONDS), 2000)
      assertEquals(settings.getHeartbeatSocketSettings.getReadTimeout(TimeUnit.MILLISECONDS), 2000)
      assertEquals(settings.getClusterSettings.getServerSelectionTimeout(TimeUnit.MILLISECONDS), 2000L)
    }
  }

  test("client acquisition does not ping; unavailable check is bounded") {
    cats.effect.Resource.fromAutoCloseable(IO.blocking(new ServerSocket(0))).use { socket =>
      MongoDatabaseProbe.resource(s"mongodb://127.0.0.1:${socket.getLocalPort}", "foundation")
        .allocated.timeout(1.second).flatMap { case (probe, release) =>
          probe.check.timed.flatMap { case (elapsed, result) =>
            IO {
              assertEquals(result, ProbeResult.Unavailable)
              assert(elapsed < 3.seconds)
            }
          }.guarantee(release)
        }
    }
  }
}
