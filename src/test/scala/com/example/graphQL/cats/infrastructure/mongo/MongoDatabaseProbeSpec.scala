package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.IO
import com.example.graphQL.cats.application.ProbeResult
import munit.CatsEffectSuite

import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

class MongoDatabaseProbeSpec extends CatsEffectSuite {
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
