package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import com.example.hiring.analytics.StreamingIngestionFixtures.*
import com.example.hiring.analytics.app.StreamingDurableBoundary
import munit.CatsEffectSuite
import org.bson.Document

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Real operating-system signals against streaming processes: crash recovery, ownership after a kill and a clean stop.
  */
final class HiringAnalyticsStreamingProcessSignalIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 30.minutes

  private val eventCount = 12

  /** A child JVM on this test's classpath; its output goes to a file so it can never block on a full pipe. */
  private def spawn(
      fixture: StreamingIngestionFixtures.Fixture,
      mainClass: String,
      name: String,
      properties: Map[String, String]
  ): Resource[IO, Process] =
    Resource.make(IO.blocking {
      val config = fixture.root.resolve(name + ".conf")
      Files.writeString(config, configurationText(fixture, 13))
      val command = Vector(
        Path.of(System.getProperty("java.home"), "bin", "java").toString,
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "-cp",
        System.getProperty("java.class.path"),
        s"-Dconfig.file=$config",
        s"-Djacoco-agent.destfile=${fixture.root.resolve(name + ".exec")}"
      ) ++ properties.map { case (key, value) => s"-D$key=$value" } :+ mainClass
      new ProcessBuilder(command*)
        .redirectErrorStream(true)
        .redirectOutput(fixture.root.resolve(name + ".log").toFile)
        .start()
    })(process =>
      IO.blocking {
        if (process.isAlive) { val _ = process.destroyForcibly().waitFor(30L, TimeUnit.SECONDS) }
      }
    )

  private def exited(process: Process, within: FiniteDuration): IO[Int] =
    IO.blocking(process.waitFor(within.toSeconds, TimeUnit.SECONDS)).flatMap {
      case true  => IO.blocking(process.exitValue())
      case false => IO.raiseError(new AssertionError(s"the process did not exit within $within"))
    }

  /** Bounded polling that fails at once if the child has died, with the tail of its output. */
  private def eventually(
      fixture: StreamingIngestionFixtures.Fixture,
      process: Process,
      name: String,
      description: String,
      attempts: Int = 600
  )(
      check: IO[Boolean]
  ): IO[Unit] =
    check.flatMap { done =>
      if (done) IO.unit
      else if (!process.isAlive)
        IO.blocking(Files.readAllLines(fixture.root.resolve(name + ".log")).toArray.takeRight(40).mkString("\n"))
          .flatMap(tail => IO.raiseError(new AssertionError(s"the process ended before $description:\n$tail")))
      else if (attempts == 0) IO.raiseError(new AssertionError(s"timed out waiting for $description"))
      else IO.sleep(500.millis) *> eventually(fixture, process, name, description, attempts - 1)(check)
    }

  private def mutexRows(fixture: StreamingIngestionFixtures.Fixture): IO[Long] = IO.blocking {
    val sync = com.mongodb.client.MongoClients.create(fixture.namespace.manifest.mongoUri)
    try sync.getDatabase(fixture.databaseName).getCollection("analytics_lakehouse_mutexes").countDocuments()
    finally sync.close()
  }

  /** The documented manual recovery after a crashed owner: delete that owner's mutex rows. */
  private def clearMutexRows(fixture: StreamingIngestionFixtures.Fixture): IO[Unit] = IO.blocking {
    val sync = com.mongodb.client.MongoClients.create(fixture.namespace.manifest.mongoUri)
    try {
      val _ =
        sync.getDatabase(fixture.databaseName).getCollection("analytics_lakehouse_mutexes").deleteMany(new Document())
    } finally sync.close()
  }

  private def killedAfterPublication(fixture: StreamingIngestionFixtures.Fixture, name: String): IO[Unit] = {
    val marker = fixture.root.resolve(name + ".reached")
    spawn(
      fixture,
      "com.example.hiring.analytics.StreamingProcessChild",
      name,
      Map(
        "hiring.child.marker" -> marker.toString,
        "hiring.child.boundary" -> StreamingDurableBoundary.PublicationCommitted.toString
      )
    ).use { child =>
      for {
        _ <- eventually(fixture, child, name, "the child to reach PublicationCommitted")(
          IO.blocking(Files.exists(marker))
        )
        _ <- IO.blocking(child.destroyForcibly())
        code <- exited(child, 1.minute)
      } yield assert(code != 0, "the killed process does not exit cleanly")
    }
  }

  test("HCS-07.9 a stream killed after PublicationCommitted converges once its owner's mutex rows are cleared") {
    fixture.use { fixture =>
      for {
        settings <- settings(fixture, 13)
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until eventCount)))
        _ <- grantActivation(fixture, settings)
        _ <- killedAfterPublication(fixture, "killed-owner")
        stale <- mutexRows(fixture)
        _ = assert(stale > 0L, "the killed owner left durable mutex rows")
        _ <- clearMutexRows(fixture)
        _ <- runToCompletion(settings).timeout(10.minutes)
        counts <- stored(fixture, settings)
        snapshot <- reportSnapshot(fixture)
      } yield {
        assertEquals(counts, Stored(eventCount.toLong, eventCount.toLong, 1L))
        assertEquals(snapshot.map(_.getString("state")), Some("Published"))
      }
    }
  }

  test("HCS-07.10 a restart that finds the killed owner's rows does not acquire the lakehouse and is cancelled") {
    fixture.use { fixture =>
      for {
        settings <- settings(fixture, 13)
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until eventCount)))
        _ <- grantActivation(fixture, settings)
        _ <- killedAfterPublication(fixture, "killed-owner-blocked")
        stale <- mutexRows(fixture)
        versions <- deltaVersions(Path.of(java.net.URI.create(settings.common.lakehouseRoot)))
        restart <- runToCompletion(settings).start
        // The restart needs its Spark session first; a long window therefore covers many acquisition attempts.
        early <- restart.join.timeout(60.seconds).attempt
        _ = assert(early.isLeft, s"the restart must keep waiting for the killed owner's mutex: $early")
        _ <- restart.cancel.timeout(2.minutes)
        after <- mutexRows(fixture)
        afterVersions <- deltaVersions(Path.of(java.net.URI.create(settings.common.lakehouseRoot)))
      } yield {
        assertEquals(after, stale, "only the killed owner's rows remain")
        assertEquals(afterVersions, versions, "the blocked restart wrote nothing")
      }
    }
  }

  test("HCS-08.7 SIGTERM during an active callback stops the real streaming process cleanly") {
    fixture.use { fixture =>
      for {
        settings <- settings(fixture, 13)
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until eventCount)))
        _ <- grantActivation(fixture, settings)
        name = "terminated-owner"
        outcome <- spawn(fixture, "com.example.hiring.analytics.cli.HiringAnalyticsStreamingMain", name, Map.empty)
          .use { child =>
            for {
              // The stream owner plus the callback each own one durable mutex: two rows mean a callback is running.
              _ <- eventually(fixture, child, name, "an active callback")(mutexRows(fixture).map(_ == 2L))
              _ <- IO.blocking(child.destroy())
              code <- exited(child, 3.minutes)
            } yield code
          }
        rows <- mutexRows(fixture)
      } yield {
        assertEquals(outcome, 0, "the process exits cleanly on SIGTERM")
        assertEquals(rows, 0L, "the mutexes are released")
        assert(!committedBatch(fixture, 0L), "the in-flight batch has no Spark commit record")
      }
    }
  }
}
