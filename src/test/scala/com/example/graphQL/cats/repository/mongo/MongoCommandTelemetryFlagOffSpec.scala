package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** HOR-03: with fiber context tracking off (the container default) Mongo telemetry is inert: no exception, the duration
  * histogram is still recorded, and no span is parented.
  */
final class MongoCommandTelemetryFlagOffSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 2.minutes

  test("HOR-03 without -Dcats.effect.trackFiberContext a real client records the histogram and creates no span") {
    IO.blocking {
      val javaBinary = System.getProperty("java.home") + "/bin/java"
      // A child JVM: this suite's own JVM runs with the flag on, the probe must run with it off.
      val process = new ProcessBuilder(
        javaBinary,
        "-cp",
        System.getProperty("java.class.path"),
        "com.example.graphQL.cats.repository.mongo.MongoFlagOffProbe"
      ).redirectErrorStream(true).start()
      val output = scala.io.Source.fromInputStream(process.getInputStream).mkString
      val finished = process.waitFor(90, java.util.concurrent.TimeUnit.SECONDS)
      if (!finished) process.destroyForcibly(): Unit
      (finished, if (finished) process.exitValue() else -1, output)
    }.map { case (finished, code, output) =>
      assert(finished, clue(output))
      assertEquals(code, 0, clue(output))
      assert(output.contains("FLAG_OFF_OK"), clue(output))
    }
  }
}
