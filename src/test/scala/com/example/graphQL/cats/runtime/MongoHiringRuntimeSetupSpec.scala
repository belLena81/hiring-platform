package com.example.graphQL.cats.runtime

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.config.AppConfigFixtures
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import munit.CatsEffectSuite

final class MongoHiringRuntimeSetupSpec extends CatsEffectSuite {
  private val unreachable =
    "include classpath(\"application.conf\")\nmongo.uri=\"mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=300&connectTimeoutMS=300\"\n" +
      "kafka.enabled=false\nvector-search.enabled=false\n"

  override def munitIOTimeout = scala.concurrent.duration.Duration(30, "s")

  test("a failed Mongo setup is reported once and the runtime never opens") {
    val config = AppConfigFixtures
      .withPackagedDefaults(unreachable)
      .fold(errors => fail(s"fixture rejected: $errors"), identity)
    for {
      events <- Ref.of[IO, Vector[LogEvent]](Vector.empty)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          events.update(_ :+ event)
      }
      opened <- MongoHiringRuntime.resource(config, diagnostics).use(_ => IO.pure(true)).attempt
      recorded <- events.get
    } yield {
      assert(opened.isLeft)
      assertEquals(recorded.count(_ == LogEvent.MongoSetupFailed), 1)
      assertEquals(recorded.lastOption, Some(LogEvent.MongoSetupFailed))
    }
  }
}
