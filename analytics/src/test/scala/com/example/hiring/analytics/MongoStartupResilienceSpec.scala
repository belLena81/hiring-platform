package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.MongoStartupCheck
import com.example.hiring.analytics.adapter.spark.{SparkBlockingExecution, SparkRuntime}
import com.example.hiring.analytics.config.{
  AnalyticsConfigFixtures,
  AnalyticsConfigReaders,
  AnalyticsMongoStartupSettings
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsError.MongoUnreachableReason

import cats.effect.{IO, Ref, Resource}
import cats.effect.testkit.TestControl
import org.apache.spark.sql.SparkSession

import java.net.{InetAddress, UnknownHostException}
import scala.concurrent.duration.*

class MongoStartupResilienceSpec extends munit.CatsEffectSuite {
  private val settings = AnalyticsMongoStartupSettings(
    checkTimeout = 5.seconds,
    initialBackoff = 1.second,
    maxBackoff = 4.seconds,
    maxWait = 10.seconds
  )
  private val unreachable = AnalyticsError.MongoUnreachable(MongoUnreachableReason.DnsResolution)

  /** A check that fails for the first `failures` calls and records the virtual time of every call. */
  private def flakyCheck(failures: Int): IO[(IO[Unit], IO[List[FiniteDuration]])] =
    Ref.of[IO, List[FiniteDuration]](Nil).map { calls =>
      val check = IO.monotonic
        .flatMap(now => calls.modify(previous => (previous :+ now, previous.size)))
        .flatMap(index => if (index < failures) IO.raiseError(unreachable) else IO.unit)
      (check, calls.get)
    }

  private def loggedWarnings: IO[(Ref[IO, List[String]], String => IO[Unit])] =
    Ref.of[IO, List[String]](Nil).map(lines => (lines, line => lines.update(_ :+ line)))

  private val noHost: String => Either[Throwable, Vector[InetAddress]] =
    _ => Left(new UnknownHostException("unresolved"))
  private val resolvesToLoopback: String => Either[Throwable, Vector[InetAddress]] =
    _ => Right(Vector(InetAddress.getByAddress(Array[Byte](127, 0, 0, 1))))

  test("AC-07.1 a waiting command retries with capped exponential backoff and proceeds once Mongo is reachable") {
    val program = for {
      checks <- flakyCheck(failures = 4)
      logs <- loggedWarnings
      _ <- MongoStartupCheck
        .ensureReachableUsing[IO](
          MongoStartupCheck.Mode.AwaitReachable,
          settings.copy(maxWait = 1.minute),
          checks._1,
          logs._2
        )
      times <- checks._2
      warnings <- logs._1.get
    } yield (times, warnings)

    TestControl.executeEmbed(program).map { case (times, warnings) =>
      // Delays 1, 2, 4 and then the 4 second cap.
      assertEquals(times, List(0.seconds, 1.second, 3.seconds, 7.seconds, 11.seconds))
      assertEquals(warnings.size, 4)
      assertEquals(
        warnings.head,
        "MongoDB is not reachable (DNS resolution failed); attempt 1, retrying in 1000 ms"
      )
    }
  }

  test("AC-07.2 a waiting command exits with one sanitized error after the maximum wait") {
    val program = for {
      checks <- flakyCheck(failures = Int.MaxValue)
      logs <- loggedWarnings
      outcome <- MongoStartupCheck
        .ensureReachableUsing[IO](MongoStartupCheck.Mode.AwaitReachable, settings, checks._1, logs._2)
        .attempt
      times <- checks._2
      warnings <- logs._1.get
    } yield (outcome, times, warnings)

    TestControl.executeEmbed(program).map { case (outcome, times, warnings) =>
      // Delays 1, 2 and 4 fit the 10 second budget (7 seconds); a further 4 second delay would exceed it.
      assertEquals(times, List(0.seconds, 1.second, 3.seconds, 7.seconds))
      assertEquals(warnings.size, 3)
      assertEquals(
        outcome,
        Left(AnalyticsError.MongoUnreachable(MongoUnreachableReason.DnsResolution, Some(10.seconds)))
      )
    }
  }

  test("AC-07.3 a one-shot command fails fast after a single check without logging or sleeping") {
    val program = for {
      checks <- flakyCheck(failures = Int.MaxValue)
      logs <- loggedWarnings
      outcome <- MongoStartupCheck
        .ensureReachableUsing[IO](MongoStartupCheck.Mode.FailFast, settings, checks._1, logs._2)
        .attempt
      times <- checks._2
      warnings <- logs._1.get
    } yield (outcome, times, warnings)

    TestControl.executeEmbed(program).map { case (outcome, times, warnings) =>
      assertEquals(outcome, Left(unreachable))
      assertEquals(times, List(0.seconds))
      assertEquals(warnings, Nil)
    }
  }

  test("AC-07.4 no Spark session is acquired while Mongo is unreachable") {
    val program = for {
      sparkAcquisitions <- Ref.of[IO, Int](0)
      checks <- flakyCheck(failures = Int.MaxValue)
      logs <- loggedWarnings
      acquire = (_: SparkBlockingExecution[IO]) =>
        sparkAcquisitions.update(_ + 1) *> IO.raiseError[SparkSession](new IllegalStateException("acquired"))
      outcome <- SparkRuntime
        .managedSparkMongo[IO](
          MongoStartupCheck
            .ensureReachableUsing[IO](MongoStartupCheck.Mode.AwaitReachable, settings, checks._1, logs._2),
          acquire,
          Resource.eval(IO.raiseError(new IllegalStateException("mongo client")))
        )
        .use(_ => IO.unit)
        .attempt
      acquired <- sparkAcquisitions.get
    } yield (outcome, acquired)

    TestControl.executeEmbed(program).map { case (outcome, acquired) =>
      assert(outcome.left.exists(_.isInstanceOf[AnalyticsError.MongoUnreachable]))
      assertEquals(acquired, 0)
    }
  }

  test("AC-07.5 warning and error text carry no host, credentials or stack trace") {
    val uri = "mongodb://app-user:s3cret-pass@mongo-host-xyz:27017/?replicaSet=rs0"
    val check = MongoStartupCheck.probe[IO](uri, noHost, IO.unit)
    val program = for {
      logs <- loggedWarnings
      outcome <- MongoStartupCheck
        .ensureReachableUsing[IO](MongoStartupCheck.Mode.AwaitReachable, settings, check, logs._2)
        .attempt
      warnings <- logs._1.get
    } yield (outcome, warnings)

    TestControl.executeEmbed(program).map { case (outcome, warnings) =>
      val error = outcome.left.getOrElse(fail("expected the wait to be exhausted"))
      assert(warnings.nonEmpty)
      (error.getMessage :: warnings).foreach { text =>
        List("mongo-host-xyz", "app-user", "s3cret-pass", "mongodb://", "\n", "\tat ")
          .foreach(forbidden => assert(!text.contains(forbidden), s"text leaked '$forbidden': $text"))
      }
      assertEquals(error.getCause, null)
    }
  }

  test("AC-07.6 the check separates a DNS failure from a failed server selection and passes when both succeed") {
    val uri = "mongodb://mongo-host-xyz:27017/?replicaSet=rs0"
    for {
      dns <- MongoStartupCheck.probe[IO](uri, noHost, IO.unit).attempt
      selection <- MongoStartupCheck
        .probe[IO](uri, resolvesToLoopback, IO.raiseError(new RuntimeException("timeout")))
        .attempt
      ok <- MongoStartupCheck.probe[IO](uri, resolvesToLoopback, IO.unit).attempt
    } yield {
      assertEquals(dns, Left(AnalyticsError.MongoUnreachable(MongoUnreachableReason.DnsResolution)))
      assertEquals(selection, Left(AnalyticsError.MongoUnreachable(MongoUnreachableReason.ServerSelection)))
      assertEquals(ok, Right(()))
    }
  }

  test("AC-07.7 an invalid Mongo URI is a configuration error that a waiting command does not retry") {
    val program = for {
      attempts <- Ref.of[IO, Int](0)
      counted = attempts.update(_ + 1) *> MongoStartupCheck.probe[IO]("not-a-mongo-uri", noHost, IO.unit)
      outcome <- MongoStartupCheck
        .ensureReachableUsing[IO](MongoStartupCheck.Mode.AwaitReachable, settings, counted, _ => IO.unit)
        .attempt
      count <- attempts.get
    } yield (outcome, count)

    TestControl.executeEmbed(program).map { case (outcome, count) =>
      assert(outcome.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
      assertEquals(count, 1)
    }
  }

  test("AC-07.10 the real check fails with a bounded server selection when nothing listens on the Mongo port") {
    val quick = settings.copy(checkTimeout = 500.milliseconds)
    MongoStartupCheck
      .check[IO]("mongodb://127.0.0.1:1/?directConnection=true", quick)
      .attempt
      .map(outcome =>
        assertEquals(outcome, Left(AnalyticsError.MongoUnreachable(MongoUnreachableReason.ServerSelection)))
      )
  }

  import AnalyticsConfigReaders.given

  private def startup(hocon: String, env: Map[String, String] = Map.empty) =
    AnalyticsConfigReaders.decode[AnalyticsMongoStartupSettings](
      AnalyticsConfigFixtures.source(hocon, env),
      "analytics.mongo.startup"
    )

  private val packagedDefaults: String = {
    val source = scala.io.Source.fromResource("application.conf")
    try source.mkString
    finally source.close()
  }

  test("AC-07.8 the packaged defaults wait 1 s to 60 s for at most 10 minutes and accept environment overrides") {
    assertEquals(
      startup(packagedDefaults).toOption,
      Some(AnalyticsMongoStartupSettings(5.seconds, 1.second, 60.seconds, 10.minutes))
    )
    assertEquals(
      startup(
        packagedDefaults,
        Map(
          "ANALYTICS_MONGO_STARTUP_CHECK_TIMEOUT" -> "2 seconds",
          "ANALYTICS_MONGO_STARTUP_INITIAL_BACKOFF" -> "500 milliseconds",
          "ANALYTICS_MONGO_STARTUP_MAX_BACKOFF" -> "30 seconds",
          "ANALYTICS_MONGO_STARTUP_MAX_WAIT" -> "2 minutes"
        )
      ).toOption,
      Some(AnalyticsMongoStartupSettings(2.seconds, 500.milliseconds, 30.seconds, 2.minutes))
    )
  }

  test("AC-07.9 configuration rejects zero durations and an initial backoff above the cap") {
    List(
      "ANALYTICS_MONGO_STARTUP_CHECK_TIMEOUT" -> "check-timeout",
      "ANALYTICS_MONGO_STARTUP_INITIAL_BACKOFF" -> "initial-backoff",
      "ANALYTICS_MONGO_STARTUP_MAX_BACKOFF" -> "max-backoff",
      "ANALYTICS_MONGO_STARTUP_MAX_WAIT" -> "max-wait"
    ).foreach { case (variable, key) =>
      val rejected = startup(packagedDefaults, Map(variable -> "0 seconds"))
      assert(rejected.isInvalid, s"$key accepted zero")
      assert(rejected.fold(_.toChain.toList.mkString.contains(key), _ => false), s"$key was not reported")
    }
    val inverted = startup(packagedDefaults, Map("ANALYTICS_MONGO_STARTUP_INITIAL_BACKOFF" -> "2 minutes"))
    val reported = inverted.fold(_.toChain.toList.mkString, _ => "")
    assert(reported.contains("initial-backoff must not exceed max-backoff"), reported)
  }
}
