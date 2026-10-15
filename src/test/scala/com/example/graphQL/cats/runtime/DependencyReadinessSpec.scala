package com.example.graphQL.cats.runtime

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.config.{CircuitBreakerConfig, ResilienceConfig}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, ProbeResult}
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class DependencyReadinessSpec extends CatsEffectSuite {
  private val quick = CircuitBreakerConfig(enabled = true, 2, 10000, 100, 1000, 1)

  private def openMongoWrites(diagnostics: Diagnostics[IO] = Diagnostics.noop): IO[DependencyBreakers] =
    for {
      breakers <- DependencyBreakers.create(ResilienceConfig(mongoWrites = quick, voyage = quick), diagnostics)
      fail = breakers.mongoWrites
        .protect(IO.raiseError[Unit](new RuntimeException("down")))(CircuitBreaker.classifier(_ => true))
        .attempt
      _ <- fail *> fail
    } yield breakers

  test("CBR-06.4 an open Mongo-writes breaker reports Unavailable and a closed one stays Ready") {
    for {
      closed <- DependencyBreakers.create(ResilienceConfig(), Diagnostics.noop)
      ready <- MongoHiringRuntime.readiness(ProbeResult.Ready, closed.mongoWrites)
      breakers <- openMongoWrites()
      unavailable <- MongoHiringRuntime.readiness(ProbeResult.Ready, breakers.mongoWrites)
    } yield {
      assertEquals(ready, ProbeResult.Ready)
      assertEquals(unavailable, ProbeResult.Unavailable)
    }
  }

  test("CBR-06.4 readiness recovers once the open period ends so traffic can probe the breaker") {
    for {
      breakers <- openMongoWrites()
      _ <- IO.sleep(200.millis)
      recovered <- MongoHiringRuntime.readiness(ProbeResult.Ready, breakers.mongoWrites)
    } yield assertEquals(recovered, ProbeResult.Ready)
  }

  test("CBR-06.4 a failed ping is reported as-is and other open breakers do not change readiness") {
    for {
      breakers <- openMongoWrites()
      failed <- MongoHiringRuntime.readiness(ProbeResult.AuthenticationFailed, breakers.mongoWrites)
      others <- DependencyBreakers.create(ResilienceConfig(voyage = quick), Diagnostics.noop)
      _ <- List
        .fill(2)(
          others.voyage
            .protect(IO.raiseError[Unit](new RuntimeException("down")))(CircuitBreaker.classifier(_ => true))
            .attempt
        )
        .sequence
      ready <- MongoHiringRuntime.readiness(ProbeResult.Ready, others.mongoWrites)
    } yield {
      assertEquals(failed, ProbeResult.AuthenticationFailed)
      assertEquals(ready, ProbeResult.Ready)
    }
  }

  test("CBR-02.5 a state change is reported once with the breaker name and no request data") {
    for {
      events <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          events.update(_ :+ (event -> fields))
      }
      _ <- openMongoWrites(diagnostics)
      observed <- events.get
    } yield assertEquals(
      observed,
      List(
        LogEvent.CircuitBreakerStateChanged ->
          Map(LogField.Breaker -> "mongo-writes", LogField.BreakerTransition -> "closed_to_open")
      )
    )
  }

  test("CBR-07.3 each disabled breaker is announced once at startup with its name and no configuration") {
    for {
      events <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          events.update(_ :+ (event -> fields))
      }
      _ <- DependencyBreakers.create(
        ResilienceConfig(voyage = quick.copy(enabled = false), kafkaPublisher = quick.copy(enabled = false)),
        diagnostics
      )
      observed <- events.get
    } yield assertEquals(
      observed.sortBy(_._2.toString),
      List(
        LogEvent.CircuitBreakerDisabled -> Map(LogField.Breaker -> "kafka-publisher"),
        LogEvent.CircuitBreakerDisabled -> Map(LogField.Breaker -> "voyage")
      )
    )
  }

  test("CBR-02.4 a disabled breaker never reports unavailable") {
    for {
      breakers <- DependencyBreakers.create(
        ResilienceConfig(mongoWrites = quick.copy(enabled = false)),
        Diagnostics.noop
      )
      _ <- List
        .fill(5)(
          breakers.mongoWrites
            .protect(IO.raiseError[Unit](new RuntimeException("down")))(CircuitBreaker.classifier(_ => true))
            .attempt
        )
        .sequence
      ready <- MongoHiringRuntime.readiness(ProbeResult.Ready, breakers.mongoWrites)
    } yield assertEquals(ready, ProbeResult.Ready)
  }
}
