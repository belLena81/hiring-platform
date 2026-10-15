package com.example.graphQL.cats.service

import cats.data.StateT
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.StateEffectFixtures.*

final class DiagnosticsGenericEffectSpec extends munit.FunSuite {
  test("EPS-04 a sink that throws synchronously never reaches the caller in a non-IO effect") {
    val throwing = new Diagnostics[Program] {
      def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): Program[Unit] =
        throw new IllegalStateException("sink broke")
    }
    val result = throwing.emit(LogEvent.EmbeddingWakeFailed).run(World())
    assert(result.isRight, clue(result))
  }

  test("EPS-04 a sink whose effect fails never reaches the caller in a non-IO effect") {
    val failing = new Diagnostics[Program] {
      def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): Program[Unit] =
        StateT.liftF[Outcome, World, Unit](Left(new IllegalStateException("sink failed")))
    }
    assert(failing.emit(LogEvent.EmbeddingWakeFailed).run(World()).isRight)
  }
}
