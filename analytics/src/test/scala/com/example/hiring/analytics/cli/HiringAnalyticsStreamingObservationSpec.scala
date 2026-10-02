package com.example.hiring.analytics.cli

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import munit.CatsEffectSuite

final class HiringAnalyticsStreamingObservationSpec extends CatsEffectSuite {
  import HiringAnalyticsStreamingWorkloadMain.{Observations, Sample, observeBronze, observeReport}

  private def fixture(initial: Vector[Sample]) =
    (Ref.of[IO, Vector[Sample]](initial), Ref.of[IO, Observations](Observations())).tupled

  test("restart workloads preserve prior skill counts including their untimed controls") {
    val (firstWorkload, firstControl) = HiringAnalyticsStreamingWorkloadMain.syntheticSkills("a" * 32)
    val (secondWorkload, secondControl) = HiringAnalyticsStreamingWorkloadMain.syntheticSkills("b" * 32)
    assertEquals(firstWorkload, "workload" + "a" * 32)
    assertEquals(firstControl, "control" + "a" * 32)
    assertEquals(HiringAnalyticsStreamingWorkloadMain.syntheticSkills("a" * 32), firstWorkload -> firstControl)
    assertEquals(Set(firstWorkload, firstControl, secondWorkload, secondControl).size, 4)
    val baseline = Map(firstWorkload -> 7500L, firstControl -> 1L)
    val afterBurst = baseline ++ Map(secondWorkload -> 1000L, secondControl -> 1L)
    assert(baseline.forall { case (skill, count) => afterBurst.get(skill).contains(count) })
    val baselineBeforeControl = Map(firstWorkload -> 7500L)
    assert(baselineBeforeControl.forall { case (skill, count) => afterBurst.get(skill).contains(count) })
  }

  test("synthetic skills reject unbounded or malformed workload namespaces") {
    Vector("", "a" * 31, "a" * 33, "A" * 32, "../" + "a" * 29).foreach { nonce =>
      intercept[IllegalArgumentException](HiringAnalyticsStreamingWorkloadMain.syntheticSkills(nonce))
    }
  }

  test("delayed report verification cannot delay Bronze availability or overwrite producer appends") {
    fixture(Vector(Sample("first", 0, 1L, 10L))).flatMap { case (samples, observations) =>
      for {
        paused <- Deferred[IO, Unit]
        resume <- Deferred[IO, Unit]
        program = observeBronze(samples, observations, Set(0 -> 1L), 20L) *>
          paused.complete(()).void *> resume.get *> observeReport(samples, Set("first"), 1000L)
        fiber <- program.start
        _ <- paused.get
        bronze <- samples.get
        _ = assertEquals(bronze.head.bronzeAt, Some(20L))
        _ = assertEquals(bronze.head.reportAt, None)
        _ <- samples.update(_ :+ Sample("second", 1, 2L, 30L))
        _ <- resume.complete(())
        _ <- fiber.joinWithNever
        result <- samples.get
      } yield {
        assertEquals(result.map(_.eventId), Vector("first", "second"))
        assertEquals(result.head.bronzeAt, Some(20L))
        assertEquals(result.head.reportAt, Some(1000L))
        assertEquals(result(1), Sample("second", 1, 2L, 30L))
      }
    }
  }

  test("Bronze backlog uses the atomically merged producer state before report checks") {
    fixture(Vector(Sample("first", 0, 1L, 10L))).flatMap { case (samples, observations) =>
      for {
        _ <- samples.update(_ :+ Sample("second", 1, 2L, 15L))
        _ <- observeBronze(samples, observations, Set(0 -> 1L), 20L)
        beforeReport <- observations.get
        _ <- observeBronze(samples, observations, Set(0 -> 1L, 1 -> 2L), 30L)
        afterDrain <- observations.get
      } yield {
        assertEquals(beforeReport.maxBacklog, 1)
        assertEquals(afterDrain.maxBacklog, 1)
      }
    }
  }

  test("repeated observations preserve the earliest Bronze and fully verified report times") {
    fixture(Vector(Sample("first", 0, 1L, 10L))).flatMap { case (samples, observations) =>
      for {
        _ <- observeBronze(samples, observations, Set(0 -> 1L), 20L)
        _ <- observeReport(samples, Set("first"), 30L)
        _ <- observeBronze(samples, observations, Set(0 -> 1L), 40L)
        _ <- observeReport(samples, Set("first"), 50L)
        result <- samples.get
      } yield assertEquals(result.head, Sample("first", 0, 1L, 10L, Some(20L), Some(30L)))
    }
  }

  test("failed report verification remains an error and leaves report availability absent") {
    fixture(Vector(Sample("first", 0, 1L, 10L))).flatMap { case (samples, observations) =>
      val failure = new IllegalStateException("synthetic report verification failure")
      for {
        result <- (observeBronze(samples, observations, Set(0 -> 1L), 20L) *>
          IO.raiseError[Set[String]](failure).flatMap(ids => observeReport(samples, ids, 30L))).attempt
        recorded <- samples.get
      } yield {
        assertEquals(result, Left(failure))
        assertEquals(recorded.head.bronzeAt, Some(20L))
        assertEquals(recorded.head.reportAt, None)
      }
    }
  }

  test("an append acknowledged after a captured observation cannot acquire negative latency") {
    fixture(Vector(Sample("first", 0, 1L, 200L))).flatMap { case (samples, observations) =>
      for {
        _ <- observeBronze(samples, observations, Set(0 -> 1L), 100L)
        _ <- observeReport(samples, Set("first"), 100L)
        before <- samples.get
        backlog <- observations.get
        _ <- observeBronze(samples, observations, Set(0 -> 1L), 210L)
        _ <- observeReport(samples, Set("first"), 220L)
        after <- samples.get
      } yield {
        assertEquals(before.head.bronzeAt, None)
        assertEquals(before.head.reportAt, None)
        assertEquals(backlog.maxBacklog, 1)
        assertEquals(after.head, Sample("first", 0, 1L, 200L, Some(210L), Some(220L)))
      }
    }
  }
}
