package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.AtlasLocalCriterionEvidence.*
import munit.FunSuite

/** Pure checks of the Atlas Local evidence helpers; they need no Atlas deployment. */
final class AtlasLocalCriterionEvidenceSpec extends FunSuite {
  private def bucket(name: String, recall: Option[Double], p95: Double, errors: Int = 0) =
    BucketFigures(name, 4, recall, Some(p95 / 2), Some(p95), Some(p95 * 1.1), errors)
  private val base = Settings(100, "none", 100, "float32-binary", false, 7, 500, 20, 32, 20261005L)
  private def settings(candidates: Int): Settings = base.copy(numCandidates = candidates)
  private def run(settings: Settings, p95: Double, bytes: Option[Long], recall: Double = 1.0, errors: Int = 0) =
    RunFigures(
      settings,
      8,
      "warm",
      bytes,
      List(
        bucket("Broad", Some(recall), p95, errors),
        bucket("Selective", Some(recall), p95),
        bucket("Empty", None, p95)
      )
    )

  test("MVR-08.L percentile uses nearest rank and is absent for an empty sample") {
    assertEquals(percentile(List(10.0, 20.0, 30.0, 40.0), 0.5), Some(20.0))
    assertEquals(percentile(List(10.0, 20.0, 30.0, 40.0), 0.95), Some(40.0))
    assertEquals(percentile(Nil, 0.5), None)
  }

  test("MVR-07.L recall is the share of exact ids that ANN also returned, absent when exact is empty") {
    assertEquals(recall(List("a", "b", "x"), List("a", "b", "c", "d")), Some(0.5))
    assertEquals(recall(List("a"), Nil), None)
  }

  test("MVR-07.L bucket figures exclude failed queries from recall and latency and count them as errors") {
    val figures = bucketFigures(
      "Broad",
      List(Sample(List("a"), List("a"), 10.0, failed = false), Sample(Nil, List("a"), 99.0, failed = true))
    )
    assertEquals(figures.recall, Some(1.0))
    assertEquals(figures.annP95, Some(10.0))
    assertEquals(figures.errors, 1)
    assertEquals(figures.queries, 2)
  }

  test("MVR-08.L without a baseline or with equal settings the outcome is retain current settings") {
    val current = run(settings(100), 10.0, Some(100L))
    assertEquals(decide(None, current)._1, SweepOutcome.RetainCurrentSettings)
    assertEquals(decide(Some(current), current)._1, SweepOutcome.RetainCurrentSettings)
  }

  test("MVR-08.L a faster setting meeting every decision 4 limit is a candidate for cloud confirmation") {
    val outcome = decide(Some(run(settings(100), 10.0, Some(100L))), run(settings(50), 8.0, Some(100L)))
    assertEquals(outcome._1, SweepOutcome.CandidateForCloudConfirmation)
  }

  test(
    "MVR-08.L low recall, latency above baseline plus 10 percent, errors or no improvement retain current settings"
  ) {
    val base = run(settings(100), 10.0, Some(100L))
    assertEquals(
      decide(Some(base), run(settings(50), 8.0, Some(100L), recall = 0.9))._1,
      SweepOutcome.RetainCurrentSettings
    )
    assertEquals(decide(Some(base), run(settings(50), 12.0, Some(50L)))._1, SweepOutcome.RetainCurrentSettings)
    assertEquals(
      decide(Some(base), run(settings(50), 8.0, Some(100L), errors = 1))._1,
      SweepOutcome.RetainCurrentSettings
    )
    assertEquals(decide(Some(base), run(settings(50), 10.5, Some(100L)))._1, SweepOutcome.RetainCurrentSettings)
  }

  test("MVR-08.L a different concurrency is not comparable and every run must agree to be a candidate") {
    val base = run(settings(100), 10.0, Some(100L))
    assertEquals(
      decide(Some(base), run(settings(50), 8.0, Some(100L)).copy(concurrency = 1))._1,
      SweepOutcome.RetainCurrentSettings
    )
    assertEquals(
      combine(List(SweepOutcome.CandidateForCloudConfirmation, SweepOutcome.RetainCurrentSettings)),
      SweepOutcome.RetainCurrentSettings
    )
    assertEquals(combine(Nil), SweepOutcome.RetainCurrentSettings)
  }

  test("MVR-08.L a written record parses back into the same figures") {
    val figures = run(settings(100), 10.0, Some(100L))
    val record = searchEvaluationRecord(
      "local-container",
      "8.3.11",
      "rev",
      "file.json",
      1000L,
      figures,
      None,
      decide(None, figures)
    )
    assertEquals(parseFigures(record.noSpaces), Right(figures))
    assertEquals(record.hcursor.downField("environment").as[String], Right("local-container"))
    assertEquals(
      record.hcursor.downField("criteria").as[List[String]],
      Right(List("MVR-07.L", "HSE-06.L", "MVR-08.L"))
    )
    assertEquals(record.hcursor.downField("mvr08").downField("outcome").as[String], Right("retain current settings"))
  }

  test("SEB-02.L SEB-04.L the curated record names its criteria, environment and fixture tags") {
    val record =
      curatedRecord("local-container", None, "rev", 8, List("curated-Lexical-8-1.json"), Map("closedJob" -> 1))
    assertEquals(record.hcursor.downField("criteria").as[List[String]], Right(List("SEB-02.L", "SEB-04.L")))
    assertEquals(record.hcursor.downField("seb04").downField("fixtureTags").downField("closedJob").as[Int], Right(1))
    assertEquals(record.hcursor.downField("environment").as[String], Right("local-container"))
  }

  test("MVR-08.L a baseline differing in any fixed input is not comparable; only one swept setting may differ") {
    val reference = run(settings(100), 10.0, Some(100L))
    val better = run(settings(50), 8.0, Some(100L))
    val changed = List(
      "branchLimit" -> base.copy(branchLimit = 50),
      "vectorStorage" -> base.copy(vectorStorage = "float64-array"),
      "storedSource" -> base.copy(storedSource = true),
      "pageSize" -> base.copy(pageSize = 5),
      "documents" -> base.copy(documents = 400),
      "queries" -> base.copy(queries = 10),
      "dimensions" -> base.copy(dimensions = 64),
      "seed" -> base.copy(seed = 1L)
    )
    changed.foreach { case (name, differing) =>
      val outcome = decide(Some(reference.copy(settings = differing.copy(numCandidates = 100))), better)
      assertEquals(outcome._1, SweepOutcome.RetainCurrentSettings, clue(name))
      assert(outcome._2.contains(name), clue(outcome))
    }
    val both = better.copy(settings = settings(50).copy(quantization = "scalar"))
    assertEquals(decide(Some(reference), both)._1, SweepOutcome.RetainCurrentSettings)
    assertEquals(
      decide(Some(reference), better.copy(settings = base.copy(quantization = "scalar")))._1,
      SweepOutcome.CandidateForCloudConfirmation
    )
  }

  test("MVR-08.L invalid baseline JSON is rejected") {
    assert(parseFigures("not json").isLeft)
    assert(parseFigures("{}").isLeft)
  }
}
