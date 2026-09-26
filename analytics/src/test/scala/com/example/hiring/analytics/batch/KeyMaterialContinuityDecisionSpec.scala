package com.example.hiring.analytics.batch

import munit.FunSuite

final class KeyMaterialContinuityDecisionSpec extends FunSuite {
  private val oldKey = "old-key" -> ("a" * 43)
  private val newKey = "new-key" -> ("b" * 43)

  test("an empty local lakehouse seeds the configured anchors") {
    assertEquals(
      KeyMaterialContinuityDecision.evaluate(false, false, Vector.empty, Vector(oldKey), Set.empty),
      Right(Vector(oldKey))
    )
  }

  test("existing data without a registry fails closed") {
    assert(KeyMaterialContinuityDecision.evaluate(false, true, Vector.empty, Vector(oldKey), Set.empty).isLeft)
  }

  test("valid anchors permit additions only when no stored row uses the unanchored key") {
    assertEquals(
      KeyMaterialContinuityDecision.evaluate(true, true, Vector(oldKey), Vector(oldKey, newKey), Set.empty),
      Right(Vector(newKey))
    )
    assert(
      KeyMaterialContinuityDecision
        .evaluate(true, true, Vector(oldKey), Vector(oldKey, newKey), Set("new-key"))
        .isLeft
    )
  }

  test("malformed, duplicate, removed, and changed anchors all fail closed") {
    assert(
      KeyMaterialContinuityDecision.evaluate(true, true, Vector("bad id" -> "x"), Vector(oldKey), Set.empty).isLeft
    )
    assert(KeyMaterialContinuityDecision.evaluate(true, true, Vector(oldKey, oldKey), Vector(oldKey), Set.empty).isLeft)
    assert(KeyMaterialContinuityDecision.evaluate(true, true, Vector(oldKey), Vector.empty, Set.empty).isLeft)
    assert(
      KeyMaterialContinuityDecision
        .evaluate(true, true, Vector("old-key" -> ("c" * 43)), Vector(oldKey), Set.empty)
        .isLeft
    )
  }
}
