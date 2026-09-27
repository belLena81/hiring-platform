package com.example.hiring.analytics.batch

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

final class KeyMaterialContinuityDecisionSpec extends ScalaCheckSuite {
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

  property("valid new key anchors are accepted only when no stored row uses them") {
    val keyIds = Gen
      .nonEmptyListOf(Gen.oneOf(('a' to 'z') ++ ('0' to '9') ++ Seq('-')))
      .map(_.take(40).mkString)
      .suchThat(_.nonEmpty)
    val verifiers = Gen
      .listOfN(43, Gen.oneOf(('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9') ++ Seq('_', '-')))
      .map(_.mkString)

    forAll(keyIds, verifiers, verifiers) { (keyId: String, oldVerifier: String, newVerifier: String) =>
      val existing = keyId -> oldVerifier
      val addition = ("new-" + keyId.take(30)) -> newVerifier
      val configured = Vector(existing, addition)

      KeyMaterialContinuityDecision.evaluate(true, true, Vector(existing), configured, Set.empty) == Right(
        Vector(addition)
      ) &&
      KeyMaterialContinuityDecision.evaluate(true, true, Vector(existing), configured, Set(addition._1)).isLeft
    }
  }
}
