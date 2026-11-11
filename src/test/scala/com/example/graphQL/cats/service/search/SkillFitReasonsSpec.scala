package com.example.graphQL.cats.service.search

import munit.FunSuite

final class SkillFitReasonsSpec extends FunSuite {
  test("GDF-02.1 missing skills are the target's skills the viewer's skills lack") {
    assertEquals(SkillMatching.missing(Set("Scala"), Set("Scala", "Kafka")), List("Kafka"))
    assertEquals(SkillMatching.matched(Set("Scala"), Set("Scala", "Kafka")), List("Scala"))
  }

  test("GDF-02.1 comparison shares the search normalization: case and surrounding spaces are ignored") {
    assertEquals(SkillMatching.missing(Set(" scala ", "KAFKA"), Set("Scala", "kafka", "Rust")), List("Rust"))
  }

  test("GDF-02.1 matched and missing together list every target skill exactly once, in target spelling") {
    val target = Set("Scala", "kafka", "Rust", " Go")
    val viewer = Set("scala", "GO")
    val matched = SkillMatching.matched(viewer, target)
    val missing = SkillMatching.missing(viewer, target)
    assertEquals((matched ++ missing).toSet, target)
    assertEquals(matched.intersect(missing), Nil)
    assertEquals(missing, List("kafka", "Rust"))
  }

  test("GDF-02.1 nothing is missing when the viewer covers the target, and everything when the viewer has no skills") {
    assertEquals(SkillMatching.missing(Set("Scala", "Kafka", "Go"), Set("scala", "kafka")), Nil)
    assertEquals(SkillMatching.missing(Set.empty, Set("b", "A")), List("A", "b"))
  }

  test("GDF-02.1 a target with no required skills has nothing missing") {
    assertEquals(SkillMatching.missing(Set("Scala"), Set.empty), Nil)
  }

  test("GDF-02.1 duplicate spellings in a target are each listed in the target's spelling when the viewer has none") {
    assertEquals(SkillMatching.missing(Set.empty, Set("Kafka", "kafka")), List("Kafka", "kafka"))
  }
}
