package com.example.graphQL.cats.domain.search

import com.example.graphQL.cats.domain.search.RankingInputError.*
import com.example.graphQL.cats.domain.model.SearchTerm
import com.example.graphQL.cats.service.search.SkillMatching
import munit.FunSuite

import scala.util.Random

class SkillEvidenceRankingSpec extends FunSuite {

  private def ev(candidate: Set[String], job: Set[String]) = SkillEvidence.of(candidate, job)
  private def hit(id: String, rank: Int, evidence: Option[SkillEvidence] = None) = RankingHit(id, rank, evidence)
  private def run(hits: List[RankingHit[String]], max: Int = 100) = SkillEvidenceRanking.rank(hits, max)(identity)
  private def ids(o: RankingOutcome[String]) = o.hits.map(_.id)
  private def asInput(o: RankingOutcome[String]) = o.hits.map(h => RankingHit(h.id, h.retrievalRank, h.evidence))
  private def ok(hits: List[RankingHit[String]], max: Int = 100) =
    run(hits, max).fold(e => fail(s"unexpected Left: $e"), identity)
  private def retrieval(n: Int) = (1 to n).toList.map(i => hit(s"r$i", i))
  private val oneOfTwo = ev(Set("a"), Set("a", "b"))

  test("HDQ-07.1 cold profile keeps the retrieval order with no evidence") {
    val job = Set("scala", "cats")
    val hits = (1 to 5).toList.map(i => hit(s"r$i", i, ev(Set.empty, job)))
    val o = ok(hits.reverse)
    assertEquals(o.basis, RankingBasis.RetrievalOrder)
    assertEquals(ids(o), List("r1", "r2", "r3", "r4", "r5"))
    assert(o.hits.forall(_.evidence.isEmpty))
  }

  test("HDQ-07.1 owned job with no skills gives the retrieval order (recruiter side)") {
    val hits = (1 to 5).toList.map(i => hit(s"c$i", i, ev(Set("scala"), Set.empty)))
    val o = ok(hits)
    assertEquals(o.basis, RankingBasis.RetrievalOrder)
    assertEquals(ids(o), List("c1", "c2", "c3", "c4", "c5"))
  }

  test("HDQ-07.2 missing evidence and zero overlap are treated alike") {
    val hits = List(hit("r1", 1, ev(Set("x"), Set.empty)), hit("r2", 2, ev(Set("x"), Set("y"))), hit("r3", 3, oneOfTwo))
    val o = ok(hits)
    assertEquals(o.basis, RankingBasis.SkillEvidence)
    assertEquals(ids(o), List("r3", "r1", "r2"))
    assertEqualsDouble(o.hits.head.fusedValue, 1.0 / 63 + 1.0 / 61, 1e-12)
    assertEqualsDouble(o.hits(1).fusedValue, 1.0 / 61, 1e-12)
  }

  test("HDQ-07.3 sparse match promotes only the hit with evidence") {
    val third = ev(Set("a"), Set("a", "b", "c"))
    val hits = retrieval(4) :+ hit("r5", 5, third)
    val o = ok(hits)
    assertEquals(ids(o), List("r5", "r1", "r2", "r3", "r4"))
    assertEqualsDouble(o.hits.head.fusedValue, 1.0 / 65 + 1.0 / 61, 1e-12)
  }

  test("HDQ-07.4 coverage decides, not candidate skill count; spelling and case are canonicalized") {
    val job = Set("Scala", "cats", "Kafka")
    val c1 = hit("c1", 1, ev(Set("java"), job))
    val c2 = hit("c2", 2, ev(Set("scala"), job))
    val c3 = hit("c3", 3, ev(Set(" SCALA ", "Cats", "kafka") ++ (1 to 47).map(i => s"other$i"), job))
    val o = ok(List(c1, c2, c3))
    assertEquals(ids(o), List("c3", "c2", "c1"))
    assertEquals(c3.evidence, ev(Set("scala", "cats", "kafka"), job))
    assertEquals(c3.evidence.map(e => (e.covered, e.required)), Some((3, 3)))
    assertEqualsDouble(o.hits(1).fusedValue, 1.0 / 62 + 1.0 / 62, 1e-12)
  }

  test("HDQ-07.4 duplicate and uncanonical job skill spellings count once") {
    val e = ev(Set("SCALA"), Set("scala", " Scala ", "SCALA", "cats", "  "))
    assertEquals(e.map(x => (x.covered, x.required)), Some((1, 2)))
    assertEquals(ev(Set("scala"), Set("  ", "")), None)
  }

  test("HDQ-07.5a equal coverage shares an evidence rank and retrieval rank breaks the tie") {
    val o = ok(List(hit("r2", 2, oneOfTwo), hit("r1", 1, oneOfTwo)))
    assertEquals(ids(o), List("r1", "r2"))
  }

  test("HDQ-07.5b equal fused value goes to the better retrieval rank") {
    val full = ev(Set("a", "b"), Set("a", "b"))
    val strongRanks = (2 to 61).toList :+ 63 // 61 hits with strictly greater coverage than the weak hit
    val strong = strongRanks.map(r => hit(s"s$r", r, full))
    val weak = hit("w62", 62, ev(Set("a"), Set("a", "b", "c")))
    val first = hit("p1", 1)
    val o = ok(first :: weak :: strong)
    val byId = o.hits.map(r => r.id -> r.fusedValue).toMap
    assertEquals(byId("w62"), 1.0 / 122 + 1.0 / 122) // evidence rank 62, retrieval rank 62
    assertEquals(byId("w62"), byId("p1")) // exactly equal in Double
    val order = ids(o)
    assert(order.indexOf("p1") < order.indexOf("w62"), order.toString)
  }

  test("HDQ-07.6 all 720 permutations of a mixed input give one outcome") {
    val base = List(
      hit("a", 1),
      hit("b", 2, oneOfTwo),
      hit("c", 3, ev(Set("a", "b"), Set("a", "b"))),
      hit("d", 4, oneOfTwo),
      hit("e", 5),
      hit("f", 6, ev(Set("a"), Set("a", "b", "c")))
    )
    val expected = ok(base)
    val results = base.permutations.map(p => ok(p)).toSet
    assertEquals(base.permutations.size, 720)
    assertEquals(results, Set(expected))
  }

  test("HDQ-07.6 fixed-seed shuffles of a 100-hit input give one outcome") {
    val jobSkills = (1 to 6).map(i => s"s$i").toSet
    val rnd = new Random(20261110L)
    val base = (1 to 100).toList.map { i =>
      hit(s"h$i", i, ev(jobSkills.take(i % 7), jobSkills))
    }
    val expected = ok(base)
    (1 to 50).foreach(_ => assertEquals(ok(rnd.shuffle(base)), expected))
  }

  test("HDQ-07.7 more hits than the bound fail with TooManyHits and no truncation") {
    val result = run(retrieval(101), 100)
    assertEquals(result.left.map(_.toChain.toList), Left(List(TooManyHits(100))))
    assert(run(retrieval(100), 100).isRight)
  }

  test("HDQ-07.7 limit below one fails with InvalidLimit") {
    assertEquals(run(retrieval(1), 0).left.map(_.toChain.toList), Left(List(InvalidLimit(0))))
  }

  test("HDQ-07.7 rank, key and duplicate problems accumulate together") {
    val hits = List(hit("a", 0), hit("a", 2), hit(" ", 2))
    val errs = run(hits).left.map(_.toChain.toList.toSet).swap.getOrElse(Set.empty)
    assertEquals(
      errs,
      Set[RankingInputError](NonPositiveRetrievalRank(0), DuplicateRetrievalRank(2), BlankKey, DuplicateHit("a"))
    )
  }

  test("HDQ-07.7 empty input gives an empty RetrievalOrder outcome; a single hit is unchanged") {
    assertEquals(ok(Nil), RankingOutcome[String](RankingBasis.RetrievalOrder, Nil))
    val single = ok(List(hit("only", 7, oneOfTwo)))
    assertEquals(ids(single), List("only"))
    assertEquals(single.basis, RankingBasis.SkillEvidence)
  }

  test("HDQ-07.7 output is always a permutation of the input") {
    val base = List(hit("a", 3, oneOfTwo), hit("b", 1), hit("c", 2, oneOfTwo))
    assertEquals(asInput(ok(base)).toSet, base.toSet)
    assertEquals(ok(base).hits.size, base.size)
  }

  test("HDQ-07.7 over-bound input fails with exactly TooManyHits and no ranked hits") {
    val result = run(retrieval(5), 3)
    assertEquals(result.left.map(_.toChain.toList), Left(List[RankingInputError](TooManyHits(3))))
  }

  test("HDQ-07.7 TooManyHits accumulates with rank and key errors") {
    val hits = List(hit("a", 1), hit("a", 1), hit(" ", 2))
    val errs = run(hits, 2).left.map(_.toChain.toList.toSet).swap.getOrElse(Set.empty)
    assertEquals(errs, Set[RankingInputError](TooManyHits(2), DuplicateRetrievalRank(1), BlankKey, DuplicateHit("a")))
  }

  test("HDQ-07.6 a retrieval rank of Int.MaxValue orders by the contract formula 1/(60+rank) without overflow") {
    val m = Int.MaxValue
    // Without evidence: fused values follow the formula and stay positive.
    val plain = ok(List(hit("far", m), hit("near", 5)))
    assertEquals(ids(plain), List("near", "far"))
    assertEquals(plain.hits.map(_.fusedValue), List(1.0 / 65.0, 1.0 / (60.0 + m.toDouble)))
    assert(plain.hits.forall(_.fusedValue > 0.0))
    // With evidence: evidence ranks are q=1, r=2; p has none.
    val full = ev(Set("a", "b"), Set("a", "b"))
    val o = ok(List(hit("p", 5), hit("q", m, full), hit("r", m - 1, oneOfTwo)))
    val expected = List(
      "q" -> (1.0 / (60.0 + m.toDouble) + 1.0 / 61.0),
      "r" -> (1.0 / (60.0 + (m - 1).toDouble) + 1.0 / 62.0),
      "p" -> (1.0 / 65.0)
    )
    assertEquals(o.hits.map(h => h.id -> h.fusedValue), expected)
    assertEquals(ids(o), expected.sortBy { case (_, v) => -v }.map(_._1))
  }

  test(
    "HDQ-07.8 the id payload (name, residence) never affects order or value: ranks and fused values match across swapped payloads"
  ) {
    final case class Person(name: String, residence: String)
    val alice = Person("Alice", "Kyiv")
    val bob = Person("Bob", "Lviv")
    val e = oneOfTwo
    def go(a: Person, b: Person) =
      SkillEvidenceRanking
        .rank(List(RankingHit(a, 1, e), RankingHit(b, 2, e)), 100)(_.name)
        .fold(err => fail(s"unexpected Left: $err"), identity)
    val x = go(alice, bob)
    val y = go(bob, alice)
    assertNotEquals(x.hits.map(_.id), y.hits.map(_.id)) // payloads and keys really differ
    assertEquals(x.hits.map(_.retrievalRank), y.hits.map(_.retrievalRank))
    assertEquals(x.hits.map(_.fusedValue), y.hits.map(_.fusedValue))
  }

  test("HDQ-07.8 structurally, RankingHit and SkillEvidence expose only rank and skill-coverage fields") {
    def fields(c: Class[?]) =
      c.getDeclaredFields
        .filter(f => !f.isSynthetic && !java.lang.reflect.Modifier.isStatic(f.getModifiers))
        .map(_.getName)
        .toSet
    assertEquals(fields(classOf[RankingHit[?]]), Set("id", "retrievalRank", "evidence"))
    assertEquals(fields(classOf[SkillEvidence]), Set("covered", "required"))
  }

  test("HDQ-07.4 covered equals the distinct canonical size of SkillMatching.matched, duplicate spellings included") {
    val cases = List(
      Set("scala") -> Set("scala", " Scala ", "cats"), // matched keeps both spellings of scala
      Set("SCALA", "cats") -> Set("Scala", "scala", "CATS", "kafka"),
      Set("java") -> Set("scala", "cats")
    )
    cases.foreach { case (candidate, job) =>
      val matched = SkillMatching.matched(candidate, job)
      val distinct = matched.map(SearchTerm.canonical).distinct.size
      assertEquals(ev(candidate, job).fold(0)(_.covered), distinct, s"candidate=$candidate job=$job")
    }
    assertEquals(SkillMatching.matched(Set("scala"), Set("scala", " Scala ", "cats")).size, 2) // raw count would be 2
    assertEquals(ev(Set("scala"), Set("scala", " Scala ", "cats")).map(_.covered), Some(1))
  }

  test("HDQ-07.4 job-side and candidate-side inputs rank by the same coverage rule") {
    val jobSide = ok(List(hit("j1", 1, ev(Set("x"), Set("a"))), hit("j2", 2, ev(Set("a"), Set("a")))))
    val candSide = ok(List(hit("c1", 1, ev(Set("x"), Set("a"))), hit("c2", 2, ev(Set("a", "q"), Set("a")))))
    assertEquals(ids(jobSide), List("j2", "j1"))
    assertEquals(ids(candSide), List("c2", "c1"))
  }

  test("HDQ-07.1 no evidence anywhere equals the baseline order for any input permutation") {
    val base = retrieval(8)
    val rnd = new Random(7L)
    (1 to 20).foreach { _ =>
      val o = ok(rnd.shuffle(base))
      assertEquals(o.basis, RankingBasis.RetrievalOrder)
      assertEquals(asInput(o), base)
    }
  }

  test("HDQ-07.6 fused value never increases along the returned order") {
    val jobSkills = (1 to 5).map(i => s"s$i").toSet
    val rnd = new Random(99L)
    val base = (1 to 60).toList.map(i => hit(s"h$i", i, ev(jobSkills.take(rnd.nextInt(6)), jobSkills)))
    val values = ok(base).hits.map(_.fusedValue)
    assert(values.zip(values.tail).forall { case (a, b) => a >= b }, values.toString)
  }
}
