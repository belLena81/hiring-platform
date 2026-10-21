package com.example.graphQL.cats.repository.mongo

import munit.FunSuite
import org.bson.Document

import MongoOutboxClaimBatch.*

final class MongoOutboxClaimBatchSpec extends FunSuite {
  private def event(id: String, subjects: String*): Candidate = Candidate(id, subjects.toList)

  test("AC-08.4 a batch takes events in scan order up to the requested count") {
    val rows = List(event("e1", "a"), event("e2", "b"), event("e3", "c"), event("e4", "d"))
    val selection = select(rows, need = 3)
    assertEquals(selection.claimable.map(_.id), List("e1", "e2", "e3"))
    assertEquals(selection.examined, 3)
  }

  test("AC-08.4 a later event of a subject already in the batch waits for the next claim") {
    val rows = List(event("e1", "a", "b"), event("e2", "b"), event("e3", "c"), event("e4", "a"), event("e5", "d"))
    val selection = select(rows, need = 3)
    assertEquals(selection.claimable.map(_.id), List("e1", "e3", "e5"))
    // The skipped rows lie before the last taken one, so the scan has passed them; they stay retryable.
    assertEquals(selection.examined, 5)
  }

  test("AC-08.4 skipped rows after the last taken event are not examined") {
    val rows = List(event("e1", "a"), event("e2", "b"), event("e3", "c"), event("e4", "a"))
    val selection = select(rows, need = 2)
    assertEquals(selection.claimable.map(_.id), List("e1", "e2"))
    assertEquals(selection.examined, 2)
  }

  test("AC-08.4 when every row shares one subject exactly one event is taken and all rows are examined") {
    val rows = List(event("e1", "s"), event("e2", "s"), event("e3", "s"))
    val selection = select(rows, need = 3)
    assertEquals(selection.claimable.map(_.id), List("e1"))
    assertEquals(selection.examined, 3)
  }

  test("AC-08.4 an event without readable subjects is still taken so the claim can fail it as invalid") {
    val selection = select(List(event("bad"), event("good", "a")), need = 2)
    assertEquals(selection.claimable.map(_.id), List("bad", "good"))
  }

  test("AC-08.4 subject leases are granted to the first event that names a subject, in ascending subject order") {
    val grants = subjectGrants(List("e1" -> List("c", "a"), "e2" -> List("b"), "e3" -> List("a")))
    assertEquals(grants, List("a" -> "e1", "b" -> "e2", "c" -> "e1"))
  }

  test("AC-08.4 leased documents come back in claim order and foreign documents are dropped") {
    val documents = List("e3", "e1", "other", "e2").map(new Document("_id", _))
    assertEquals(inOrder(List("e1", "e2", "e3"), documents).map(_.getString("_id")), List("e1", "e2", "e3"))
  }
}
