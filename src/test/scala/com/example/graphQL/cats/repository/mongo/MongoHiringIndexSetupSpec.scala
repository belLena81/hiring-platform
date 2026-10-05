package com.example.graphQL.cats.repository.mongo

import com.mongodb.MongoClientSettings
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import munit.FunSuite
import org.bson.Document
import java.util.concurrent.TimeUnit

class MongoHiringIndexSetupSpec extends FunSuite {
  private val registry = MongoClientSettings.getDefaultCodecRegistry

  test("ordinary index definition verification accepts ordered keys and declared options") {
    val spec = IndexSpec(
      "applications",
      Indexes.compoundIndex(Indexes.ascending("candidateId"), Indexes.descending("createdAt", "_id")),
      new IndexOptions()
        .name("applications_candidate_created")
        .unique(true)
        .partialFilterExpression(Filters.eq("state", "Active"))
    )

    assertEquals(MongoHiringIndexSetup.definitionMismatch(spec, actual(spec)), None)
  }

  test("ordinary index definition verification rejects key order and direction changes") {
    val spec = IndexSpec(
      "applications",
      Indexes.compoundIndex(Indexes.ascending("candidateId"), Indexes.descending("createdAt", "_id")),
      new IndexOptions().name("applications_candidate_created")
    )

    val reordered = actual(spec).append("key", new Document("createdAt", -1).append("candidateId", 1).append("_id", -1))
    val directionChanged = actual(spec).append(
      "key",
      new Document("candidateId", -1).append("createdAt", -1).append("_id", -1)
    )

    assertEquals(MongoHiringIndexSetup.definitionMismatch(spec, reordered), Some("ordered keys"))
    assertEquals(MongoHiringIndexSetup.definitionMismatch(spec, directionChanged), Some("ordered keys"))
  }

  test("ordinary index definition verification checks unique and sparse flags") {
    val uniqueSpec = IndexSpec("users", Indexes.ascending("email"), new IndexOptions().name("users_email").unique(true))
    val sparseSpec = IndexSpec("users", Indexes.ascending("email"), new IndexOptions().name("users_email").sparse(true))

    assertEquals(MongoHiringIndexSetup.definitionMismatch(uniqueSpec, actual(uniqueSpec)), None)
    assertEquals(
      MongoHiringIndexSetup.definitionMismatch(uniqueSpec, actual(uniqueSpec).append("unique", false)),
      Some("unique")
    )
    assertEquals(MongoHiringIndexSetup.definitionMismatch(sparseSpec, actual(sparseSpec)), None)
    assertEquals(
      MongoHiringIndexSetup.definitionMismatch(sparseSpec, actual(sparseSpec).append("sparse", false)),
      Some("sparse")
    )
  }

  test("ordinary index definition verification checks partial predicates and TTL seconds") {
    val partialSpec = IndexSpec(
      "event_outbox",
      Indexes.ascending("state"),
      new IndexOptions().name("event_outbox_published").partialFilterExpression(Filters.eq("state", "Published"))
    )
    val ttlSpec = IndexSpec(
      "search_sessions",
      Indexes.ascending("expiresAt"),
      new IndexOptions().name("search_sessions_expiry").expireAfter(30L, TimeUnit.SECONDS)
    )

    assertEquals(MongoHiringIndexSetup.definitionMismatch(partialSpec, actual(partialSpec)), None)
    assertEquals(
      MongoHiringIndexSetup.definitionMismatch(
        partialSpec,
        actual(partialSpec).append("partialFilterExpression", new Document("state", "Failed"))
      ),
      Some("partial filter")
    )
    assertEquals(MongoHiringIndexSetup.definitionMismatch(ttlSpec, actual(ttlSpec)), None)
    assertEquals(
      MongoHiringIndexSetup.definitionMismatch(ttlSpec, actual(ttlSpec).append("expireAfterSeconds", 60L)),
      Some("TTL seconds")
    )
  }

  private def actual(spec: IndexSpec): Document = {
    val expected = new Document("name", spec.options.getName)
      .append("key", Document.parse(spec.keys.toBsonDocument(classOf[Document], registry).toJson))
    if (spec.options.isUnique) {
      expected.append("unique", true)
      ()
    }
    if (spec.options.isSparse) {
      expected.append("sparse", true)
      ()
    }
    Option(spec.options.getPartialFilterExpression)
      .foreach(filter =>
        expected.append(
          "partialFilterExpression",
          Document.parse(filter.toBsonDocument(classOf[Document], registry).toJson)
        )
      )
    Option(spec.options.getExpireAfter(TimeUnit.SECONDS)).foreach(seconds =>
      expected.append("expireAfterSeconds", seconds)
    )
    expected
  }
}
