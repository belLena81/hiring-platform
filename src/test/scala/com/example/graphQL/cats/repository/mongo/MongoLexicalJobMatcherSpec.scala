package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.search.{JobSearchFilter, ScoredJobId}
import java.time.Instant
import java.util.UUID
import munit.FunSuite
import org.bson.Document
import scala.jdk.CollectionConverters.*

final class MongoLexicalJobMatcherSpec extends FunSuite {
  private val filter = JobSearchFilter(Some("Nicosia"), Set("Scala"), Some(Instant.parse("2026-10-01T00:00:00Z")))
  private def id(n: Int): JobId = JobId(new UUID(0L, n.toLong))

  private def doc(value: Any*): Document = value.grouped(2).foldLeft(new Document()) { (document, pair) =>
    document.append(pair.head.toString, pair(1))
  }

  test("HDQ-12.3 the step 1 pipeline is $search, $limit N+1, then an ID and score projection") {
    val pipeline = MongoLexicalJobMatcher.pipeline("jobs_lexical", "scala", filter, 1001)
    assertEquals(pipeline.map(_.keySet.asScala.head), List("$search", "$limit", "$project"))
    assertEquals(pipeline(1).getInteger("$limit").intValue, 1001)
    assertEquals(
      pipeline(2).get("$project", classOf[Document]).toJson,
      """{"_id": 1, "score": {"$meta": "searchScore"}}"""
    )
  }

  test("HDQ-12.1 the search filter is the Open and structured criteria without the embedding-model predicate") {
    val search =
      MongoLexicalJobMatcher.pipeline("jobs_lexical", "scala", filter, 11).head.get("$search", classOf[Document])
    val rendered = search.toJson
    assert(!rendered.contains("embeddingMeta.model"), rendered)
    assert(rendered.contains(""""path": "status""""), rendered)
    assert(rendered.contains(""""path": "location.city""""), rendered)
    assert(rendered.contains(""""path": "skills""""), rendered)
    assert(rendered.contains(""""path": "createdAt""""), rendered)
  }

  test("HDQ-12.13 the open-job criteria render without the embedding predicate while hybrid criteria keep it") {
    import com.example.graphQL.cats.service.search.SearchEligibilityCriteria
    val withModel = MongoSearchEligibilityRendering.indexed(SearchEligibilityCriteria.jobs("voyage", filter)).toJson
    val withoutModel = MongoSearchEligibilityRendering.indexed(SearchEligibilityCriteria.openJobs(filter)).toJson
    assert(withModel.contains("embeddingMeta.model"))
    assert(!withoutModel.contains("embeddingMeta.model"))
  }

  test("HDQ-12.5 operator-like query text is only the value of the fixed-path text.query") {
    val hostile = """{"$ne":1}"""
    val search =
      MongoLexicalJobMatcher.pipeline("jobs_lexical", hostile, filter, 11).head.get("$search", classOf[Document])
    val must = search.get("compound", classOf[Document]).getList("must", classOf[Document]).asScala.toList
    assertEquals(must.size, 1)
    val text = must.head.get("text", classOf[Document])
    assertEquals(text.getString("query"), hostile)
    assertEquals(
      text.getList("path", classOf[String]).asScala.toList,
      List("title", "description", "requirements", "skills")
    )
    assert(!search.toJson.replace("\\\"", "").contains("\"$ne\""), "operator text must not become a key")
  }

  test("HDQ-12.7 step 2 adds the ID restriction to the same $and as the eligibility predicate") {
    val restricted = MongoJobRepository.discoveryFilter(filter, Some(Set(id(2), id(1))))
    val clauses = restricted.getList("$and", classOf[Document]).asScala.toList
    assertEquals(clauses.head.getString("status"), "Open")
    assert(clauses.exists(_.containsKey("location.city")))
    assert(clauses.exists(_.containsKey("skills")))
    assert(clauses.exists(_.containsKey("createdAt")))
    assertEquals(
      clauses
        .find(_.containsKey("_id"))
        .map(_.get("_id", classOf[Document]).getList("$in", classOf[String]).asScala.toList),
      Some(List(id(1).value.toString, id(2).value.toString))
    )
    val unrestricted = MongoJobRepository.discoveryFilter(filter, None).getList("$and", classOf[Document]).asScala
    assert(!unrestricted.exists(_.containsKey("_id")))
    assertEquals(clauses.size, unrestricted.size + 1)
  }

  test("HDQ-12.8 undecodable index hits are stored-data errors, never partial results") {
    assertEquals(
      MongoLexicalJobMatcher.decode(List(doc("_id", id(1).value.toString, "score", Double.box(1.5d)))),
      Right(List(ScoredJobId(id(1), 1.5d)))
    )
    assertEquals(
      MongoLexicalJobMatcher.decode(List(doc("_id", "not-a-uuid", "score", Double.box(1d)))),
      Left(RepositoryError.InvalidStoredData)
    )
    assertEquals(
      MongoLexicalJobMatcher.decode(List(doc("_id", id(1).value.toString))),
      Left(RepositoryError.InvalidStoredData)
    )
  }

  test("HDQ-12.2 with a radius the ID restriction sits inside $geoNear.query and $geoNear stays first") {
    import com.example.graphQL.cats.domain.model.GeoPoint
    import com.example.graphQL.cats.service.search.{JobFacetQuery, NearbyRadius}
    val pipeline = MongoJobRepository.facetPipeline(
      JobFacetQuery(filter, Some(NearbyRadius(GeoPoint(35d, 33d), 20d)), Some("scala")),
      Some(Set(id(1)))
    )
    assertEquals(pipeline.map(_.keySet.asScala.head), List("$geoNear", "$project", "$facet"))
    val query = pipeline.head.get("$geoNear", classOf[Document]).get("query", classOf[Document])
    assertEquals(query.get("location.remote"), java.lang.Boolean.FALSE)
    assert(query.getList("$and", classOf[Document]).asScala.exists(_.containsKey("_id")))
    assert(query.getList("$and", classOf[Document]).asScala.exists(_.getString("status") == "Open"))
  }

  test("HDQ-12 pipeline shape: without a radius the restricted eligibility match is the first stage") {
    import com.example.graphQL.cats.service.search.JobFacetQuery
    val pipeline = MongoJobRepository.facetPipeline(JobFacetQuery(filter, None), None)
    assertEquals(pipeline.map(_.keySet.asScala.head), List("$match", "$project", "$facet"))
    assert(!pipeline.head.toJson.contains("$in"))
  }
}
