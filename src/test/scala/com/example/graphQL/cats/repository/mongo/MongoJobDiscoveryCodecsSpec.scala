package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.search.{JobDiscoveryFacets, JobFacetBucket}
import munit.FunSuite
import org.bson.Document
import scala.jdk.CollectionConverters.*

final class MongoJobDiscoveryCodecsSpec extends FunSuite {
  private def empty: Document = new Document("skills", List.empty[Document].asJava)
    .append("countries", List.empty[Document].asJava)
    .append("cities", List.empty[Document].asJava)
    .append("remote", List.empty[Document].asJava)

  test("legitimate empty arrays return exact empty facets") {
    assertEquals(
      MongoJobDiscoveryCodecs.facets(Some(empty)).toOption,
      Some(JobDiscoveryFacets(Nil, Nil, Nil, Nil, truncated = false))
    )
  }

  test("absent result and missing or malformed dimension arrays fail closed") {
    assertEquals(
      MongoStoredDocumentDecoding.repository(MongoJobDiscoveryCodecs.facets(None)),
      Left(RepositoryError.InvalidStoredData)
    )
    val missing = empty
    val _ = missing.remove("skills")
    List(missing, empty.append("skills", "bad"), empty.append("skills", List("bad").asJava)).foreach { result =>
      assert(MongoJobDiscoveryCodecs.facets(Some(result)).isInvalid)
    }
  }

  test("buckets require the correct identity type and positive integral counts") {
    val invalid = List(
      new Document("count", 1),
      new Document("_id", "Scala"),
      new Document("_id", "").append("count", 1),
      new Document("_id", " ").append("count", 1),
      new Document("_id", true).append("count", 1),
      new Document("_id", "Scala").append("count", -1),
      new Document("_id", "Scala").append("count", 0),
      new Document("_id", "Scala").append("count", 1.5d),
      new Document("_id", "Scala").append("count", "1")
    )
    invalid.foreach { bucket =>
      assert(MongoJobDiscoveryCodecs.facets(Some(empty.append("skills", List(bucket).asJava))).isInvalid)
    }
    assert(
      MongoJobDiscoveryCodecs
        .facets(Some(empty.append("remote", List(new Document("_id", "false").append("count", 1)).asJava)))
        .isInvalid
    )
  }

  test("valid numeric and Boolean values preserve exact counts and order with sentinel truncation") {
    val buckets =
      (1 to 21).toList.map(index => new Document("_id", s"Skill$index").append("count", (22 - index).toLong))
    val remote = List(new Document("_id", false).append("count", 5))
    val result =
      MongoJobDiscoveryCodecs.facets(Some(empty.append("skills", buckets.asJava).append("remote", remote.asJava)))
    val expected = (1 to 20).toList.map(index => JobFacetBucket(s"Skill$index", (22 - index).toLong))
    assertEquals(result.toOption.map(_.skills), Some(expected))
    assertEquals(result.toOption.map(_.remote), Some(List(JobFacetBucket("false", 5L))))
    assertEquals(result.toOption.map(_.truncated), Some(true))
    assertEquals(
      MongoJobDiscoveryCodecs.facets(Some(empty.append("skills", buckets.take(20).asJava))).toOption.map(_.truncated),
      Some(false)
    )
  }
}
