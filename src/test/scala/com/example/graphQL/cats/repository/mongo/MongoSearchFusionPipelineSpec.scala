package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.SearchMode
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import com.example.graphQL.cats.service.search.{CandidateMatchFilters, JobSearchFilter, VectorSearchQuery}
import munit.FunSuite
import org.bson.Document
import com.mongodb.MongoClientSettings

import java.util.UUID
import scala.jdk.CollectionConverters.*

class MongoSearchFusionPipelineSpec extends FunSuite {
  private val names = List("jobVector", "queryVector", "lexical")
  private val inputs = new Document()
    .append("jobVector", List(new Document("$match", new Document("role", "Candidate"))).asJava)
    .append("queryVector", List(new Document("$match", new Document("role", "Candidate"))).asJava)
    .append("lexical", List(new Document("$match", new Document("accountStatus", "Active"))).asJava)

  private val pageSize = PageSize.fromInt(10).toOption.getOrElse(fail("invalid page size"))
  private val searchQuery = VectorSearchQuery(
    List(0.1f, 0.2f),
    Some("Scala engineer"),
    JobSearchFilter(Some("Nicosia"), Set("Scala"), None),
    pageSize,
    SearchMode.HYBRID,
    "voyage-4-lite",
    UUID.fromString("00000000-0000-0000-0000-000000000901"),
    Some(List(0.3f, 0.4f)),
    CandidateMatchFilters(List(" Scala "), Some("cy"), Some("nicosia"), Some("AVAILABLE_NOW"))
  )

  private def repository(
      strategy: SearchFusionStrategy = SearchFusionStrategy.MongoRankFusion,
      rerank: Boolean = true,
      branchLimit: Int = 0
  ): MongoSemanticSearchRepository =
    new MongoSemanticSearchRepository(
      null,
      "jobs_vector",
      "candidates_vector",
      "jobs_lexical",
      "candidates_lexical",
      numCandidates = 60,
      branchResultLimit = branchLimit,
      fusionStrategy = strategy,
      rerankEnabled = rerank,
      diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
    )

  private def bsonJson(value: org.bson.conversions.Bson): String =
    value.toBsonDocument(classOf[org.bson.BsonDocument], MongoClientSettings.getDefaultCodecRegistry).toJson()

  test("production job fusion branches retain their vector and lexical constraints and validation candidate limit") {
    val repo = repository()
    val stages = repo.nativeJobFusionStages(
      searchQuery,
      searchQuery.lexicalQuery.getOrElse(""),
      repo.jobFilter(searchQuery, searchQuery.filter)
    )
    val fusion = stages.head.get("$rankFusion", classOf[Document])
    val branches = fusion.get("input", classOf[Document]).get("pipelines", classOf[Document])
    val vector = branches.getList("vector", classOf[Document]).get(0).get("$vectorSearch", classOf[Document])
    val lexical = branches.getList("lexical", classOf[Document]).asScala.toList

    assertEquals(vector.getString("index"), "jobs_vector")
    assertEquals(vector.getString("path"), "embedding")
    assertEquals(vector.getInteger("numCandidates").intValue(), 60)
    assertEquals(vector.getInteger("limit").intValue(), 60)
    assert(bsonJson(vector.get("filter", classOf[org.bson.conversions.Bson])).contains("Open"))
    assertEquals(lexical.head.get("$search", classOf[Document]).getString("index"), "jobs_lexical")
    assertEquals(
      lexical.head
        .get("$search", classOf[Document])
        .get("text", classOf[Document])
        .getList("path", classOf[String])
        .asScala
        .toList,
      List("title", "description", "requirements", "skills")
    )
    assertEquals(lexical(2).getInteger("$limit").intValue(), 60)
    assertEquals(stages(3).getInteger("$limit").intValue(), 60)
    assert(stages.exists(_.containsKey("$rerank")))
  }

  test("production fusion keeps branch result limits separate from ANN exploration") {
    val repo = repository(branchLimit = 30)
    val stages = repo.nativeJobFusionStages(
      searchQuery,
      searchQuery.lexicalQuery.getOrElse(""),
      repo.jobFilter(searchQuery, searchQuery.filter)
    )
    val pipelines = stages.head
      .get("$rankFusion", classOf[Document])
      .get("input", classOf[Document])
      .get("pipelines", classOf[Document])
    val vector = pipelines.getList("vector", classOf[Document]).get(0).get("$vectorSearch", classOf[Document])
    val lexical = pipelines.getList("lexical", classOf[Document]).get(2)

    assertEquals(vector.getInteger("numCandidates").intValue(), 60)
    assertEquals(vector.getInteger("limit").intValue(), 30)
    assertEquals(lexical.getInteger("$limit").intValue(), 30)
    assertEquals(stages(3).getInteger("$limit").intValue(), 30)
    assertEquals(stages(4).get("$rerank", classOf[Document]).getInteger("numDocsToRerank").intValue(), 30)
  }

  test("production candidate fusion branches apply account, skill and opt-in private filters before ranking") {
    val repo = repository(branchLimit = 30)
    val filter = repo.candidateFilter(searchQuery, includeEmbeddingModel = true)
    val stages = repo.nativeCandidateFusionStages(searchQuery, List(0.3f, 0.4f), filter)
    val fusion = stages.head.get("$rankFusion", classOf[Document])
    val branches = fusion.get("input", classOf[Document]).get("pipelines", classOf[Document])
    val jobVector = branches.getList("jobVector", classOf[Document]).get(0).get("$vectorSearch", classOf[Document])
    val queryVector =
      branches.getList("queryVector", classOf[Document]).get(0).get("$vectorSearch", classOf[Document])
    val lexical = branches.getList("lexical", classOf[Document]).asScala.toList
    val filterJson = bsonJson(filter)

    assertEquals(jobVector.getString("index"), "candidates_vector")
    assertEquals(jobVector.getInteger("numCandidates").intValue(), 60)
    assertEquals(jobVector.getInteger("limit").intValue(), 30)
    assertEquals(
      queryVector.getList("queryVector", classOf[java.lang.Double]).asScala.map(_.doubleValue()).toList,
      List(0.3f.toDouble, 0.4f.toDouble)
    )
    assertEquals(queryVector.getInteger("limit").intValue(), 30)
    assert(filterJson.contains("Candidate"))
    assert(filterJson.contains("Active"))
    assert(filterJson.contains("recruiterSearchOptIn"))
    assert(filterJson.contains("profile.currentResidence.countryCanonical"))
    assert(filterJson.contains("profile.availabilityStatus"))
    assert(filterJson.contains("skillsCanonical"))
    assertEquals(lexical.head.get("$search", classOf[Document]).getString("index"), "candidates_lexical")
    assertEquals(
      lexical.head
        .get("$search", classOf[Document])
        .get("text", classOf[Document])
        .getList("path", classOf[String])
        .asScala
        .toList,
      List("profile.skills", "profile.experienceSummary")
    )
    assertEquals(lexical(2).getInteger("$limit").intValue(), 30)
    assertEquals(stages(3).getInteger("$limit").intValue(), 30)
    assertEquals(stages(5).get("$rerank", classOf[Document]).getInteger("numDocsToRerank").intValue(), 30)
    assertEquals(stages.lastOption.map(_.keySet().iterator().next()), Some("$project"))
  }

  test("Mongo rank fusion uses equal branch weights and preserves candidate retrieval branches") {
    val stage = MongoSearchFusionPipeline.fusionStage(SearchFusionStrategy.MongoRankFusion, inputs, names)
    val config = stage.get("$rankFusion", classOf[Document])
    val pipelines = config.get("input", classOf[Document]).get("pipelines", classOf[Document])
    val weights = config.get("combination", classOf[Document]).get("weights", classOf[Document])

    assertEquals(pipelines.keySet().asScala.toSet, names.toSet)
    assertEquals(names.map(name => weights.getDouble(name).doubleValue()), List(1.0d, 1.0d, 1.0d))
  }

  test("Mongo score fusion uses sigmoid normalization and equal branch weights") {
    val stage = MongoSearchFusionPipeline.fusionStage(SearchFusionStrategy.MongoScoreFusion, inputs, names)
    val config = stage.get("$scoreFusion", classOf[Document])
    val weights = config.get("combination", classOf[Document]).get("weights", classOf[Document])

    assertEquals(config.get("input", classOf[Document]).getString("normalization"), "sigmoid")
    assertEquals(config.get("combination", classOf[Document]).getString("method"), "avg")
    assertEquals(names.map(name => weights.getDouble(name).doubleValue()), List(1.0d, 1.0d, 1.0d))
  }

  test("reranking is explicitly bounded to the requested result page") {
    val stage = MongoSearchFusionPipeline
      .rerankStage(
        "rerank-2.5-lite",
        "Scala backend",
        List("profile.skills", "profile.experienceSummary"),
        limit = 20
      )
      .get("$rerank", classOf[Document])

    assertEquals(stage.getInteger("numDocsToRerank").intValue(), 20)
    assertEquals(stage.getString("model"), "rerank-2.5-lite")
    assertEquals(
      stage.getList("path", classOf[String]).asScala.toList,
      List("profile.skills", "profile.experienceSummary")
    )
    assertEquals(stage.get("query", classOf[Document]).getString("text"), "Scala backend")
  }

  test("composed native fusion preserves retrieval score and reranks only after page truncation") {
    val rerank = MongoSearchFusionPipeline.rerankStage(
      "rerank-2.5-lite",
      "Scala backend",
      List("profile.skills"),
      limit = 20
    )
    val preparation = new Document("$set", new Document("profile.experienceSummary", ""))
    val scoreStage = new Document("$set", new Document("score", new Document("$meta", "score")))
    val stages = MongoSearchFusionPipeline.rankedStages(
      SearchFusionStrategy.MongoRankFusion,
      inputs,
      names,
      limit = 20,
      Some(rerank),
      List(preparation),
      scoreStage
    )

    assertEquals(
      stages.map(_.keySet().iterator().next()),
      List("$rankFusion", "$set", "$sort", "$limit", "$set", "$rerank", "$set")
    )
    assertEquals(
      stages(1).get("$set", classOf[Document]).get("retrievalScore", classOf[Document]).getString("$meta"),
      "score"
    )
    assertEquals(stages(3).getInteger("$limit").intValue(), 20)
    assertEquals(stages(5).get("$rerank", classOf[Document]).getInteger("numDocsToRerank").intValue(), 20)
  }

  test("fusion without reranking sorts and limits by its current score") {
    val scoreStage = new Document("$set", new Document("score", new Document("$meta", "score")))
    val stages = MongoSearchFusionPipeline.rankedStages(
      SearchFusionStrategy.MongoScoreFusion,
      inputs,
      names,
      limit = 10,
      None,
      Nil,
      scoreStage
    )

    assertEquals(stages.map(_.keySet().iterator().next()), List("$scoreFusion", "$set", "$sort", "$limit"))
    assertEquals(stages(2).get("$sort", classOf[Document]).getInteger("score").intValue(), -1)
    assertEquals(stages(3).getInteger("$limit").intValue(), 10)
  }
  test("lexical capture adapter fixes public job fields and applies eligibility before its bounded limit") {
    val repo = repository()
    val stages = repo.lexicalJobStages("Scala engineer", repo.jobFilter(searchQuery, searchQuery.filter)).asScala.toList
    assertEquals(
      stages.head
        .get("$search", classOf[Document])
        .get("text", classOf[Document])
        .getList("path", classOf[String])
        .asScala
        .toList,
      List("title", "description", "requirements", "skills")
    )
    assert(stages(1).containsKey("$match"))
    assert(stages(2).containsKey("$limit"))
    assert(!stages.head.toJson.contains("residence"))
    assert(!stages.head.toJson.contains("availability"))
  }

}
