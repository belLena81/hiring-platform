package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.SearchTerm
import com.example.graphQL.cats.service.search.{JobFacetQuery, JobSearchFilter, SearchEligibilityCriteria}
import java.time.Instant
import munit.FunSuite
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** MSC-08 / MSC-09: job filters compare canonical values; the definitions that carry them are pinned here. */
final class MongoJobCanonicalFiltersSpec extends FunSuite {
  private val filter = JobSearchFilter(Some(" Kyiv "), Set("Scala", " KAFKA", "scala"), Some(Instant.EPOCH))

  test("MSC-08.1 SearchTerm is the one trim and root-locale lower-case shared by candidates and jobs") {
    assertEquals(SearchTerm.canonical("  İstanbul Scala "), "i̇stanbul scala")
    assertEquals(SearchTerm.canonicalSorted(List("Scala", " scala", "KAFKA")), List("kafka", "scala"))
    // Candidate filters use the same function, so job and candidate matching cannot drift apart.
    assertEquals(
      com.example.graphQL.cats.service.search.ValidatedCandidateMatchFilters
        .from(com.example.graphQL.cats.service.search.CandidateMatchFilters(List(" Scala"), None, None, None))
        .toOption
        .map(_.requiredSkills),
      Some(List(SearchTerm.canonical(" Scala")))
    )
  }

  test("MSC-08.1 eligibility criteria carry canonical, distinct, ordered values") {
    val rendered = MongoSearchEligibilityRendering.indexed(SearchEligibilityCriteria.openJobs(filter)).toJson
    assert(rendered.contains(""""path": "location.cityCanonical", "value": "kyiv""""), rendered)
    assert(rendered.contains(""""path": "skillsCanonical", "value": "kafka""""), rendered)
    assert(rendered.contains(""""path": "skillsCanonical", "value": "scala""""), rendered)
    assertEquals(rendered.split("\"path\": \"skillsCanonical\"").length - 1, 2, rendered)
    assert(!rendered.contains("Kyiv") && !rendered.contains("KAFKA"), rendered)
    val operational = MongoSearchEligibilityRendering
      .operational(SearchEligibilityCriteria.jobs("voyage", filter))
      .toBsonDocument
      .toJson
    assert(operational.contains(""""location.cityCanonical": "kyiv""""), operational)
    assert(!operational.contains(""""location.city":"""), operational)
  }

  test("MSC-08.1 the discovery filter and the facet pipeline match on the canonical paths") {
    val discovery = MongoJobRepository.discoveryFilter(filter, None)
    val clauses = discovery.getList("$and", classOf[Document]).asScala.toList
    assert(clauses.exists(_.get("location.cityCanonical") == "kyiv"), clauses.toString)
    assertEquals(
      clauses
        .flatMap(c => Option(c.get("skillsCanonical", classOf[Document])))
        .map(_.getList("$all", classOf[String]).asScala.toList),
      List(List("kafka", "scala"))
    )
    assert(!clauses.exists(_.containsKey("location.city")) && !clauses.exists(_.containsKey("skills")))

    val facets = MongoJobRepository
      .facetPipeline(JobFacetQuery(filter, None), None)
      .last
      .get("$facet", classOf[Document])
    val skills = facets.getList("skills", classOf[Document]).asScala.toList
    assert(skills.head.get("$project", classOf[Document]).toJson.contains("$skillsCanonical"), skills.toString)
    val cities = facets.getList("cities", classOf[Document]).asScala.toList
    assertEquals(
      cities.find(_.containsKey("$group")).map(_.get("$group", classOf[Document]).getString("_id")),
      Some("$location.cityCanonical")
    )
    // Buckets keep one display spelling per canonical city (the smallest), so two spellings count as one city.
    assertEquals(cities.last.get("$project", classOf[Document]).getString("_id"), "$display")
  }

  test("MSC-08.1 the jobs validator history keeps V2, adds the canonical V3 and the binary-vector V4") {
    val history = MongoHiringValidators.jobValidatorHistory
    assertEquals(history.distinct.size, 3)
    assertEquals(history.last, MongoHiringValidators.jobValidator)
    val v2 = MongoHiringValidators.jobValidatorV2.toBsonDocument.toJson
    val v3 = MongoHiringValidators.jobValidatorV3.toBsonDocument.toJson
    val v4 = MongoHiringValidators.jobValidator.toBsonDocument.toJson
    assert(!v2.contains("skillsCanonical") && !v2.contains("cityCanonical"))
    assert(v3.contains("skillsCanonical") && v3.contains("cityCanonical") && !v3.contains("binData"))
    assert(v4.contains("skillsCanonical") && v4.contains("binData"))
  }

  test("MSC-09.1 the users validator history adds an embedding that must be a binary vector") {
    val history = MongoHiringValidators.userValidatorHistory
    assertEquals(history.distinct.size, 3)
    assertEquals(history.last, MongoHiringValidators.userValidator)
    assert(!MongoHiringValidators.userValidatorV2.toBsonDocument.toJson.contains("binData"))
    assert(MongoHiringValidators.userValidator.toBsonDocument.toJson.contains("binData"))
  }

  test("MSC-08.1 the replacement indexes key the canonical city and keep the page order") {
    val specs = MongoHiringIndexSetup.jobCanonicalFilterIndexes
    assertEquals(
      specs.map(_.options.getName),
      List("jobs_open_city_canonical_created_id", "jobs_embedding_meta_canonical_filters")
    )
    specs.foreach { spec =>
      val keys = spec.keys.toBsonDocument.keySet().asScala.toList
      assert(keys.contains("location.cityCanonical") && !keys.contains("location.city"), keys.toString)
    }
    assertEquals(
      specs.head.keys.toBsonDocument.keySet().asScala.toList,
      List("status", "location.cityCanonical", "createdAt", "_id")
    )
    assert(
      !MongoHiringIndexSetup.allSpecs.exists(spec =>
        MongoJobCanonicalFilterMigrations.LegacyIndexNames.contains(spec.options.getName)
      )
    )
  }

  test("MSC-08.1 the Atlas job definitions use the canonical paths under new default index names") {
    val vectorFilters = MongoAtlasSearchSetup.jobVectorFilterPaths
    assertEquals(
      vectorFilters.toSet,
      Set("status", "location.cityCanonical", "skillsCanonical", "createdAt", "recruiterId", "embeddingMeta.model")
    )
    val lexical = MongoAtlasSearchSetup.jobLexicalDefinition.toBsonDocument.toJson
    assert(lexical.contains("skillsCanonical") && lexical.contains("cityCanonical"), lexical)
  }

  test("MSC-08.1 a 256-character display value whose canonical form doubles in length fits the validator bound") {
    val worst = "İ" * 256
    assertEquals(SearchTerm.canonical(worst).length, 512)
    val v3 = MongoHiringValidators.jobValidatorV3.toBsonDocument.toJson
    assert(v3.contains("\"maxLength\": 512"), v3)
  }

  test("MSC-09.1 the migration fails closed on a double beyond float32 range, NaN and non-numbers") {
    import MongoBinaryEmbeddingVectorMigrations.floats
    assertEquals(floats(java.util.List.of(0.5d, 1.0d)), Some(List(0.5f, 1.0f)))
    assertEquals(floats(java.util.List.of(java.lang.Double.valueOf(1e300))), None)
    assertEquals(floats(java.util.List.of(java.lang.Double.valueOf(Double.NaN))), None)
    assertEquals(floats(java.util.List.of("x")), None)
  }
}
