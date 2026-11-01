package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import com.mongodb.client.model.SearchIndexModel
import com.mongodb.{MongoCommandException, ServerAddress}
import munit.CatsEffectSuite
import org.bson.{BsonDocument, BsonInt32, BsonString, Document}
import scala.jdk.CollectionConverters.*

/** Restartable search-index setup, driven through a fake admin (no Atlas). */
final class MongoAtlasSearchSetupSpec extends CatsEffectSuite {
  private val config = AtlasSearchIndexConfig("jv", "cv", "jl", "cl", 8, 1000, 1)
  private val owners = Map(
    "jv" -> MongoCollections.Jobs,
    "jl" -> MongoCollections.Jobs,
    "cv" -> MongoCollections.Users,
    "cl" -> MongoCollections.Users
  )

  private def allListed: Map[String, Document] = List("jv", "cv", "jl", "cl").map(name => name -> listed(name)).toMap

  private def vector(dimension: Int, filters: List[String]): Document = new Document(
    "fields",
    (new Document("type", "vector")
      .append("path", MongoFields.Embedding)
      .append("numDimensions", Int.box(dimension))
      .append("similarity", "cosine") :: filters.map(path =>
      new Document("type", MongoFields.Filter).append("path", path)
    )).asJava
  )
  private val candidateFilters = List(
    MongoFields.Role,
    MongoFields.AccountStatus,
    MongoFields.EmbeddingMetaModel,
    MongoFields.ProfileSkillsCanonical,
    MongoFields.ProfileRecruiterSearchOptIn,
    MongoFields.ProfileCurrentResidenceCountryCanonical,
    MongoFields.ProfileCurrentResidenceCityCanonical,
    MongoFields.ProfileAvailabilityStatus
  )

  private def definition(name: String, dimension: Int): Document = name match {
    case "jv" => vector(dimension, MongoAtlasSearchSetup.jobVectorFilterPaths)
    case "cv" => vector(dimension, candidateFilters)
    case "jl" => MongoAtlasSearchSetup.jobLexicalDefinition
    case _    => MongoAtlasSearchSetup.candidateLexicalDefinition
  }

  private def listed(name: String, dimension: Int = 8) =
    new Document("name", name)
      .append("queryable", java.lang.Boolean.TRUE)
      .append("latestDefinition", definition(name, dimension))

  /** Records creates; `existing` is what a list returns; `onCreate` lets a test fail the create. */
  private final class FakeAdmin(
      existing: Ref[IO, Map[String, Document]],
      created: Ref[IO, List[String]],
      failAfterCreate: Map[String, Throwable]
  ) extends MongoAtlasSearchSetup.SearchIndexAdmin {
    def list(collectionName: String): IO[List[Document]] =
      existing.get.map(_.collect { case (name, document) if owners(name) == collectionName => document }.toList)
    def create(collectionName: String, model: SearchIndexModel): IO[Unit] =
      created.update(model.getName :: _) *>
        existing.update(_ + (model.getName -> listed(model.getName))) *>
        failAfterCreate.get(model.getName).fold(IO.unit)(IO.raiseError)
  }

  private def run(
      present: Map[String, Document],
      failAfterCreate: Map[String, Throwable] = Map.empty
  ): IO[(Either[Throwable, Unit], List[String])] =
    for {
      existing <- Ref.of[IO, Map[String, Document]](present)
      created <- Ref.of[IO, List[String]](Nil)
      outcome <- MongoAtlasSearchSetup.provision(config, new FakeAdmin(existing, created, failAfterCreate)).attempt
      names <- created.get
    } yield (outcome, names.sorted)

  private def alreadyExists: Throwable = new MongoCommandException(
    new BsonDocument("ok", new BsonInt32(0))
      .append("code", new BsonInt32(68))
      .append("errmsg", new BsonString("Duplicate Index")),
    new ServerAddress()
  )

  test("MVR-10 all four indexes present: nothing is created and every definition is validated") {
    run(allListed).map { case (outcome, created) =>
      assertEquals(outcome, Right(()))
      assertEquals(created, Nil)
    }
  }

  test("MVR-10 some indexes present: only the missing names are created") {
    run(Map("jv" -> listed("jv"), "cl" -> listed("cl"))).map { case (outcome, created) =>
      assertEquals(outcome, Right(()))
      assertEquals(created, List("cv", "jl"))
    }
  }

  test("MVR-09 a definition mismatch is a sanitized MongoSetupError and nothing is created or replaced") {
    val present = List("jv", "cv", "jl", "cl").map(name => name -> listed(name, if (name == "jv") 4 else 8)).toMap
    run(present).map { case (outcome, created) =>
      assertEquals(created, Nil)
      outcome match {
        case Left(error: MongoSetupError) =>
          assertEquals(error.getMessage, "Atlas Search index 'jv' has an incompatible definition")
        case other => fail(s"Expected MongoSetupError, got $other")
      }
    }
  }

  test("MVR-10 an already-exists error from a concurrent creator proceeds to validation") {
    // The other creator's index appears (valid) and our create reports it exists.
    run(Map.empty, Map("jl" -> alreadyExists)).map { case (outcome, created) =>
      assertEquals(outcome, Right(()))
      assertEquals(created, List("cl", "cv", "jl", "jv"))
    }
  }

  test("MVR-10 already-exists is recognised by code 68 or message, and other errors still fail") {
    assert(MongoAtlasSearchSetup.isAlreadyExists(alreadyExists))
    assert(!MongoAtlasSearchSetup.isAlreadyExists(new RuntimeException("boom")))
    run(Map.empty, Map("jl" -> new RuntimeException("boom")))
      .map(result => assert(result._1.isLeft))
  }
}
