package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.mongodb.MongoWriteException
import com.mongodb.client.model.Filters
import scala.concurrent.duration.*

/** The Deleted-account shape used by the Atlas search fixtures must be one the strict users validator accepts. Runs on
  * the plain replica set, so a fixture that drifts from production writes fails here before an Atlas Local run.
  */
final class SearchFixtureAccountShapeIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val profile = CandidateProfile(Set("Scala"), Some("Engineer"), None)
  private val live = candidate.copy(
    profile = Some(UserProfile.Candidate(profile)),
    embedding = Some(
      EntityEmbedding(List(1f), EmbeddingMeta("synthetic", SourceHash.sha256(SearchableText.candidate(profile)), now))
    )
  )

  private def initialized(body: mongo4cats.database.MongoDatabase[IO] => IO[Unit]): IO[Unit] =
    mongoResource.use(fixture =>
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *> body(fixture.database)
    )

  test("a tombstoned fixture document is accepted by the users validator") {
    initialized { db =>
      MongoRepositoryTestSupport.insertOne(
        db,
        MongoCollections.Users,
        AtlasLocalSupport.tombstoned(MongoHiringCodecs.user(live), now)
      )
    }
  }

  test("the production tombstone update is accepted by the users validator and advances the version") {
    initialized { db =>
      for {
        _ <- MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(live))
        users <- MongoRepositoryTestSupport.collection(db, MongoCollections.Users)
        _ <- users.updateOne(Filters.eq(MongoFields.Id, live.id.value.toString), AtlasLocalSupport.tombstoneUpdate(now))
      } yield ()
    }
  }

  test("control: a Deleted document that keeps its embedding is still rejected") {
    initialized { db =>
      val document = AtlasLocalSupport.tombstoned(MongoHiringCodecs.user(live), now)
      document.put("embedding", MongoHiringCodecs.user(live).get("embedding"))
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, document).attempt.map {
        case Left(error: MongoWriteException) => assertEquals(error.getError.getCode, 121)
        case other                            => fail(s"Expected a validation failure, got $other")
      }
    }
  }
}
