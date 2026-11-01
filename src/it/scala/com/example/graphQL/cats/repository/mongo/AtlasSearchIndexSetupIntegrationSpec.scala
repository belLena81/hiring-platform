package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.Diagnostics
import com.example.hiring.testing.DisposableAtlas
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Atlas Local gates for search-index setup (MVR-09.L and MVR-10.L). An absent URI is a skipped external gate, never
  * acceptance. Every test owns a nonce database and drops it afterwards.
  */
final class AtlasSearchIndexSetupIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val indexes =
    AtlasSearchIndexConfig("jobs_vector", "candidates_vector", "jobs_lexical", "candidates_lexical", 32, 120000, 500)
  private val indexNames =
    Set(indexes.jobVectorIndex, indexes.candidateVectorIndex, indexes.jobLexicalIndex, indexes.candidateLexicalIndex)

  private def authorized: IO[String] = IO.defer {
    assume(sys.env.contains("ATLAS_TEST_URI"), "BLOCKED: ATLAS_TEST_URI absent; live Atlas gate not executed")
    IO.fromEither(DisposableAtlas.authorizedUri(sys.env).left.map(new IllegalArgumentException(_)))
  }

  private def withDatabase(body: MongoDatabase[IO] => IO[Unit]): IO[Unit] =
    authorized.flatMap(uri =>
      AtlasLocalSupport
        .client(uri, None)
        .flatMap(client =>
          AtlasLocalSupport.nonceDatabase(client, AtlasLocalSupport.nonceName("search_evaluation_setup_"))
        )
        .use(body)
    )

  test("MVR-09.L an index definition mismatch fails closed with a sanitized error and preserves the existing indexes") {
    withDatabase { db =>
      for {
        // First provision under a different vector dimension, then run production setup with the configured one.
        _ <- MongoHiringSetup.initialize(db, Some(indexes.copy(dimension = 16)), Diagnostics.noop[IO])
        before <- AtlasLocalSupport.searchIndexDefinitions(db)
        outcome <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO]).attempt
        after <- AtlasLocalSupport.searchIndexDefinitions(db)
      } yield {
        outcome match {
          case Left(error: MongoSetupError) =>
            assert(error.getMessage.contains("incompatible definition"), clue(error.getMessage))
            assert(!error.getMessage.contains("mongodb://"), clue(error.getMessage))
            assert(!error.getMessage.contains("numDimensions"), clue(error.getMessage))
          case other => fail(s"Expected a sanitized MongoSetupError, got: ${other.left.map(_.getClass.getName)}")
        }
        assertEquals(before.map(_._1).toSet, indexNames)
        assertEquals(after, before)
      }
    }
  }

  test("MVR-09.L a readiness timeout fails closed with a sanitized error and leaves the created indexes in place") {
    withDatabase { db =>
      for {
        outcome <- MongoHiringSetup
          .initialize(db, Some(indexes.copy(readyTimeoutMillis = 1)), Diagnostics.noop[IO])
          .attempt
        after <- AtlasLocalSupport.searchIndexDefinitions(db)
      } yield {
        outcome match {
          case Left(error: MongoSetupError) =>
            assert(error.getMessage.contains("not queryable"), clue(error.getMessage))
            assert(!error.getMessage.contains("mongodb://"), clue(error.getMessage))
          case Right(_)    => assume(false, "Inconclusive: indexes were queryable within 1 ms, timeout not observed")
          case Left(other) => fail(s"Expected a sanitized MongoSetupError, got ${other.getClass.getName}")
        }
        assertEquals(after.map(_._1).toSet, indexNames)
      }
    }
  }

  /** Search indexes of a dropped database: a missing namespace has none; any other failure is raised. */
  private def definitionsOrEmpty(db: MongoDatabase[IO]) =
    AtlasLocalSupport.searchIndexDefinitions(db).recover {
      case error: com.mongodb.MongoCommandException if error.getErrorCode == NamespaceNotFound => Nil
    }
  private val NamespaceNotFound = 26

  private def untilEmpty(db: MongoDatabase[IO], attempts: Int): IO[List[(String, String)]] =
    definitionsOrEmpty(db).flatMap {
      case Nil               => IO.pure(Nil)
      case _ if attempts > 0 => IO.sleep(500.millis) *> untilEmpty(db, attempts - 1)
      case remaining         => IO.pure(remaining)
    }

  test(
    "MVR-10.L cancelled setup resumes on retry through the restartable provision path, and the dropped nonce database holds no search index"
  ) {
    authorized.flatMap { uri =>
      val name = AtlasLocalSupport.nonceName("search_evaluation_setup_")
      AtlasLocalSupport.client(uri, None).use { client =>
        for {
          // The database handle and how far setup got when it was cancelled.
          run <- AtlasLocalSupport.nonceDatabase(client, name).use { db =>
            for {
              fiber <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO]).start
              _ <- IO.sleep(3.seconds)
              _ <- fiber.cancel
              outcome <- fiber.join
              _ = assert(!outcome.isError, clue(outcome))
              atCancel <- definitionsOrEmpty(db)
              // Retry the same setup on the same database: it lists, creates only missing indexes, and validates all.
              _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
              definitions <- AtlasLocalSupport.searchIndexDefinitions(db)
            } yield {
              assertEquals(definitions.map(_._1).toSet, indexNames)
              (db, atCancel.map(_._1).toSet.size)
            }
          }
          (db, createdAtCancel) = run
          remaining <- client.listDatabaseNames
          _ = assert(!remaining.exists(_ == name), clue(remaining))
          leftover <- untilEmpty(db, 60)
          _ = assertEquals(leftover, Nil)
          // Only a cancel that landed between creating none and all four indexes exercises the resume path.
          _ = assume(
            createdAtCancel > 0 && createdAtCancel < indexNames.size,
            s"Inconclusive: setup had $createdAtCancel of ${indexNames.size} indexes at cancel time; resume not exercised"
          )
        } yield ()
      }
    }
  }
}
