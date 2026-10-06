package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import org.bson.Document
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class MongoHiringIndexRecoveryIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val support = MongoAccessEvaluationSupport

  private def indexes(database: MongoDatabase[IO]): IO[Map[String, List[String]]] =
    support.command(database, new Document("listCollections", 1).append("nameOnly", true)).flatMap { result =>
      val names = result
        .get("cursor", classOf[Document])
        .getList("firstBatch", classOf[Document])
        .asScala
        .toList
        .map(_.getString("name"))
      names
        .traverse { name =>
          MongoRepositoryTestSupport
            .collection(database, name)
            .flatMap(_.listIndexes[Document])
            .map(values => name -> values.toList.map(_.toJson).sorted)
        }
        .map(_.toMap)
    }

  private val mismatches = List(
    IndexSpec(
      MongoCollections.Jobs,
      Indexes.ascending(MongoFields.Id, MongoFields.CreatedAt),
      new IndexOptions().name(MongoHiringSetup.JobsCreatedIndex)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.NameCanonical),
      new IndexOptions().name(MongoHiringSetup.UsersNameIndex).unique(false)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.EmailCanonical),
      new IndexOptions().name(MongoHiringSetup.UsersEmailIndex).unique(true).sparse(false)
    ),
    IndexSpec(
      MongoCollections.Users,
      Indexes.ascending(MongoFields.AdminSingletonKey),
      new IndexOptions()
        .name(MongoHiringSetup.UsersAdminSingletonIndex)
        .unique(true)
        .partialFilterExpression(Filters.eq(MongoFields.Role, "Recruiter"))
    ),
    IndexSpec(
      MongoCollections.SearchSessions,
      Indexes.ascending(MongoFields.ExpiresAt),
      new IndexOptions().name(MongoHiringSetup.SearchSessionsExpiryIndex).expireAfter(60L, TimeUnit.SECONDS)
    )
  )

  test("repeated ordinary index setup preserves every definition and integrity constraint") {
    support.resource.use { fixture =>
      for {
        _ <- MongoHiringIndexSetup.create(fixture.database)
        before <- indexes(fixture.database)
        _ <- MongoHiringIndexSetup.create(fixture.database)
        after <- indexes(fixture.database)
        _ <- assertApplicationUniqueness(fixture.database)
      } yield assertEquals(after, before)
    }
  }

  mismatches.zipWithIndex.foreach { case (spec, position) =>
    test(s"ordinary index mismatch fails closed and preserves existing definitions: variant $position") {
      support.resource.use { fixture =>
        for {
          _ <- MongoHiringIndexSetup.create(fixture.database)
          coll <- MongoRepositoryTestSupport.collection(fixture.database, spec.collection)
          _ <- coll.dropIndex(spec.options.getName)
          _ <- coll.createIndex(spec.keys, spec.options)
          before <- indexes(fixture.database)
          result <- MongoHiringIndexSetup.create(fixture.database).attempt
          after <- indexes(fixture.database)
          _ <- assertApplicationUniqueness(fixture.database)
        } yield {
          assert(result.isLeft)
          assert(result.left.toOption.exists(_.getMessage.contains("index definition mismatch")))
          assertEquals(after, before)
        }
      }
    }
  }

  test("interrupted ordinary index creation resumes from preserved integrity indexes") {
    support.resource.use { fixture =>
      for {
        coll <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Applications)
        _ <- coll.createIndex(
          Indexes.ascending(MongoFields.CandidateId, MongoFields.JobId),
          new IndexOptions().name(MongoHiringSetup.ApplicationsCandidateJobIndex).unique(true)
        )
        admin <- fixture.client.getDatabase("admin")
        _ <- support.command(
          admin,
          new Document("configureFailPoint", "failCommand")
            .append("mode", new Document("times", 1))
            .append("data", new Document("failCommands", List("createIndexes").asJava).append("errorCode", 11601))
        )
        interrupted <- MongoHiringIndexSetup.create(fixture.database).attempt
        retained <- coll.listIndexes[Document]
        _ <- MongoHiringIndexSetup.create(fixture.database)
        complete <- indexes(fixture.database)
        _ <- MongoHiringIndexSetup.create(fixture.database)
        repeated <- indexes(fixture.database)
        _ <- assertApplicationUniqueness(fixture.database)
      } yield {
        assert(interrupted.isLeft)
        assert(retained.exists(_.getString("name") == MongoHiringSetup.ApplicationsCandidateJobIndex))
        assertEquals(repeated, complete)
      }
    }
  }

  private def assertApplicationUniqueness(database: MongoDatabase[IO]): IO[Unit] =
    for {
      coll <- MongoRepositoryTestSupport.collection(database, MongoCollections.Applications)
      _ <- coll.insertOne(
        new Document("_id", "integrity-first")
          .append("candidateId", "synthetic-candidate")
          .append("jobId", "synthetic-job")
      )
      duplicate <- coll
        .insertOne(
          new Document("_id", "integrity-second")
            .append("candidateId", "synthetic-candidate")
            .append("jobId", "synthetic-job")
        )
        .attempt
    } yield assert(duplicate.left.toOption.exists {
      case error: com.mongodb.MongoWriteException => error.getError.getCode == 11000
      case _                                      => false
    })
}
