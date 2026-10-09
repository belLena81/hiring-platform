package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.util.UUID
import scala.concurrent.duration.*

/** `019_interview_request_receipt_index`: restart, concurrent start, upgrade and fail-closed behavior. */
final class InterviewRequestReceiptIndexMigrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val ledgerId = MigrationIds.InterviewRequestReceiptIndex.value
  private val indexName = MongoIndexNames.InterviewWorkflowRequestReceipt

  private def initialize(database: MongoDatabase[IO]): IO[Unit] =
    MongoHiringSetup.initialize(database, Diagnostics.noop)

  private def workflowIndexes(database: MongoDatabase[IO]): IO[List[Document]] =
    MongoRepositoryTestSupport
      .collection(database, MongoCollections.InterviewWorkflows)
      .flatMap(_.listIndexes[Document].map(_.toList))

  test("a fresh start installs the sparse request index and records the proof") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        indexes <- workflowIndexes(fixture.database)
        ledger <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq(MongoFields.Id, ledgerId)
        )
      } yield {
        val index = indexes.find(_.getString("name") == indexName)
        assert(index.nonEmpty, clue(indexes.map(_.getString("name"))))
        assertEquals(index.map(_.getBoolean("sparse", false)), Some(true))
        assertEquals(
          index.map(_.get("key", classOf[Document]).keySet().toArray.toList),
          Some(List("requestWorkflowId"))
        )
        assert(ledger.nonEmpty)
      }
    }
  }

  test("a restart is a no-op and a lost index is recreated before workers start") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        _ <- initialize(fixture.database)
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.InterviewWorkflows)
        _ <- collection.dropIndex(indexName)
        _ <- initialize(fixture.database)
        indexes <- workflowIndexes(fixture.database)
      } yield assert(indexes.exists(_.getString("name") == indexName), clue(indexes.map(_.getString("name"))))
    }
  }

  test("upgrading existing data keeps every document and recreates the sparse request index") {
    mongoResource.use { fixture =>
      val workflow = UUID.randomUUID().toString
      val receipt = new Document(MongoFields.Id, s"request:${UUID.randomUUID()}:${UUID.randomUUID()}")
        .append("documentType", "requestReceipt")
        .append("requestWorkflowId", workflow)
        .append("requestFingerprint", "f")
      for {
        _ <- initialize(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.InterviewWorkflows, receipt)
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.InterviewWorkflows)
        _ <- collection.dropIndex(indexName)
        _ <- MongoRepositoryTestSupport
          .collection(fixture.database, MongoCollections.HiringMigrationLedger)
          .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, ledgerId)))
        _ <- initialize(fixture.database)
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          Filters.eq(MongoFields.Id, receipt.getString(MongoFields.Id))
        )
        indexes <- workflowIndexes(fixture.database)
      } yield {
        assertEquals(stored, Some(receipt))
        assert(indexes.exists(_.getString("name") == indexName))
      }
    }
  }

  test("an index of that name with another definition fails startup closed") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.InterviewWorkflows)
        _ <- collection.dropIndex(indexName)
        _ <- collection.createIndex(Indexes.ascending("candidateId"), new IndexOptions().name(indexName))
        drifted <- initialize(fixture.database).attempt
      } yield assertEquals(drifted.left.toOption.map(_.getClass.getSimpleName), Some("IndexMismatch"))
    }
  }

  test("concurrent starts all complete") {
    mongoResource.use { fixture =>
      for {
        results <- List.fill(4)(initialize(fixture.database).attempt).parSequence
        indexes <- workflowIndexes(fixture.database)
      } yield {
        assert(results.forall(_.isRight), clue(results))
        assertEquals(indexes.count(_.getString("name") == indexName), 1)
      }
    }
  }

  test("retention finds a workflow's evidence through indexes in every collection, never by scanning") {
    mongoResource.use { fixture =>
      val workflow = InterviewWorkflowId(UUID.randomUUID())
      def explain(collection: String, filter: MongoFilter): IO[String] =
        MongoAccessEvaluationSupport
          .command(
            fixture.database,
            new Document(
              "explain",
              new Document("find", collection).append("filter", filter.bson.toBsonDocument())
            ).append("verbosity", "queryPlanner")
          )
          .map(_.get("queryPlanner", classOf[Document]).get("winningPlan", classOf[Document]).toJson)
      for {
        _ <- initialize(fixture.database)
        // The production filters themselves, so a changed retention query cannot drift from what is explained.
        plans <- MongoInterviewWorkflowRepository
          .retentionTargets(workflow)
          .traverse((collection, filter) => explain(collection, filter).map(collection -> _))
      } yield {
        assertEquals(plans.size, 5)
        plans.foreach((collection, plan) => assert(!plan.contains("COLLSCAN"), clue((collection, plan))))
      }
    }
  }
}
