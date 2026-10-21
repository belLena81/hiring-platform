package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.util.UUID
import scala.concurrent.duration.*

/** `019_interview_request_receipt_index` and its successor `028_interview_request_receipt_removal`: 019 still records
  * its proof, the index it created is retired by 028, and retention stays index-served without it.
  */
final class InterviewRequestReceiptIndexMigrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val ledgerId = MigrationIds.InterviewRequestReceiptIndex.value
  private val indexName = MongoIndexNames.InterviewWorkflowRequestReceipt

  private def initialize(database: MongoDatabase[IO]): IO[Unit] =
    MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def workflowIndexes(database: MongoDatabase[IO]): IO[List[Document]] =
    MongoRepositoryTestSupport
      .collection(database, MongoCollections.InterviewWorkflows)
      .flatMap(_.listIndexes[Document].map(_.toList))

  test("a fresh start records the proof of 019 and ends without the request index that 028 retires") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        indexes <- workflowIndexes(fixture.database)
        ledger <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq(MongoFields.Id, ledgerId)
        )
        successor <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq(MongoFields.Id, MigrationIds.InterviewRequestReceiptRemoval.value)
        )
      } yield {
        assert(!indexes.exists(_.getString("name") == indexName), clue(indexes.map(_.getString("name"))))
        assertEquals(ledger.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(successor.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  test("a restart and concurrent starts all complete without the retired index coming back") {
    mongoResource.use { fixture =>
      for {
        results <- List.fill(4)(initialize(fixture.database).attempt).parSequence
        _ <- initialize(fixture.database)
        indexes <- workflowIndexes(fixture.database)
      } yield {
        assert(results.forall(_.isRight), clue(results))
        assertEquals(indexes.count(_.getString("name") == indexName), 0)
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
        plans <- MongoInterviewFields
          .retentionTargets(workflow)
          .traverse((collection, filter) => explain(collection, filter).map(collection -> _))
      } yield {
        assertEquals(plans.size, 5)
        plans.foreach((collection, plan) => assert(!plan.contains("COLLSCAN"), clue((collection, plan))))
      }
    }
  }
}
