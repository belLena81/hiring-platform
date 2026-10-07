package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.mongodb.client.model.{Filters, Updates}
import org.bson.Document

final class MongoAccessMeasurementIntegrationSpec extends MongoIntegrationSuite {
  test("response counters include every returned cursor batch and distinguish claim misses and duplicate errors") {
    mongoResource.use { fixture =>
      def drain(response: Document): IO[Unit] = {
        val cursor = response.get("cursor", classOf[Document])
        val next = cursor.get("id", classOf[Number]).longValue()
        if (next == 0L) IO.unit
        else
          MongoAccessEvaluationSupport
            .command(
              fixture.database,
              new Document("getMore", Long.box(next))
                .append("collection", MongoCollections.EventOutbox)
                .append("batchSize", 2)
            )
            .flatMap(drain)
      }
      for {
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.EventOutbox)
        _ <- collection.insertMany((0 until 7).toList.map(index => new Document("_id", Int.box(index))))
        _ <- fixture.commands.clear
        response <- MongoAccessEvaluationSupport.command(
          fixture.database,
          new Document("find", MongoCollections.EventOutbox).append("filter", new Document()).append("batchSize", 2)
        )
        _ <- drain(response)
        _ <- collection.findOneAndUpdate(Filters.eq("_id", 0), Updates.set("observed", true))
        _ <- collection.findOneAndUpdate(Filters.eq("_id", 99), Updates.set("observed", true))
        duplicate <- collection.insertOne(new Document("_id", Int.box(0))).attempt
        counts <- fixture.commands.responseCounts
        _ <- fixture.commands.clear
        cleared <- fixture.commands.responseCounts
      } yield {
        assert(duplicate.isLeft)
        val values = counts.hcursor
        assertEquals(values.get[Long]("returnedOutboxRows"), Right(7L))
        assertEquals(values.get[Long]("outboxFindResponses"), Right(1L))
        assert(values.get[Long]("outboxGetMoreResponses").exists(_ >= 3L))
        assertEquals(values.get[Long]("matchedOutboxClaimResponses"), Right(1L))
        assertEquals(values.get[Long]("emptyOutboxClaimResponses"), Right(1L))
        assertEquals(values.get[Long]("mongoDuplicateKeyErrors"), Right(1L))
        assert(cleared.asObject.exists(_.values.forall(_.asNumber.flatMap(_.toLong).contains(0L))))
      }
    }
  }
}
