package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.infrastructure.auth.HmacAuthenticationFingerprint
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.MutationReceiptFingerprint
import com.mongodb.client.model.{Filters, InsertOneOptions, Updates}
import org.bson.Document

class MongoAuthenticationReceiptMigrationIntegrationSpec extends MongoIntegrationSuite {
  private val fingerprints = new HmacAuthenticationFingerprint("synthetic-migration-key-material-unchanged")
  private val digest = MutationReceiptFingerprint.fromCanonicalInput("synthetic-password-bearing-input")
  private def receipt(id: String, operation: String = "login", value: String = digest.value): Document =
    new Document("_id", id)
      .append("operation", operation)
      .append("actorScope", "public:synthetic")
      .append(
        "idempotencyKey",
        java.util.UUID.nameUUIDFromBytes(id.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString
      )
      .append("fingerprint", value)
      .append("state", "Completed")
      .append("entity", new Document("type", "user").append("entityId", "synthetic-entity"))
      .append("createdAt", new java.util.Date(0L))
      .append("expiresAt", new java.util.Date(4102444800000L))

  test("concurrent migration preserves receipt metadata and permits key rotation only after completion") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        rows <- Mongo4catsCollections.documents(fixture.database, MongoCollections.MutationReceipts)
        originals = List(
          receipt("a", "login"),
          receipt("b", "signUp"),
          receipt("c", "bootstrapAdmin"),
          receipt("d", "updateJob")
        )
        _ <- originals.traverse_(row => rows.insertOne(row, new InsertOneOptions()).void)
        _ <- List.fill(2)(MongoAuthenticationReceiptMigration.initialize(fixture.database, fingerprints)).parSequence_
        updated <- rows.find(Filters.empty()).sort(com.mongodb.client.model.Sorts.ascending("_id")).all
        _ <- IO {
          assertEquals(updated.size, 4)
          originals.zip(updated.toList).foreach { case (original, actual) =>
            val expected = new Document(original)
            if (original.getString("operation") != "updateJob") {
              val _ = expected.put(
                "fingerprint",
                fingerprints.protect(original.getString("operation"), "public:synthetic", digest).value
              )
            }
            assertEquals(actual, expected)
          }
        }
        _ <- MongoAuthenticationReceiptMigration.initialize(
          fixture.database,
          new HmacAuthenticationFingerprint("synthetic-rotated-key-material-after-completion")
        )
        _ <- rows.insertOne(receipt("e"), new InsertOneOptions())
        drift <- MongoAuthenticationReceiptMigration.initialize(fixture.database, fingerprints).attempt
      } yield assert(drift.isLeft)
    }
  }

  test("bounded checkpoint resumes after malformed row repair and rejects a changed running key") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        rows <- Mongo4catsCollections.documents(fixture.database, MongoCollections.MutationReceipts)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- (0 until 501).toList.traverse_(index =>
          rows.insertOne(receipt(f"$index%04d"), new InsertOneOptions()).void
        )
        _ <- rows.insertOne(receipt("z", value = "malformed"), new InsertOneOptions())
        first <- MongoAuthenticationReceiptMigration.initialize(fixture.database, fingerprints).attempt
        checkpoint <- ledger.find(Filters.eq("_id", MongoAuthenticationReceiptMigration.MigrationId)).first
        changedKey <- MongoAuthenticationReceiptMigration
          .initialize(
            fixture.database,
            new HmacAuthenticationFingerprint("synthetic-different-key-while-migration-running")
          )
          .attempt
        _ <- rows.updateOne(Filters.eq("_id", "z"), Updates.set("fingerprint", digest.value))
        _ <- MongoAuthenticationReceiptMigration.initialize(fixture.database, fingerprints)
        complete <- ledger.find(Filters.eq("_id", MongoAuthenticationReceiptMigration.MigrationId)).first
        unprotected <- rows.find(Filters.not(Filters.regex("fingerprint", "^hmac-sha256:[0-9a-f]{64}$"))).first
      } yield {
        assert(first.isLeft)
        assert(changedKey.isLeft)
        assertEquals(checkpoint.map(_.getString("lastId")), Some("0499"))
        assertEquals(checkpoint.map(_.getString("state")), Some("Running"))
        assertEquals(complete.map(_.getString("state")), Some("Complete"))
        assert(complete.forall(!_.containsKey("lastId")))
        assertEquals(unprotected, None)
      }
    }
  }
}
