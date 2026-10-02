package com.example.hiring.analytics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.adapter.mongo.MongoStreamingActivationGate
import com.example.hiring.analytics.domain.{StreamingActivationAuthorization, StreamingActivationIdentity}
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, Updates}
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Duration, Instant}
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Authenticated store proof: runtime grants are find-only and ownership records are append-only. */
final class StreamingActivationPermissionsIntegrationSpec extends munit.FunSuite {
  override val munitTimeout: FiniteDuration = 3.minutes
  private final class AuthenticatedMongo
      extends GenericContainer[AuthenticatedMongo](
        DockerImageName.parse(
          "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
        )
      )

  test("authenticated analytics runtime cannot grant activation, rewrite ownership or manage Mongo roles") {
    val adminPassword = UUID.randomUUID().toString
    val runtimePassword = UUID.randomUUID().toString
    val container = new AuthenticatedMongo()
      .withEnv("MONGO_INITDB_ROOT_USERNAME", "proof_operator")
      .withEnv("MONGO_INITDB_ROOT_PASSWORD", adminPassword)
      .withExposedPorts(27017)
      .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 2).withStartupTimeout(Duration.ofSeconds(90)))
    container.start()
    val host = container.getHost + ":" + container.getMappedPort(27017)
    val operator = MongoClients.create(s"mongodb://proof_operator:$adminPassword@$host/?authSource=admin")
    try {
      val database = "hiring_activation_proof_" + UUID.randomUUID().toString.replace("-", "")
      val db = operator.getDatabase(database)
      db.createCollection("analytics_streaming_activation")
      db.createCollection("analytics_streaming_lakehouses")
      val privileges = Vector(
        new Document("resource", new Document("db", database).append("collection", "analytics_streaming_activation"))
          .append("actions", List("find").asJava),
        new Document("resource", new Document("db", database).append("collection", "analytics_streaming_lakehouses"))
          .append("actions", List("find", "insert").asJava)
      )
      db.runCommand(
        new Document("createRole", "analytics_runtime_proof")
          .append("privileges", privileges.asJava)
          .append("roles", List.empty[Document].asJava)
      )
      db.runCommand(
        new Document("createUser", "analytics_runtime")
          .append("pwd", runtimePassword)
          .append("roles", List(new Document("role", "analytics_runtime_proof").append("db", database)).asJava)
      )
      val now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
      val identity = StreamingActivationIdentity("synthetic-stream", "source", "lakehouse", "contract", "settings")
      val grant = StreamingActivationAuthorization
        .fromEvidence(
          identity,
          "synthetic-grant",
          now.minusSeconds(1),
          now.plusSeconds(120),
          Vector("synthetic-fixture"),
          Vector("security-fixture", "qa-fixture")
        )
        .fold(problem => fail(problem), identity => identity)
      db.getCollection("analytics_streaming_activation")
        .insertOne(
          new Document("_id", grant.grantId)
            .append("grantId", grant.grantId)
            .append("streamId", identity.streamId)
            .append("sourceIdentity", identity.sourceIdentity)
            .append("lakehouseId", identity.lakehouseId)
            .append("contractFingerprint", identity.contractFingerprint)
            .append("settingsFingerprint", identity.settingsFingerprint)
            .append("validFrom", Date.from(grant.validFrom))
            .append("expiresAt", Date.from(grant.expiresAt))
            .append("evidenceReferences", grant.evidenceReferences.asJava)
            .append("independentReviewerReferences", grant.independentReviewerReferences.asJava)
            .append("evidenceDigest", grant.evidenceDigest)
        )
      val runtimeUri = s"mongodb://analytics_runtime:$runtimePassword@$host/?authSource=$database"
      val runtime = MongoClients.create(runtimeUri)
      try {
        val runtimeDb = runtime.getDatabase(database)
        val grants = runtimeDb.getCollection("analytics_streaming_activation")
        assertEquals(grants.countDocuments(), 1L)
        def denied(action: => Unit): Unit = {
          val error = intercept[com.mongodb.MongoException](action)
          assertEquals(error.getCode, 13)
        }
        denied { grants.insertOne(new Document("_id", "forged")); () }
        denied {
          grants.updateOne(
            Filters.eq("_id", grant.grantId),
            Updates.set("expiresAt", Date.from(now.plusSeconds(9999)))
          );
          ()
        }
        denied { grants.deleteOne(Filters.eq("_id", grant.grantId)); () }
        val registrations = runtimeDb.getCollection("analytics_streaming_lakehouses")
        registrations.insertOne(new Document("_id", "lakehouse").append("lakehouseId", "lakehouse"))
        denied { registrations.updateOne(Filters.eq("_id", "lakehouse"), Updates.set("lakehouseId", "other")); () }
        denied { registrations.deleteOne(Filters.eq("_id", "lakehouse")); () }
        denied {
          runtimeDb.runCommand(
            new Document("createRole", "forged")
              .append("privileges", List.empty[Document].asJava)
              .append("roles", List.empty[Document].asJava)
          );
          ()
        }
        denied {
          runtimeDb.runCommand(
            new Document("grantRolesToUser", "analytics_runtime")
              .append("roles", List(new Document("role", "root").append("db", "admin")).asJava)
          );
          ()
        }
      } finally runtime.close()
      mongo4cats.client.MongoClient
        .fromConnectionString[IO](runtimeUri)
        .use { client =>
          client.getDatabase(database).flatMap { reactiveDb =>
            val gate = new MongoStreamingActivationGate[IO](reactiveDb, AnalyticsTestOperationalConfig.streams)
            for {
              expiry <- gate.requireAuthorized(identity, grant.grantId)
              _ <- IO(assertEquals(expiry, grant.expiresAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)))
              mismatch <- gate.requireAuthorized(identity.copy(settingsFingerprint = "drifted"), grant.grantId).attempt
              _ <- IO(assert(mismatch.isLeft))
              _ <- IO.blocking(
                db.getCollection("analytics_streaming_activation")
                  .updateOne(Filters.eq("_id", grant.grantId), Updates.set("expiresAt", Date.from(now.minusSeconds(1))))
              )
              expired <- gate.requireAuthorized(identity, grant.grantId).attempt
              _ <- IO(assert(expired.isLeft))
            } yield ()
          }
        }
        .unsafeRunSync()
    } finally {
      operator.close()
      container.stop()
    }
  }
}
