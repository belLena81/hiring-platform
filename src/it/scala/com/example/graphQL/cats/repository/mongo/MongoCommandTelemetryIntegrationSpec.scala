package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.infrastructure.telemetry.TelemetryCapture
import com.example.hiring.testing.LocalTestServices
import io.opentelemetry.api.trace.SpanKind
import org.bson.Document
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.oteljava.context.IOLocalContextStorage
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** HOR-02 and HOR-03 against the real driver: client spans follow the calling fiber and nothing sensitive is exported.
  */
class MongoCommandTelemetryIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val email = "jane.doe@example.com"
  private val resume = "PRIVATE-RESUME-BODY"

  test("HOR-03 Mongo commands issued inside a request span are its child client spans without filters or contents") {
    TelemetryCapture.resource.use { capture =>
      val telemetry = MongoCommandTelemetry.create(capture.java)
      MongoDatabaseProbe.clientResource(mongoEndpoint.uri, telemetry.instrument(MongoClientRole.Hiring)).use { client =>
        LocalTestServices.database(client).use { database =>
          for {
            provider <- IO.pure(IOLocalContextStorage.localProvider[IO])
            otel <- OtelJava.fromJOpenTelemetry[IO](capture.java)(using summon, provider)
            tracer <- otel.tracerProvider.get("it")
            documents <- Mongo4catsCollections.documents(database, MongoCollections.Applications)
            requestTraceId <- tracer.span("graphql.field.applications").use { request =>
              for {
                _ <- documents.insertOne(new Document("_id", "a-1").append("email", email).append("resume", resume))
                found <- documents.find(new Document("email", email).append("resume", resume)).first
                _ = assert(found.nonEmpty)
              } yield request.context.traceIdHex
            }
            metrics <- capture.collectMetrics.map(TelemetryCapture.points)
          } yield {
            val clientSpans = capture.finishedSpans.filter(_.getKind == SpanKind.CLIENT)
            assert(clientSpans.nonEmpty, clue(capture.finishedSpans.map(_.getName)))
            assert(clientSpans.forall(_.getTraceId == requestTraceId), clue(clientSpans))
            assert(clientSpans.map(_.getName).toSet.exists(_.startsWith("find ")), clue(clientSpans.map(_.getName)))
            assert(clientSpans.map(_.getName).toSet.exists(_.startsWith("insert ")), clue(clientSpans.map(_.getName)))
            val durations = metrics.filter(_.name == MongoCommandTelemetry.DurationInstrument)
            assert(durations.nonEmpty)
            val rendered = durations.toString + capture.finishedSpans
              .map(span => span.getName + span.getAttributes + span.getEvents + span.getStatus)
            List(email, resume, "a-1", "jane", database.name).foreach { text =>
              assert(!rendered.contains(text), clue(s"$text leaked in $rendered"))
            }
            assert(
              durations.forall(
                _.attributes.keySet.subsetOf(
                  Set("db.system.name", "db.operation.name", "db.collection.name", "hiring.mongo.client", "error.type")
                )
              )
            )
            assert(
              capture.finishedSpans
                .flatMap(_.getAttributes.asMap.asScala.keys.map(_.getKey))
                .toSet
                .subsetOf(
                  Set("db.system.name", "db.operation.name", "db.collection.name", "hiring.mongo.client", "error.type")
                )
            )
          }
        }
      }
    }
  }
}
