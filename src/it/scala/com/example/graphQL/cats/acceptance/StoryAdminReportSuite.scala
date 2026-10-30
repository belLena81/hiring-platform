package com.example.graphQL.cats.acceptance

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.repository.mongo.MongoRepositoryTestSupport
import com.mongodb.client.model.Filters
import io.circe.Json
import io.circe.syntax.*
import java.nio.file.{Files, Path}
import org.bson.Document
import scala.jdk.CollectionConverters.*

/** The report the real analytics batch published for the hiring story, read through the real Admin `analyticsReport`.
  *
  * Hand-off: `HiringStoryAcceptanceIntegrationSpec` (HEA-11.3) exports the story's events, the analytics-build suite
  * `HiringStoryAnalyticsReportIntegrationSpec` runs the production batch over them and exports the two report documents
  * it published. This suite stores those documents, unchanged, in its own owned database and asks the running API.
  * Nothing here recomputes the report. The tests run only in a started chain (marker file `chain-started`); otherwise
  * they are skipped (unverified, never a pass). In a started chain a missing events or report file, or a report whose
  * digest differs from the current events file, fails.
  *
  * Evidence is provisional until the analytics refactoring closes.
  */
abstract class StoryAdminReportSuite(eventsFile: String, reportFile: String) extends HiringStoryStackSuite {
  protected val query =
    """query Q($from: Instant!, $to: Instant!) { analyticsReport(from: $from, to: $to) {
      |  asOf
      |  funnel { day created accepted declined interview hired rejected }
      |  timeToHire { p50Hours p75Hours p90Hours p95Hours eligibleCount excludedCount }
      |  stageDurations { metric eligibleCount }
      |} }""".stripMargin

  private def sha256(file: Path): String =
    java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)).map("%02x".format(_)).mkString

  /** The report documents, only if the analytics step built them from exactly the events file that is on disk now. A
    * missing file or a different digest FAILS: a skipped or stale earlier step is never a pass.
    */
  protected def requireChain(): Unit = AnalyticsChain.require()

  protected def documents: Vector[(String, Document)] = {
    val directory = Path.of(stack.running.namespace.manifest.workspace).resolve(".local/data/hiring-story-analytics")
    val events = directory.resolve(eventsFile)
    val report = directory.resolve(reportFile)
    if (!Files.isRegularFile(events))
      fail(s"hand-off events file missing ($eventsFile): run the story export step first")
    if (!Files.isRegularFile(report)) fail(s"hand-off report file missing ($reportFile): run the analytics step first")
    val lines = Files.readAllLines(report).asScala.toVector.filter(_.nonEmpty)
    val expected = "events-sha256\t" + sha256(events)
    assertEquals(
      lines.headOption,
      Some(expected),
      "the report was not built from the current events file (stale hand-off)"
    )
    lines.tail.map { line =>
      val Array(collection, json) = line.split("\t", 2)
      collection -> Document.parse(json)
    }
  }

  protected def storePublishedReport: IO[Unit] =
    documents.traverse_ { case (collection, document) =>
      MongoRepositoryTestSupport.collection(stack.database, collection).flatMap { target =>
        target.deleteOne(Filters.eq("_id", document.get("_id"))) *> target.insertOne(document).void
      }
    }

  protected val period = Json.obj(
    "from" -> java.time.Instant.now().minus(java.time.Duration.ofDays(3)).toString.asJson,
    "to" -> java.time.Instant.now().plus(java.time.Duration.ofDays(1)).toString.asJson
  )
}
