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
  * Nothing here recomputes the report. Without the hand-off file the suite is BLOCKED, never a pass.
  *
  * Evidence is provisional until the analytics refactoring closes.
  */
abstract class StoryAdminReportSuite(reportFile: String) extends HiringStoryStackSuite {
  protected val query =
    """query Q($from: Instant!, $to: Instant!) { analyticsReport(from: $from, to: $to) {
      |  asOf
      |  funnel { day created accepted declined interview hired rejected }
      |  timeToHire { p50Hours eligibleCount excludedCount }
      |  stageDurations { metric eligibleCount }
      |} }""".stripMargin

  protected def documents: Vector[(String, Document)] = {
    val file: Path = Path
      .of(stack.running.namespace.manifest.workspace)
      .resolve(".local/data/hiring-story-analytics")
      .resolve(reportFile)
    assume(Files.isRegularFile(file), s"BLOCKED: run the analytics-build story suite first (it writes $file)")
    Files.readAllLines(file).asScala.toVector.filter(_.nonEmpty).map { line =>
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
