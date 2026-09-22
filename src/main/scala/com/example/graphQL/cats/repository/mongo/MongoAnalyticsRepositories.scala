package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{AnalyticsErasureRequestRepository, AnalyticsFunnelDay, AnalyticsReportRepository, AnalyticsReportSnapshot, AnalyticsSkillPostingDay, AnalyticsTimeToHire, MutationWriteContext, RepositoryError}
import com.mongodb.client.model.{Filters, Sorts, UpdateOptions, Updates}
import com.mongodb.reactivestreams.client.{MongoClient, MongoDatabase}
import org.bson.Document

import java.util.Date
import java.time.Instant

/** Stores one idempotent erasure request per subject in the same Mongo transaction as account deletion. */
final class MongoAnalyticsErasureRequestRepository(
    database: MongoDatabase,
    transactionRunner: MongoTransactionRunner = MongoTransactionRunner.noTransaction
) extends AnalyticsErasureRequestRepository {
  private val collection = database.getCollection("analytics_erasure_requests")

  override def enqueue(userId: UserId, now: Instant, context: MutationWriteContext): IO[Either[RepositoryError, Unit]] =
    MongoMutationWriteContext.run(context, transactionRunner, transactionRequired = true) { session =>
      val filter = Filters.eq("_id", userId.value.toString)
      val update = Updates.combine(
        Updates.setOnInsert("_id", userId.value.toString),
        Updates.setOnInsert("requestedAt", Date.from(now)),
        Updates.setOnInsert("state", "Pending")
      )
      val operation = session.fold(
        PublisherBridge.first(collection.updateOne(filter, update, new UpdateOptions().upsert(true)))
      )(active => PublisherBridge.first(collection.updateOne(active, filter, update, new UpdateOptions().upsert(true))))
      operation.map {
        case Some(_) => Right(())
        case None => Left(RepositoryError.Unavailable)
      }.handleError(_ => Left(RepositoryError.Unavailable))
    }
}

object MongoAnalyticsErasureRequestRepository {
  def transactional(database: MongoDatabase, client: MongoClient): MongoAnalyticsErasureRequestRepository =
    new MongoAnalyticsErasureRequestRepository(database, MongoTransactionRunner.sessions(client, RepositoryError.Conflict))
}

/** Reads only the current aggregate snapshot written by the analytics batch. */
final class MongoAnalyticsReportRepository(database: MongoDatabase) extends AnalyticsReportRepository {
  private val collection = database.getCollection("analytics_report_snapshots")

  override def latest: IO[Either[RepositoryError, Option[AnalyticsReportSnapshot]]] =
    PublisherBridge.first(collection.find(Filters.eq("state", "Published")).sort(Sorts.descending("asOf")))
      .map(_.flatMap(read).asRight[RepositoryError])
      .handleError(_ => Left(RepositoryError.Unavailable))

  private def read(document: Document): Option[AnalyticsReportSnapshot] =
    for {
      asOf <- Option(document.getDate("asOf")).map(_.toInstant)
      funnel <- documents(document, "funnel").flatMap(_.traverse(readFunnel))
      skills <- documents(document, "skillPostingActivity").flatMap(_.traverse(readSkill))
    } yield AnalyticsReportSnapshot(asOf, funnel, Option(document.get("timeToHire", classOf[Document])).flatMap(readTimeToHire), skills)

  private def readFunnel(document: Document): Option[AnalyticsFunnelDay] =
    for {
      day <- Option(document.getDate("day")).map(_.toInstant)
      created <- count(document, "created")
      accepted <- count(document, "accepted")
      declined <- count(document, "declined")
      interview <- count(document, "interview")
      hired <- count(document, "hired")
      rejected <- count(document, "rejected")
    } yield AnalyticsFunnelDay(day, created, accepted, declined, interview, hired, rejected)

  private def readTimeToHire(document: Document): Option[AnalyticsTimeToHire] =
    for {
      p50 <- decimal(document, "p50Hours")
      p75 <- decimal(document, "p75Hours")
      p90 <- decimal(document, "p90Hours")
      p95 <- decimal(document, "p95Hours")
      eligible <- count(document, "eligibleCount")
      excluded <- count(document, "excludedCount")
    } yield AnalyticsTimeToHire(p50, p75, p90, p95, eligible, excluded)

  private def readSkill(document: Document): Option[AnalyticsSkillPostingDay] =
    for {
      day <- Option(document.getDate("day")).map(_.toInstant)
      skill <- Option(document.getString("skill"))
      postings <- count(document, "postings")
    } yield AnalyticsSkillPostingDay(day, skill, postings)

  private def documents(document: Document, field: String): Option[List[Document]] =
    Option(document.get(field, classOf[java.util.List[?]])).map(_.toArray.toList.collect { case value: Document => value })

  private def count(document: Document, field: String): Option[Long] =
    Option(document.get(field)).collect { case value: Number => value.longValue }

  private def decimal(document: Document, field: String): Option[Double] =
    Option(document.get(field)).collect { case value: Number => value.doubleValue }
}
