package com.example.graphQL.cats.repository.mongo

import cats.data.OptionT
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.AccountDeletionStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{
  AnalyticsFunnelDay,
  AnalyticsReportSnapshot,
  AnalyticsSkillPostingDay,
  AnalyticsTimeToHire
}
import com.example.graphQL.cats.service.port.{
  AnalyticsErasureRequestRepository,
  AnalyticsReportRepository,
  MutationWriteContext,
  RepositoryIO
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.shared.Parsing
import com.mongodb.client.model.UpdateOptions
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.util.Date
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Stores one idempotent erasure request per subject in the same Mongo transaction as account deletion. */
final class MongoAnalyticsErasureRequestRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics,
    topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair =
      com.example.graphQL.cats.domain.workflow.InterviewTopicPair.Default
) extends AnalyticsErasureRequestRepository {
  private val collection = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsErasureRequests)
  private val outbox = Mongo4catsCollections.documents(database, MongoCollections.EventOutbox)
  private val users = Mongo4catsCollections.documents(database, MongoCollections.Users)
  private val completions = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsErasureCompletions)
  private val workerHeartbeats = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsWorkerHeartbeats)

  private def storedString(document: Document, field: String): Either[RepositoryError, Option[String]] =
    MongoDocumentFields.toRepository(MongoDocumentFields.optionalString(document, field))

  override def workerReady(now: Instant): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.workerReady")(
        RepositoryIO
          .lift(
            MongoSessionOperations.findOne(
              workerHeartbeats,
              None,
              MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, "analytics-erasure"),
                MongoFilter.eq(MongoFields.State, "Ready"),
                MongoFilter.gt(MongoFields.LeaseUntil, now.toDate)
              )
            )
          )
          .subflatMap(_.fold[Either[RepositoryError, Unit]](Left(RepositoryError.Unavailable))(_ => Right(())))
      )

  /** The receipt already stored for `requestId` in `documents`, assigning `freshReceiptId` when none was recorded. */
  private def receiptOrAssign(
      documents: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      requestId: String,
      freshReceiptId: String
  ): OptionT[RepositoryIO, String] = {
    val byId = MongoFilter.eq(MongoFields.Id, requestId)
    OptionT(RepositoryIO.lift(MongoSessionOperations.findOne(documents, session, byId))).semiflatMap { document =>
      RepositoryIO.fromEither(storedString(document, MongoFields.ReceiptId)).flatMap {
        case Some(receipt) => receipt.pure[RepositoryIO]
        case None          =>
          RepositoryIO
            .lift(
              MongoSessionOperations
                .updateOne(documents, session, byId, MongoUpdate.set(MongoFields.ReceiptId, freshReceiptId))
            )
            .as(freshReceiptId)
      }
    }
  }

  override def enqueue(
      userId: UserId,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[String] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.enqueue")(
        RepositoryIO.lift(UUIDGen[IO].randomUUID.map(_.toString)).flatMap { freshReceiptId =>
          MongoMutationWriteContext.run(context, transactionRunner, transactionRequired = true) { session =>
            val requestId = userId.value.toString
            receiptOrAssign(completions, session, requestId, freshReceiptId)
              .orElse(receiptOrAssign(collection, session, requestId, freshReceiptId))
              .getOrElseF(createRequest(requestId, freshReceiptId, now, session))
          }
        }
      )

  private def createRequest(
      requestId: String,
      freshReceiptId: String,
      now: Instant,
      session: Option[ClientSession[IO]]
  ): RepositoryIO[String] = {
    val request = new Document(MongoFields.Id, requestId)
      .append(MongoFields.RequestedAt, now.toDate)
      .append(MongoFields.ReceiptId, freshReceiptId)
      .append(MongoFields.State, "Pending")
      .append(MongoFields.FencingVersion, 1)
      .append("producerRegistry", true)
    val reportControl = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsReportControl)
    val hide = MongoUpdate.combine(
      MongoUpdate.inc(MongoFields.Generation, 1L),
      MongoUpdate.set(MongoFields.State, AnalyticsReportSnapshotDocument.Hidden),
      MongoUpdate.set(MongoFields.HiddenAt, now.toDate)
    )
    for {
      _ <- RepositoryIO.lift(MongoSessionOperations.insertOne(collection, session, request))
      hidden <- RepositoryIO.lift(
        MongoSessionOperations.updateOne(
          reportControl,
          session,
          MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId),
          hide,
          new UpdateOptions
        )
      )
      _ <- RepositoryIO.fromEither(MongoRepositorySupport.matchedOne(hidden))
    } yield freshReceiptId
  }

  override def statusForSubject(
      userId: UserId,
      receiptId: String
  ): RepositoryIO[AccountDeletionStatus] =
    if (Parsing.parseUuid(receiptId).isLeft) AccountDeletionStatus.NotFound.pure[RepositoryIO]
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "analyticsErasure.statusForSubject")({
          val requestFilter = MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, userId.value.toString),
            MongoFilter.eq(MongoFields.ReceiptId, receiptId)
          )
          val completionFilter = MongoFilter.and(
            MongoFilter.eq(MongoFields.Id, userId.value.toString),
            MongoFilter.eq(MongoFields.ReceiptId, receiptId)
          )
          RepositoryIO
            .lift(MongoSessionOperations.findOne(collection, None, requestFilter))
            .flatMap {
              case Some(document) =>
                RepositoryIO.fromEither(storedString(document, MongoFields.State).flatMap {
                  case Some("Complete")               => Right(AccountDeletionStatus.Complete)
                  case Some("Pending" | "Processing") => Right(AccountDeletionStatus.Pending)
                  case _                              => Left(RepositoryError.InvalidStoredData)
                })
              case None =>
                RepositoryIO
                  .lift(MongoSessionOperations.findOne(completions, None, completionFilter))
                  .subflatMap(
                    _.fold[Either[RepositoryError, AccountDeletionStatus]](Right(AccountDeletionStatus.NotFound))(_ =>
                      Right(AccountDeletionStatus.Complete)
                    )
                  )
            }
        })
        .flatMap {
          case AccountDeletionStatus.Complete =>
            RepositoryIO
              .lift(new MongoInterviewSubjectCleanup(database, diagnostics, topics).complete(userId))
              .map(if (_) AccountDeletionStatus.Complete else AccountDeletionStatus.Pending)
          case other => other.pure[RepositoryIO]
        }

  override def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.purgeSubjectOutbox")(
        RepositoryIO
          .lift(
            outbox
              .flatMap(_.deleteMany(MongoFilter.in(MongoFields.SubjectIds, List(userId.value.toString)).bson))
          )
          .void
      )

  override def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.markComplete")(transactionRunner.run { session =>
        val requestId = userId.value.toString
        val user =
          RepositoryIO.lift(MongoSessionOperations.findOne(users, session, MongoFilter.eq(MongoFields.Id, requestId)))
        user
          .flatMap {
            case Some(document) if storedString(document, MongoFields.AccountStatus).contains(Some("Deleted")) =>
              val filter = MongoFilter.and(
                MongoFilter.eq(MongoFields.Id, requestId),
                MongoFilter.in(MongoFields.State, List("Pending", "Processing"))
              )
              val update = MongoUpdate.combine(
                MongoUpdate.set(MongoFields.State, "Complete"),
                MongoUpdate.set(MongoFields.CompletedAt, now.toDate),
                MongoUpdate.set(MongoFields.ExpiresAt, Date.from(now.plusSeconds(31L * 24L * 60L * 60L)))
              )
              val requests =
                RepositoryIO.lift(
                  MongoSessionOperations.updateOne(collection, session, filter, update, new UpdateOptions)
                )
              requests.flatMap { result =>
                if (result.getMatchedCount == 1L) persistCompletion(session, requestId, now)
                else {
                  val completed = RepositoryIO.lift(
                    MongoSessionOperations.findOne(
                      collection,
                      session,
                      MongoFilter
                        .and(MongoFilter.eq(MongoFields.Id, requestId), MongoFilter.eq(MongoFields.State, "Complete"))
                    )
                  )
                  completed.flatMap(
                    _.fold(RepositoryIO.raise(RepositoryError.Conflict))(_ =>
                      persistCompletion(session, requestId, now)
                    )
                  )
                }
              }
            case _ => RepositoryIO.raise(RepositoryError.Conflict)
          }
      })

  private def persistCompletion(
      session: Option[ClientSession[IO]],
      userId: String,
      now: Instant
  ): RepositoryIO[Unit] = {
    val request =
      RepositoryIO.lift(MongoSessionOperations.findOne(collection, session, MongoFilter.eq(MongoFields.Id, userId)))
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsErasure.persistCompletion")(
        request
          .subflatMap(_.flatTraverse(storedString(_, MongoFields.ReceiptId)))
          .flatMap { receiptId =>
            val updates = List(
              Some(MongoUpdate.setOnInsert(MongoFields.Id, userId)),
              Some(MongoUpdate.setOnInsert(MongoFields.CompletedAt, now.toDate)),
              receiptId.map(MongoUpdate.setOnInsert(MongoFields.ReceiptId, _))
            ).flatten
            val filter = MongoFilter.eq(MongoFields.Id, userId)
            val update = MongoUpdate.combine(updates*)
            val result = RepositoryIO.lift(
              MongoSessionOperations.updateOne(completions, session, filter, update, new UpdateOptions().upsert(true))
            )
            result.void
          }
      )
  }
}

object MongoAnalyticsErasureRequestRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics,
      topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair =
        com.example.graphQL.cats.domain.workflow.InterviewTopicPair.Default
  ): MongoAnalyticsErasureRequestRepository =
    new MongoAnalyticsErasureRequestRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics,
      topics
    )
}

/** Exposes the newest valid analytics snapshot published by the analytics batch. */
final class MongoAnalyticsReportRepository(
    database: MongoDatabase[IO],
    transactionRunner: MongoTransactionRunner,
    diagnostics: Diagnostics,
    clock: Clock[IO] = Clock[IO]
) extends AnalyticsReportRepository {
  private val collection = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsReportSnapshots)
  private val control = Mongo4catsCollections.documents(database, MongoCollections.AnalyticsReportControl)

  override def latest: RepositoryIO[Option[AnalyticsReportSnapshot]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "analyticsReport.latest")(transactionRunner.run { session =>
        for {
          stored <- RepositoryIO.lift(
            MongoSessionOperations
              .findOne(control, session, MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.ControlId))
          )
          state <- RepositoryIO.fromEither(
            stored.traverse(
              MongoHiringPersistenceCodecs
                .decodeAnalyticsReportControl(_)
                .leftMap(_ => RepositoryError.InvalidStoredData)
            )
          )
          published = state.filter(value =>
            value.state == AnalyticsReportSnapshotDocument.Published && value.generation >= 0L &&
              value.lastPublishedRevision >= 0L
          )
          now <- RepositoryIO.lift(clock.realTimeInstant)
          snapshot <- published.flatTraverse { control =>
            RepositoryIO
              .lift(
                MongoSessionOperations.findOne(
                  collection,
                  session,
                  MongoFilter.and(
                    MongoFilter.eq(MongoFields.Id, AnalyticsReportSnapshotDocument.CurrentId),
                    MongoFilter.eq(MongoFields.State, AnalyticsReportSnapshotDocument.Published),
                    MongoFilter.eq(MongoFields.Generation, control.generation),
                    MongoFilter.eq(MongoFields.Revision, control.lastPublishedRevision),
                    MongoFilter.gt(MongoFields.ExpiresAt, now.toDate)
                  )
                )
              )
              .subflatMap(
                _.traverse(document => MongoDocumentFields.toRepository(AnalyticsReportSnapshotDocument.read(document)))
              )
          }
        } yield snapshot
      })
}

object MongoAnalyticsReportRepository {
  def transactional(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      diagnostics: Diagnostics
  ): MongoAnalyticsReportRepository =
    new MongoAnalyticsReportRepository(
      database,
      MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics),
      diagnostics
    )
}

private[mongo] object AnalyticsReportSnapshotDocument {
  import MongoDocumentFields.*

  val CurrentId = "current"
  val ControlId = "analytics-report"
  val Hidden = "Hidden"
  val Unpublished = "Unpublished"
  val Published = "Published"

  def write(snapshot: AnalyticsReportSnapshot, expiresAt: Instant): Document = {
    val document = new Document(MongoFields.Id, CurrentId)
      .append(MongoFields.State, "Published")
      .append(MongoFields.AsOf, snapshot.asOf.toDate)
      .append(MongoFields.ExpiresAt, expiresAt.toDate)
      .append(MongoFields.Funnel, snapshot.funnel.map(writeFunnel).asJava)
      .append(MongoFields.SkillPostingActivity, snapshot.skillPostingActivity.map(writeSkill).asJava)
    snapshot.timeToHire.fold(document)(value => document.append(MongoFields.TimeToHire, writeTimeToHire(value)))
  }

  def read(document: Document): Read[AnalyticsReportSnapshot] =
    for {
      asOf <- requiredInstant(document, MongoFields.AsOf)
      funnel <- requiredDocumentVector(document, MongoFields.Funnel).flatMap(_.toList.traverse(readFunnel))
      skills <- requiredDocumentVector(document, MongoFields.SkillPostingActivity).flatMap(_.toList.traverse(readSkill))
      timeToHire <- optionalDocument(document, MongoFields.TimeToHire).flatMap(_.traverse(readTimeToHire))
    } yield AnalyticsReportSnapshot(asOf, funnel, timeToHire, skills)

  private def writeFunnel(value: AnalyticsFunnelDay): Document =
    new Document(MongoFields.Day, value.day.toDate)
      .append(MongoFields.Created, value.created)
      .append(MongoFields.Accepted, value.accepted)
      .append(MongoFields.Declined, value.declined)
      .append(MongoFields.Interview, value.interview)
      .append(MongoFields.Hired, value.hired)
      .append(MongoFields.Rejected, value.rejected)

  private def writeTimeToHire(value: AnalyticsTimeToHire): Document =
    new Document(MongoFields.P50Hours, value.p50Hours)
      .append(MongoFields.P75Hours, value.p75Hours)
      .append(MongoFields.P90Hours, value.p90Hours)
      .append(MongoFields.P95Hours, value.p95Hours)
      .append(MongoFields.EligibleCount, value.eligibleCount)
      .append(MongoFields.ExcludedCount, value.excludedCount)

  private def writeSkill(value: AnalyticsSkillPostingDay): Document =
    new Document(MongoFields.Day, value.day.toDate)
      .append(MongoFields.Skill, value.skill)
      .append(MongoFields.Postings, value.postings)

  private def readFunnel(document: Document): Read[AnalyticsFunnelDay] =
    (
      requiredInstant(document, MongoFields.Day),
      count(document, MongoFields.Created),
      count(document, MongoFields.Accepted),
      count(document, MongoFields.Declined),
      count(document, MongoFields.Interview),
      count(document, MongoFields.Hired),
      count(document, MongoFields.Rejected)
    ).mapN(AnalyticsFunnelDay.apply)

  private def readTimeToHire(document: Document): Read[AnalyticsTimeToHire] =
    (
      requiredDouble(document, MongoFields.P50Hours),
      requiredDouble(document, MongoFields.P75Hours),
      requiredDouble(document, MongoFields.P90Hours),
      requiredDouble(document, MongoFields.P95Hours),
      count(document, MongoFields.EligibleCount),
      count(document, MongoFields.ExcludedCount)
    ).mapN(AnalyticsTimeToHire.apply)

  private def readSkill(document: Document): Read[AnalyticsSkillPostingDay] =
    (
      requiredInstant(document, MongoFields.Day),
      requiredString(document, MongoFields.Skill),
      count(document, MongoFields.Postings)
    ).mapN(AnalyticsSkillPostingDay.apply)

  private def count(document: Document, field: String): Read[Long] = requiredNumberAsLong(document, field)
}
