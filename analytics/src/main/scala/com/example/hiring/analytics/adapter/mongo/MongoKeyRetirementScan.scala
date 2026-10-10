package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.service.erasure.ErasureRequestState
import com.example.hiring.analytics.service.keyretirement.KeyRetirementEvidence.ScanResult

import cats.data.Chain
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.MongoDatabase
import org.bson.Document

import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Mongo side of the key-retirement audit: the bounded collection scan and its BSON readers. */
private[analytics] object MongoKeyRetirementScan {
  final case class MongoScanState(count: Long, activeSubjects: Set[String], blockers: Chain[String])

  /** Purely assesses one fetched Mongo row, retaining scan bounds and cross-collection active-subject tracking. */
  def reduceMongoObservation(
      state: MongoScanState,
      collectionName: String,
      document: Document,
      keyId: String,
      now: Instant,
      collectionDocuments: Int
  ): (MongoScanState, Int) = {
    val collectionCount = state.count + 1L
    val isErasure = collectionName == AnalyticsCollections.ErasureRequests
    val nextCollectionDocuments = collectionDocuments + 1
    val overflow =
      if (isErasure && nextCollectionDocuments > MaximumAuditedErasureSubjects)
        Chain.one("active erasure subject inventory exceeds its audit bound; no retirement decision is available")
      else Chain.empty[String]
    val keyReference =
      if (containsKeyReferenceInValue(document, keyId))
        Chain.one(s"Mongo collection '$collectionName' contains a retiring-key reference")
      else Chain.empty[String]
    val (activeSubjects, erasureBlockers) = if (isErasure) erasureRequestActivity(document, now) match {
      case Right(true) => {
        requiredString(document, AnalyticsCollections.Fields.Id) match {
          case Some(subjectId) if state.activeSubjects.size < MaximumAuditedErasureSubjects =>
            (state.activeSubjects + subjectId, Chain.one("an active or unexpired erasure marker/request remains"))
          case _ =>
            (
              state.activeSubjects,
              Chain(
                "active erasure subject inventory is malformed or exceeds its audit bound",
                "an active or unexpired erasure marker/request remains"
              )
            )
        }
      }
      case Right(false) => (state.activeSubjects, Chain.empty[String])
      case Left(reason) => (state.activeSubjects, Chain.one(reason))
    }
    else (state.activeSubjects, Chain.empty[String])
    val outboxBlockers = if (collectionName == AnalyticsCollections.EventOutbox) {
      val decodedSubjectIds = for {
        rows <- requiredStringList(document, AnalyticsCollections.Fields.SubjectIds)
        version <- requiredInt32(document, AnalyticsCollections.Fields.SubjectRefsVersion)
      } yield (rows.toSet, version)
      if (decodedSubjectIds.forall(_._2 != 1))
        Chain.one("outbox subject-reference migration or a stored row is incomplete")
      else if (decodedSubjectIds.exists(_._1.exists(state.activeSubjects.contains)))
        Chain.one("outbox contains replay work for an active or unexpired erasure subject")
      else Chain.empty[String]
    } else Chain.empty[String]
    val fenceLease = if (collectionName == AnalyticsCollections.OutboxSubjectFences) {
      optionalDate(document, AnalyticsCollections.Fields.LeaseUntil)
    } else Right(None)
    val fenceBlockers = fenceLease match {
      case Left(_) => Chain.one("an operational outbox publication fence has a malformed lease")
      case Right(Some(until)) if until.toInstant.isAfter(now) =>
        Chain.one("an active operational outbox publication fence remains")
      case _ => Chain.empty[String]
    }
    (
      state.copy(
        count = collectionCount,
        activeSubjects = activeSubjects,
        blockers = state.blockers ++ overflow ++ keyReference ++ erasureBlockers ++ outboxBlockers ++ fenceBlockers
      ),
      nextCollectionDocuments
    )
  }

  private val MongoCollections = Vector(
    AnalyticsCollections.ReportSnapshots,
    AnalyticsCollections.ReportRuns,
    AnalyticsCollections.ReportControl,
    AnalyticsCollections.ErasureRequests,
    AnalyticsCollections.ErasureCompletions,
    AnalyticsCollections.ErasureDeltaFiles,
    AnalyticsCollections.EventOutbox,
    AnalyticsCollections.HiringMigrationLedger,
    AnalyticsCollections.OutboxSubjectFences
  )

  def scan[F[_]: Async](
      database: MongoDatabase,
      keyId: String,
      now: Instant,
      streams: MongoPublisherStream,
      inventoriedCollections: Vector[String]
  ): F[ScanResult] = {
    streams.stream[F, String](database.listCollectionNames()).compile.toVector.map(_.toSet).flatMap { names =>
      val missing = MongoCollections.toSet -- names
      val missingBlockers =
        if (missing.nonEmpty)
          Chain.one("one or more required Mongo report, erasure, or replay collections are unavailable")
        else Chain.empty[String]
      val initial = MongoScanState(0L, Set.empty, missingBlockers)
      val optionalControls = inventoriedCollections.filterNot(MongoCollections.contains).filter(names.contains)
      (MongoCollections.filter(names.contains) ++ optionalControls)
        .foldM(initial) { (state, collectionName) =>
          streams
            .stream {
              val find = database.getCollection(collectionName, classOf[Document]).find()
              if (collectionName == AnalyticsCollections.ErasureRequests)
                find.limit(MaximumAuditedErasureSubjects + 1)
              else find
            }
            .compile
            .fold((state, 0)) { case ((current, collectionDocuments), document) =>
              reduceMongoObservation(current, collectionName, document, keyId, now, collectionDocuments)
            }
            .map(_._1)
        }
        .flatMap { scanned =>
          if (names.contains(AnalyticsCollections.HiringMigrationLedger))
            streams
              .optional {
                database
                  .getCollection(AnalyticsCollections.HiringMigrationLedger, classOf[Document])
                  .find(
                    new Document(
                      AnalyticsCollections.Fields.Id,
                      AnalyticsCollections.MigrationIds.OutboxSubjectReferences
                    )
                  )
                  .first()
              }
              .map { migration =>
                val migrationBlockers =
                  if (
                    migration.forall { row =>
                      !requiredString(row, AnalyticsCollections.Fields.State).contains("Complete")
                    }
                  )
                    Chain.one("outbox subject-reference migration is not complete")
                  else Chain.empty[String]
                ScanResult(scanned.count, scanned.blockers ++ migrationBlockers)
              }
          else Async[F].pure(ScanResult(scanned.count, scanned.blockers))
        }
    }
  }

  val MaximumAuditedErasureSubjects = 100000

  private def requiredString(document: Document, field: String): Option[String] =
    Option(document.get(field)).collect { case value: String => value }

  private def requiredInt32(document: Document, field: String): Option[Int] =
    Option(document.get(field)).collect { case value: java.lang.Integer => value.intValue() }

  private def requiredStringList(document: Document, field: String): Option[Vector[String]] =
    Option(document.get(field))
      .collect { case values: java.util.List[?] => values.asScala.toVector }
      .flatMap(_.traverse { case text: String => Some(text); case _ => None })

  private def optionalDate(document: Document, field: String): Either[Unit, Option[java.util.Date]] =
    Option(document.get(field)) match {
      case None                        => Right(None)
      case Some(value: java.util.Date) => Right(Some(value))
      case Some(_)                     => Left(())
    }

  def erasureRequestActivity(document: Document, now: Instant): Either[String, Boolean] = {
    requiredString(document, AnalyticsCollections.Fields.State)
      .toRight("erasure request has no state")
      .flatMap(state => ErasureRequestState.fromString(state).toRight("erasure request has an unknown state"))
      .flatMap {
        case ErasureRequestState.Pending | ErasureRequestState.Processing => Right(true)
        case ErasureRequestState.Complete                                 =>
          requiredDate(document, AnalyticsCollections.Fields.ExpiresAt)
            .toRight("completed erasure request has a missing or invalid retention expiry")
            .map(_.toInstant.isAfter(now))
      }
  }

  private def requiredDate(document: Document, field: String): Option[java.util.Date] =
    Option(document.get(field)).collect { case value: java.util.Date => value }

  def containsKeyReferenceInValue(value: Any, keyId: String): Boolean = value match {
    case text: String                  => text.contains(keyId + "_")
    case document: Document            => document.values().asScala.exists(containsKeyReferenceInValue(_, keyId))
    case map: java.util.Map[?, ?]      => map.values().asScala.exists(containsKeyReferenceInValue(_, keyId))
    case values: java.lang.Iterable[?] => values.asScala.exists(containsKeyReferenceInValue(_, keyId))
    case values: Iterable[?]           => values.exists(containsKeyReferenceInValue(_, keyId))
    case values: Array[?]              => values.exists(containsKeyReferenceInValue(_, keyId))
    case _                             => false
  }
}
