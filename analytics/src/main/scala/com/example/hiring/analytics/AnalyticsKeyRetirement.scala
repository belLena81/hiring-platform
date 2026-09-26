package com.example.hiring.analytics

import com.example.hiring.analytics.batch.{AnalyticsLakehouseLock, AnalyticsLakehousePaths}
import cats.data.Chain
import cats.effect.IO
import cats.syntax.all.*
import com.example.hiring.analytics.mongo.{AnalyticsCollections, MongoCursorStream}
import com.mongodb.client.MongoDatabase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.bson.Document

import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Read-only audit of repo-controlled data plus explicit operator evidence for every Delta writer. */
private[analytics] object AnalyticsKeyRetirement {
  enum WriterDisposition {
    case Stopped
    case AccessRevoked
    case Active
    case Unknown
  }

  final case class WriterRecord(
      identity: String,
      disposition: WriterDisposition,
      evidenceReference: String
  )

  /** Inventory references must point to operator evidence covering deployments, jobs, and unmanaged writers. */
  final case class WriterInventory(
      observedAt: Instant,
      coverageReference: String,
      managed: Vector[WriterRecord],
      unmanaged: Vector[WriterRecord]
  )

  final case class KafkaRetentionEvidence(
      barrierOffset: Option[Long],
      earliestAvailableOffset: Option[Long],
      evidenceReference: String
  )

  final case class RetentionHorizon(retainedUntil: Option[Instant], evidenceReference: String)

  final case class RetentionEvidence(
      kafka: KafkaRetentionEvidence,
      deltaData: RetentionHorizon,
      deltaLogs: RetentionHorizon,
      reports: RetentionHorizon
  )

  /** A clear report means repository-controlled surfaces were scanned; operator evidence is not cryptographically
    * attested.
    */
  final case class AuditSummary(
      checkedAt: Instant,
      deltaFilesScanned: Long,
      mongoDocumentsScanned: Long,
      operatorEvidence: String = "DIAGNOSTIC ONLY: operator-attested; not an authorization to retire a key"
  )

  private val RegistryIdPattern = "[A-Za-z0-9-]{1,40}".r
  private val VerifierPattern = "[A-Za-z0-9_-]{43}".r
  private[analytics] final case class ScanResult(count: Long, blockers: Chain[String] = Chain.empty)
  private[analytics] final case class MongoScanState(count: Long, activeSubjects: Set[String], blockers: Chain[String])

  /** Purely assesses one fetched Mongo row, retaining scan bounds and cross-collection active-subject tracking. */
  private[analytics] def reduceMongoObservation(
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
      case Right(true) =>
        Option(document.getString(AnalyticsCollections.Fields.Id)) match {
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
      case Right(false) => (state.activeSubjects, Chain.empty[String])
      case Left(reason) => (state.activeSubjects, Chain.one(reason))
    }
    else (state.activeSubjects, Chain.empty[String])
    val outboxBlockers = if (collectionName == AnalyticsCollections.EventOutbox) {
      val subjectIds =
        Option(document.getList(AnalyticsCollections.Fields.SubjectIds, classOf[String])).map(_.asScala.toSet)
      if (
        subjectIds.isEmpty || Option(document.getInteger(AnalyticsCollections.Fields.SubjectRefsVersion)).forall(_ != 1)
      ) Chain.one("outbox subject-reference migration or a stored row is incomplete")
      else if (subjectIds.exists(_.exists(state.activeSubjects.contains)))
        Chain.one("outbox contains replay work for an active or unexpired erasure subject")
      else Chain.empty[String]
    } else Chain.empty[String]
    val fenceBlockers =
      if (
        collectionName == AnalyticsCollections.OutboxSubjectFences &&
        Option(document.getDate(AnalyticsCollections.Fields.LeaseUntil)).exists(_.toInstant.isAfter(now))
      ) Chain.one("an active operational outbox publication fence remains")
      else Chain.empty[String]
    (
      state.copy(
        count = collectionCount,
        activeSubjects = activeSubjects,
        blockers = state.blockers ++ overflow ++ keyReference ++ erasureBlockers ++ outboxBlockers ++ fenceBlockers
      ),
      nextCollectionDocuments
    )
  }

  /** Inspects current Delta snapshots, all retained physical files/logs, required Mongo collections and registry. No
    * HMAC material is accepted by this API. Writer and retention evidence remains an explicit operator input.
    */
  def audit(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      database: MongoDatabase,
      retiringKeyId: String,
      retention: RetentionEvidence,
      writers: WriterInventory,
      now: Instant
  ): IO[Either[Vector[String], AuditSummary]] =
    AnalyticsLakehouseLock
      .resource(paths.root)
      .use { _ =>
        val keyBlockers =
          if (Option(retiringKeyId).forall(id => !RegistryIdPattern.matches(id)))
            Chain.one("retiring key ID is invalid")
          else Chain.empty[String]
        val evidenceBlockers =
          Chain.fromSeq(validateRetention(retention, now)) ++ Chain.fromSeq(validateWriters(writers, now))
        IO.blocking {
          val delta = scanDelta(spark, paths, Option(retiringKeyId).getOrElse(""))
          val registry = validateRegistry(spark, paths, Option(retiringKeyId).getOrElse(""))
          (delta, registry)
        }.adaptError { case NonFatal(_) =>
          AnalyticsError.InvalidConfiguration("key retirement audit could not verify every required surface")
        }.flatMap { case (delta, registryBlockers) =>
          scanMongo(database, Option(retiringKeyId).getOrElse(""), now).map { mongo =>
            val blockers = keyBlockers ++ evidenceBlockers ++ delta.blockers ++ mongo.blockers ++ registryBlockers
            val errors = blockers.toList.toVector
            if (errors.nonEmpty) Left(errors)
            else Right(AuditSummary(now, delta.count, mongo.count))
          }
        }
      }
      .attempt
      .map {
        case Right(result)           => result
        case Left(_: AnalyticsError) =>
          Left(Vector("key retirement audit could not acquire the lakehouse audit boundary"))
        case Left(_) => Left(Vector("key retirement audit could not verify every required surface"))
      }

  private[analytics] def validateRetention(evidence: RetentionEvidence, now: Instant): Vector[String] = {
    val kafkaReasons = (evidence.kafka.barrierOffset, evidence.kafka.earliestAvailableOffset) match {
      case (Some(barrier), Some(earliest)) if barrier >= 0L && earliest >= barrier => Chain.empty[String]
      case _ => Chain.one("Kafka retention barrier is missing, invalid, or not yet passed")
    }
    val horizonReasons = Vector(
      "Delta data-file" -> evidence.deltaData,
      "Delta transaction-log" -> evidence.deltaLogs,
      "report" -> evidence.reports
    ).foldLeft(Chain.empty[String]) {
      case (reasons, (_, horizon))
          if horizon.retainedUntil.exists(deadline => !now.isBefore(deadline)) &&
            Option(horizon.evidenceReference).exists(_.trim.nonEmpty) =>
        reasons
      case (reasons, (name, _)) =>
        reasons.append(s"$name retention horizon or its evidence reference is missing or has not elapsed")
    }
    val evidenceReasons =
      if (Option(evidence.kafka.evidenceReference).forall(_.trim.isEmpty))
        Chain.one("Kafka retention barrier evidence reference is missing")
      else Chain.empty[String]
    (kafkaReasons ++ horizonReasons ++ evidenceReasons).toList.toVector
  }

  private[analytics] def validateWriters(inventory: WriterInventory, now: Instant): Vector[String] = {
    val freshnessReasons =
      if (
        inventory == null || inventory.observedAt == null || inventory.observedAt.isAfter(now) ||
        inventory.observedAt.isBefore(now.minus(MaximumWriterEvidenceAge)) ||
        Option(inventory.coverageReference).forall(_.trim.isEmpty)
      )
        Chain.one("managed and unmanaged Delta writer inventory lacks fresh operator-attested coverage evidence")
      else Chain.empty[String]
    val all = Option(inventory).toVector.flatMap(i => i.managed ++ i.unmanaged)
    val identityReasons =
      if (all.isEmpty) Chain.one("writer inventory contains no individually accounted writer identities")
      else Chain.empty[String]
    val writerReasons = all.foldLeft(Chain.empty[String]) { (reasons, writer) =>
      val missing =
        if (
          writer == null || Option(writer.identity).forall(_.trim.isEmpty) ||
          Option(writer.evidenceReference).forall(_.trim.isEmpty)
        )
          Chain.one("a Delta writer identity or evidence reference is missing")
        else Chain.empty[String]
      val disposition = Option(writer).map(_.disposition) match {
        case Some(WriterDisposition.Stopped | WriterDisposition.AccessRevoked) => Chain.empty[String]
        case Some(WriterDisposition.Active) => Chain.one("a Delta writer remains active")
        case _ => Chain.one("a Delta writer is not accounted for as stopped or access-revoked")
      }
      reasons ++ missing ++ disposition
    }
    (freshnessReasons ++ identityReasons ++ writerReasons).toList.toVector
  }

  private def scanDelta(spark: SparkSession, paths: AnalyticsLakehousePaths, keyId: String): ScanResult = {
    val tables = Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold,
      paths.manifests
    )
    tables.foldLeft(ScanResult(0L)) { (total, tablePath) =>
      val tableResult = try {
        val root = new Path(tablePath)
        val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        if (fs.exists(root)) {
          val files = visitFiles(fs, root) { file =>
            val pathText = file.getPath.toString
            if (pathText.contains(keyId + "_")) {
              ScanResult(0L, Chain.one("a retained Delta path references the retiring key"))
            } else if (pathText.endsWith(".parquet")) {
              // Checkpoint Parquet stores add-file statistics and is scanned as checkpoint metadata,
              // separately from ordinary table data files.
              val frame = spark.read.parquet(pathText)
              deltaParquetVerdict(pathText, containsKeyReferenceInDataFrame(frame, keyId))
            } else if (pathText.endsWith(".json") && pathText.contains("_delta_log")) {
              val source = scala.io.Source.fromInputStream(fs.open(file.getPath), "UTF-8")
              try {
                val found = source.getLines().exists(_.contains(keyId + "_"))
                if (found) ScanResult(0L, Chain.one("a retained Delta transaction log references the retiring key"))
                else ScanResult(0L)
              } finally source.close()
            } else ScanResult(0L)
          }
          if (io.delta.tables.DeltaTable.isDeltaTable(spark, tablePath)) {
            val current = spark.read.format("delta").load(tablePath)
            if (containsKeyReferenceInDataFrame(current, keyId))
              files.copy(blockers = files.blockers.append("a current Delta snapshot references the retiring key"))
            else files
          } else files
        } else ScanResult(0L)
      } catch { case NonFatal(_) => ScanResult(0L, Chain.one("a current or retained Delta surface is unavailable")) }
      ScanResult(total.count + tableResult.count, total.blockers ++ tableResult.blockers)
    }
  }

  private def containsKeyReferenceInDataFrame(frame: org.apache.spark.sql.DataFrame, keyId: String): Boolean =
    if (frame.columns.isEmpty) false
    else {
      val fields = frame.columns.map(name => col(name))
      val serialized = frame.select(to_json(struct(fields*)).as("record"))
      serialized.filter(col("record").contains(keyId + "_")).limit(1).count() > 0L
    }

  private[analytics] def deltaParquetVerdict(path: String, containsReference: Boolean): ScanResult =
    if (!containsReference) ScanResult(1L)
    else if (path.contains("_delta_log") && path.contains("checkpoint"))
      ScanResult(1L, Chain.one("a retained Delta checkpoint references the retiring key"))
    else ScanResult(1L, Chain.one("a current or retained Delta data file references the retiring key"))

  /** Visits files incrementally and stops the table walk as soon as a reference is proven. */
  private def visitFiles(fs: org.apache.hadoop.fs.FileSystem, root: Path)(
      visit: org.apache.hadoop.fs.FileStatus => ScanResult
  ): ScanResult = {
    @annotation.tailrec
    def walk(pending: List[Path], result: ScanResult): ScanResult = pending match {
      case Nil               => result
      case directory :: rest =>
        val (nextDirs, nextResult, stopped) =
          fs.listStatus(directory).iterator.foldLeft((List.empty[Path], result, false)) {
            case ((dirs, accumulated, true), _)                             => (dirs, accumulated, true)
            case ((dirs, accumulated, false), status) if status.isDirectory =>
              (status.getPath :: dirs, accumulated, false)
            case ((dirs, accumulated, false), status) if status.isFile =>
              val found = visit(status)
              val combined = ScanResult(accumulated.count + found.count, accumulated.blockers ++ found.blockers)
              (dirs, combined, found.blockers.nonEmpty)
            case (state, _) => state
          }
        if (stopped) nextResult else walk(nextDirs.reverse ::: rest, nextResult)
    }
    walk(List(root), ScanResult(0L))
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

  private val MaximumWriterEvidenceAge = java.time.Duration.ofHours(1)

  private def scanMongo(database: MongoDatabase, keyId: String, now: Instant): IO[ScanResult] = {
    IO.blocking(database.listCollectionNames().into(new java.util.ArrayList[String]()).asScala.toSet).flatMap { names =>
      val missing = MongoCollections.toSet -- names
      val missingBlockers =
        if (missing.nonEmpty)
          Chain.one("one or more required Mongo report, erasure, or replay collections are unavailable")
        else Chain.empty[String]
      val initial = MongoScanState(0L, Set.empty, missingBlockers)
      MongoCollections
        .filter(names.contains)
        .foldM(initial) { (state, collectionName) =>
          MongoCursorStream {
            val find = database.getCollection(collectionName, classOf[Document]).find()
            if (collectionName == AnalyticsCollections.ErasureRequests) find.limit(MaximumAuditedErasureSubjects + 1)
            find.iterator()
          }.compile
            .fold((state, 0)) { case ((current, collectionDocuments), document) =>
              reduceMongoObservation(current, collectionName, document, keyId, now, collectionDocuments)
            }
            .map(_._1)
        }
        .flatMap { scanned =>
          if (names.contains(AnalyticsCollections.HiringMigrationLedger))
            IO.blocking {
              val migration = database
                .getCollection(AnalyticsCollections.HiringMigrationLedger, classOf[Document])
                .find(
                  com.mongodb.client.model.Filters
                    .eq(AnalyticsCollections.Fields.Id, AnalyticsCollections.MigrationIds.OutboxSubjectReferences)
                )
                .first()
              val migrationBlockers =
                if (
                  migration == null || !Option(migration.getString(AnalyticsCollections.Fields.State))
                    .contains("Complete")
                )
                  Chain.one("outbox subject-reference migration is not complete")
                else Chain.empty[String]
              ScanResult(scanned.count, scanned.blockers ++ migrationBlockers)
            }
          else IO.pure(ScanResult(scanned.count, scanned.blockers))
        }
    }
  }

  private val MaximumAuditedErasureSubjects = 100000

  private[analytics] def erasureRequestActivity(document: Document, now: Instant): Either[String, Boolean] =
    Option(document.getString(AnalyticsCollections.Fields.State)) match {
      case Some("Pending" | "Processing") => Right(true)
      case Some("Complete")               =>
        Option(document.get(AnalyticsCollections.Fields.ExpiresAt)) match {
          case Some(expiry: java.util.Date) => Right(expiry.toInstant.isAfter(now))
          case _ => Left("completed erasure request has a missing or invalid retention expiry")
        }
      case Some(_) => Left("erasure request has an unknown state")
      case None    => Left("erasure request has no state")
    }

  private[analytics] def containsKeyReferenceInValue(value: Any, keyId: String): Boolean = value match {
    case text: String                  => text.contains(keyId + "_")
    case document: Document            => document.values().asScala.exists(containsKeyReferenceInValue(_, keyId))
    case map: java.util.Map[?, ?]      => map.values().asScala.exists(containsKeyReferenceInValue(_, keyId))
    case values: java.lang.Iterable[?] => values.asScala.exists(containsKeyReferenceInValue(_, keyId))
    case values: Iterable[?]           => values.exists(containsKeyReferenceInValue(_, keyId))
    case values: Array[?]              => values.exists(containsKeyReferenceInValue(_, keyId))
    case _                             => false
  }

  private def validateRegistry(spark: SparkSession, paths: AnalyticsLakehousePaths, keyId: String): Chain[String] =
    try {
      if (!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry)) {
        Chain.one("permanent HMAC continuity registry is missing")
      } else {
        val registry = spark.read.format("delta").load(paths.hmacKeyRegistry)
        if (!Set("keyId", "verifier").subsetOf(registry.columns.toSet))
          Chain.one("permanent HMAC continuity registry schema is malformed")
        else {
          val rows = registry.select("keyId", "verifier").limit(1001).collect().toVector
          val sizeBlockers =
            if (rows.size > 1000) Chain.one("permanent HMAC continuity registry exceeds the audited key limit")
            else Chain.empty[String]
          val valid = rows.nonEmpty && rows.forall { row =>
            val id = row.getString(0)
            val verifier = row.getString(1)
            id != null && RegistryIdPattern.matches(id) && verifier != null && VerifierPattern.matches(verifier)
          } && rows.map(_.getString(0)).distinct.size == rows.size
          val validityBlockers =
            if (!valid) Chain.one("permanent HMAC continuity registry is empty, malformed, or contains duplicate IDs")
            else if (!rows.exists(_.getString(0) == keyId))
              Chain.one("retiring key ID is absent from the permanent continuity registry")
            else Chain.empty[String]
          sizeBlockers ++ validityBlockers
        }
      }
    } catch {
      case NonFatal(_) => Chain.one("permanent HMAC continuity registry is unavailable")
    }
}
