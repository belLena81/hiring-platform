package com.example.hiring.analytics

import com.example.hiring.analytics.batch.{AnalyticsLakehouseLock, AnalyticsLakehousePaths}
import cats.effect.IO
import cats.syntax.all.*
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

  /** A clear report means repository-controlled surfaces were scanned; operator evidence is not cryptographically attested. */
  final case class AuditSummary(checkedAt: Instant, deltaFilesScanned: Long, mongoDocumentsScanned: Long,
      operatorEvidence: String = "DIAGNOSTIC ONLY: operator-attested; not an authorization to retire a key")

  private val RegistryIdPattern = "[A-Za-z0-9-]{1,40}".r
  private val VerifierPattern = "[A-Za-z0-9_-]{43}".r

  /** Inspects current Delta snapshots, all retained physical files/logs, required Mongo collections and registry.
    * No HMAC material is accepted by this API. Writer and retention evidence remains an explicit operator input.
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
    AnalyticsLakehouseLock.resource(paths.root).use { _ =>
      IO.blocking {
        val blockers = Vector.newBuilder[String]
        if (Option(retiringKeyId).forall(id => !RegistryIdPattern.matches(id)))
          blockers += "retiring key ID is invalid"
        blockers ++= validateRetention(retention, now)
        blockers ++= validateWriters(writers, now)

        val deltaCount = scanDelta(spark, paths, Option(retiringKeyId).getOrElse(""), blockers)
        val mongoCount = scanMongo(database, Option(retiringKeyId).getOrElse(""), now, blockers)
        validateRegistry(spark, paths, Option(retiringKeyId).getOrElse(""), blockers)

        val errors = blockers.result()
        if (errors.nonEmpty) Left(errors)
        else Right(AuditSummary(now, deltaCount, mongoCount))
      }.adaptError { case NonFatal(_) => AnalyticsError.InvalidConfiguration("key retirement audit could not verify every required surface") }
    }.attempt.map {
      case Right(result) => result
      case Left(_: AnalyticsError) => Left(Vector("key retirement audit could not acquire the lakehouse audit boundary"))
      case Left(_) => Left(Vector("key retirement audit could not verify every required surface"))
    }

  private[analytics] def validateRetention(evidence: RetentionEvidence, now: Instant): Vector[String] = {
    val reasons = Vector.newBuilder[String]
    (evidence.kafka.barrierOffset, evidence.kafka.earliestAvailableOffset) match {
      case (Some(barrier), Some(earliest)) if barrier >= 0L && earliest >= barrier => ()
      case _ => reasons += "Kafka retention barrier is missing, invalid, or not yet passed"
    }
    Vector(
      "Delta data-file" -> evidence.deltaData,
      "Delta transaction-log" -> evidence.deltaLogs,
      "report" -> evidence.reports
    ).foreach {
      case (_, horizon) if horizon.retainedUntil.exists(deadline => !now.isBefore(deadline)) &&
          Option(horizon.evidenceReference).exists(_.trim.nonEmpty) => ()
      case (name, _) => reasons += s"$name retention horizon or its evidence reference is missing or has not elapsed"
    }
    if (Option(evidence.kafka.evidenceReference).forall(_.trim.isEmpty))
      reasons += "Kafka retention barrier evidence reference is missing"
    reasons.result()
  }

  private[analytics] def validateWriters(inventory: WriterInventory, now: Instant): Vector[String] = {
    val reasons = Vector.newBuilder[String]
    if (inventory == null || inventory.observedAt == null || inventory.observedAt.isAfter(now) ||
        inventory.observedAt.isBefore(now.minus(MaximumWriterEvidenceAge)) ||
        Option(inventory.coverageReference).forall(_.trim.isEmpty))
      reasons += "managed and unmanaged Delta writer inventory lacks fresh operator-attested coverage evidence"
    val all = Option(inventory).toVector.flatMap(i => i.managed ++ i.unmanaged)
    if (all.isEmpty) reasons += "writer inventory contains no individually accounted writer identities"
    all.foreach { writer =>
      if (writer == null || Option(writer.identity).forall(_.trim.isEmpty) ||
          Option(writer.evidenceReference).forall(_.trim.isEmpty))
        reasons += "a Delta writer identity or evidence reference is missing"
      Option(writer).map(_.disposition) match {
        case Some(WriterDisposition.Stopped | WriterDisposition.AccessRevoked) => ()
        case Some(WriterDisposition.Active) => reasons += "a Delta writer remains active"
        case _ => reasons += "a Delta writer is not accounted for as stopped or access-revoked"
      }
    }
    reasons.result()
  }

  private def scanDelta(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      keyId: String,
      blockers: scala.collection.mutable.Builder[String, Vector[String]]
  ): Long = {
    val tables = Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold,
      paths.manifests
    )
    var count = 0L
    tables.foreach { tablePath =>
      try {
        val root = new Path(tablePath)
        val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        if (fs.exists(root)) {
          visitFiles(fs, root) { file =>
            val pathText = file.getPath.toString
            if (pathText.contains(keyId + "_")) {
              blockers += "a retained Delta path references the retiring key"
              true
            } else if (pathText.endsWith(".parquet")) {
              count += 1L
              // Checkpoint Parquet stores add-file statistics and is scanned as checkpoint metadata,
              // separately from ordinary table data files.
              val isCheckpoint = pathText.contains("_delta_log") && pathText.contains("checkpoint")
              val frame = spark.read.parquet(pathText)
              if (containsKeyReference(frame, keyId)) {
                blockers += (if (isCheckpoint) "a retained Delta checkpoint references the retiring key"
                  else "a current or retained Delta data file references the retiring key")
                true
              } else false
            } else if (pathText.endsWith(".json") && pathText.contains("_delta_log")) {
              val source = scala.io.Source.fromInputStream(fs.open(file.getPath), "UTF-8")
              try {
                val found = source.getLines().exists(_.contains(keyId + "_"))
                if (found)
                  blockers += "a retained Delta transaction log references the retiring key"
                found
              } finally source.close()
            } else false
          }
          if (io.delta.tables.DeltaTable.isDeltaTable(spark, tablePath)) {
            val current = spark.read.format("delta").load(tablePath)
            if (containsKeyReference(current, keyId)) blockers += "a current Delta snapshot references the retiring key"
          }
        }
      } catch {
        case NonFatal(_) => blockers += "a current or retained Delta surface is unavailable"
      }
    }
    count
  }

  private def containsKeyReference(frame: org.apache.spark.sql.DataFrame, keyId: String): Boolean =
    if (frame.columns.isEmpty) false
    else {
      val fields = frame.columns.map(name => col(name))
      val serialized = frame.select(to_json(struct(fields*)).as("record"))
      serialized.filter(col("record").contains(keyId + "_")).limit(1).count() > 0L
    }

  /** Visits files incrementally and stops the table walk as soon as a reference is proven. */
  private def visitFiles(fs: org.apache.hadoop.fs.FileSystem, root: Path)(visit: org.apache.hadoop.fs.FileStatus => Boolean): Unit = {
    val pending = scala.collection.mutable.Stack(root)
    var stopped = false
    while (pending.nonEmpty && !stopped) {
      val statuses = fs.listStatus(pending.pop())
      var index = 0
      while (index < statuses.length && !stopped) {
        val status = statuses(index)
        if (status.isDirectory) pending.push(status.getPath)
        else if (status.isFile) stopped = visit(status)
        index += 1
      }
    }
  }

  private val MongoCollections = Vector(
    "analytics_report_snapshots",
    "analytics_report_runs",
    "analytics_report_control",
    "analytics_erasure_requests",
    "analytics_erasure_completions",
    "analytics_erasure_delta_files",
    "event_outbox",
    "hiring_migration_ledger",
    "outbox_subject_fences"
  )

  private val MaximumWriterEvidenceAge = java.time.Duration.ofHours(1)

  private def scanMongo(database: MongoDatabase, keyId: String, now: Instant,
      blockers: scala.collection.mutable.Builder[String, Vector[String]]): Long = {
    var count = 0L
    val activeSubjects = scala.collection.mutable.Set.empty[String]
    val names = database.listCollectionNames().into(new java.util.ArrayList[String]()).asScala.toSet
    val missing = MongoCollections.toSet -- names
    if (missing.nonEmpty) blockers += "one or more required Mongo report, erasure, or replay collections are unavailable"
    MongoCollections.filter(names.contains).foreach { collectionName =>
      val find = database.getCollection(collectionName, classOf[Document]).find()
      if (collectionName == "analytics_erasure_requests") find.limit(MaximumAuditedErasureSubjects + 1)
      val cursor = find.iterator()
      var collectionDocuments = 0
      try {
        while (cursor.hasNext) {
          val document = cursor.next()
          collectionDocuments += 1
          count += 1L
          if (collectionName == "analytics_erasure_requests" && collectionDocuments > MaximumAuditedErasureSubjects)
            blockers += "active erasure subject inventory exceeds its audit bound; no retirement decision is available"
          if (containsKeyReference(document, keyId))
            blockers += s"Mongo collection '$collectionName' contains a retiring-key reference"
          if (collectionName == "analytics_erasure_requests") {
            erasureRequestActivity(document, now) match {
              case Right(true) =>
                Option(document.getString("_id")) match {
                  case Some(subjectId) if activeSubjects.size < MaximumAuditedErasureSubjects => activeSubjects += subjectId
                  case _ => blockers += "active erasure subject inventory is malformed or exceeds its audit bound"
                }
                blockers += "an active or unexpired erasure marker/request remains"
              case Right(false) => ()
              case Left(reason) => blockers += reason
            }
          }
          if (collectionName == "event_outbox") {
            val subjectIds = Option(document.getList("subjectIds", classOf[String])).map(_.asScala.toSet)
            if (subjectIds.isEmpty || Option(document.getInteger("subjectRefsVersion")).forall(_ != 1))
              blockers += "outbox subject-reference migration or a stored row is incomplete"
            else if (subjectIds.exists(_.exists(activeSubjects.contains)))
              blockers += "outbox contains replay work for an active or unexpired erasure subject"
          }
          if (collectionName == "outbox_subject_fences" &&
              Option(document.getDate("leaseUntil")).exists(_.toInstant.isAfter(now)))
            blockers += "an active operational outbox publication fence remains"
        }
      } finally cursor.close()
    }
    if (names.contains("hiring_migration_ledger")) {
      val migration = database.getCollection("hiring_migration_ledger", classOf[Document])
        .find(com.mongodb.client.model.Filters.eq("_id", "003_event_outbox_subject_references"))
        .first()
      if (migration == null || !Option(migration.getString("state")).contains("Complete"))
        blockers += "outbox subject-reference migration is not complete"
    }
    count
  }

  private val MaximumAuditedErasureSubjects = 100000

  private[analytics] def erasureRequestActivity(document: Document, now: Instant): Either[String, Boolean] =
    Option(document.getString("state")) match {
      case Some("Pending" | "Processing") => Right(true)
      case Some("Complete") =>
        Option(document.get("expiresAt")) match {
          case Some(expiry: java.util.Date) => Right(expiry.toInstant.isAfter(now))
          case _ => Left("completed erasure request has a missing or invalid retention expiry")
        }
      case Some(_) => Left("erasure request has an unknown state")
      case None    => Left("erasure request has no state")
    }

  private def containsKeyReference(value: Any, keyId: String): Boolean = value match {
    case text: String => text.contains(keyId + "_")
    case document: Document => document.values().asScala.exists(containsKeyReference(_, keyId))
    case map: java.util.Map[?, ?] => map.values().asScala.exists(containsKeyReference(_, keyId))
    case values: java.lang.Iterable[?] => values.asScala.exists(containsKeyReference(_, keyId))
    case values: Iterable[?] => values.exists(containsKeyReference(_, keyId))
    case values: Array[?] => values.exists(containsKeyReference(_, keyId))
    case _ => false
  }

  private def validateRegistry(spark: SparkSession, paths: AnalyticsLakehousePaths, keyId: String,
      blockers: scala.collection.mutable.Builder[String, Vector[String]]): Unit =
    try {
      if (!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry)) {
        blockers += "permanent HMAC continuity registry is missing"
      } else {
        val registry = spark.read.format("delta").load(paths.hmacKeyRegistry)
        if (!Set("keyId", "verifier").subsetOf(registry.columns.toSet))
          blockers += "permanent HMAC continuity registry schema is malformed"
        else {
          val rows = registry.select("keyId", "verifier").limit(1001).collect().toVector
          if (rows.size > 1000) blockers += "permanent HMAC continuity registry exceeds the audited key limit"
          val valid = rows.nonEmpty && rows.forall { row =>
            val id = row.getString(0)
            val verifier = row.getString(1)
            id != null && RegistryIdPattern.matches(id) && verifier != null && VerifierPattern.matches(verifier)
          } && rows.map(_.getString(0)).distinct.size == rows.size
          if (!valid) blockers += "permanent HMAC continuity registry is empty, malformed, or contains duplicate IDs"
          else if (!rows.exists(_.getString(0) == keyId))
            blockers += "retiring key ID is absent from the permanent continuity registry"
        }
      }
    } catch {
      case NonFatal(_) => blockers += "permanent HMAC continuity registry is unavailable"
    }
}
