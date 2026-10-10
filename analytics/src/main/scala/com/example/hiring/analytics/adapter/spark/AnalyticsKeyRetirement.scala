package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.batch.{AnalyticsStorageInventory, AnalyticsStorageKind}

import cats.data.{Chain, NonEmptyChain}
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.mongo.{MongoKeyRetirementScan, MongoPublisherStream}
import com.example.hiring.analytics.service.keyretirement.KeyRetirementEvidence
import com.mongodb.reactivestreams.client.MongoDatabase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct, to_json}

import java.time.Instant
import scala.util.control.NonFatal

/** Read-only audit of repo-controlled data plus explicit operator evidence for every Delta writer. */
private[analytics] object AnalyticsKeyRetirement {
  export com.example.hiring.analytics.service.keyretirement.KeyRetirementEvidence.ScanResult
  export com.example.hiring.analytics.domain.{
    KafkaRetentionEvidence,
    RetentionEvidence,
    RetentionHorizon,
    WriterDisposition,
    WriterInventory,
    WriterRecord
  }

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

  private[analytics] def validRegistryRows(rows: Vector[(String, String)]): Boolean =
    rows.nonEmpty && rows.forall { case (id, verifier) =>
      Option(id).exists(RegistryIdPattern.matches) && Option(verifier).exists(VerifierPattern.matches)
    } && rows.map(_._1).distinct.size == rows.size

  private val unverifiedSurface = NonEmptyChain.one("key retirement audit could not verify every required surface")

  /** Inspects current Delta snapshots, all retained physical files/logs, required Mongo collections and registry. No
    * HMAC material is accepted by this API. Writer and retention evidence remains an explicit operator input.
    */
  def audit[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      database: MongoDatabase,
      retiringKeyId: String,
      retention: RetentionEvidence,
      writers: WriterInventory,
      now: Instant,
      lakehouseLock: AnalyticsLakehouseLock[F],
      streams: MongoPublisherStream,
      sparkExecution: SparkExecution[F],
      storageInventory: Option[AnalyticsStorageInventory] = None
  ): F[Either[NonEmptyChain[String], AuditSummary]] =
    lakehouseLock
      .resource(paths.root)
      .use { _ =>
        auditUnderLock(
          spark,
          paths,
          database,
          retiringKeyId,
          retention,
          writers,
          now,
          streams,
          sparkExecution,
          storageInventory
        )
      }
      .attempt
      .map {
        case Right(result)                                        => result
        case Left(_: AnalyticsError.KeyRetirementAuditUnverified) => Left(unverifiedSurface)
        case Left(_: AnalyticsError)                              =>
          Left(NonEmptyChain.one("key retirement audit could not acquire the lakehouse audit boundary"))
        case Left(_) => Left(unverifiedSurface)
      }

  /** Caller already owns the shared lakehouse mutex; used by the guarded authorization transaction. */
  private[analytics] def auditUnderLock[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      database: MongoDatabase,
      retiringKeyId: String,
      retention: RetentionEvidence,
      writers: WriterInventory,
      now: Instant,
      streams: MongoPublisherStream,
      sparkExecution: SparkExecution[F],
      storageInventory: Option[AnalyticsStorageInventory] = None
  ): F[Either[NonEmptyChain[String], AuditSummary]] = {
    val keyBlockers =
      if (!RegistryIdPattern.matches(retiringKeyId))
        Chain.one("retiring key ID is invalid")
      else Chain.empty[String]
    val evidenceBlockers =
      (KeyRetirementEvidence.validateRetention(retention, now), KeyRetirementEvidence.validateWriters(writers, now))
        .mapN((_, _) => ())
        .fold(_.toChain, _ => Chain.empty[String])
    DeltaPurgeRewrite.verifyRecovered(
      paths.root,
      spark.sparkContext.hadoopConfiguration,
      MongoKeyRetirementScan.MaximumAuditedErasureSubjects,
      sparkExecution
    ) *> sparkExecution {
      val inventory = storageInventory.getOrElse(paths.inventory)
      val delta = scanDelta(spark, paths, retiringKeyId)
      val external = scanCheckpoints(spark, inventory, retiringKeyId)
      val lineage = scanLineageControls(spark, paths)
      val scratch = inventory.surfaces
        .filter(surface => surface.kind == AnalyticsStorageKind.Scratch && !surface.location.endsWith("*"))
        .foldLeft(ScanResult(0L)) { (total, surface) =>
          val root = new Path(SparkPhysicalLocation.resolve(surface.location))
          val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
          val remaining = fs.exists(root) && fs.listFiles(root, true).hasNext
          ScanResult(
            total.count,
            total.blockers ++ (if (remaining) Chain.one("inventoried Spark spill files remain")
                               else Chain.empty[String])
          )
        }
      val streamingPresent = Vector(paths.streamingProgress, paths.streamingDecisions, paths.streamingLineage).exists {
        path =>
          val value = new Path(SparkPhysicalLocation.resolve(path))
          value.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(value)
      }
      val missingCheckpoints =
        if (streamingPresent && !inventory.surfaces.exists(_.kind == AnalyticsStorageKind.SparkCheckpoint))
          Chain.one("streaming retirement requires an explicit checkpoint storage inventory")
        else Chain.empty[String]
      val missingSpill =
        if (
          streamingPresent && !inventory.surfaces
            .exists(surface => surface.kind == AnalyticsStorageKind.Scratch && !surface.location.endsWith("*"))
        )
          Chain.one("streaming retirement requires an explicit Spark spill storage inventory")
        else Chain.empty[String]
      val registry = validateRegistry(spark, paths, retiringKeyId)
      (
        ScanResult(
          delta.count + external.count + lineage.count,
          delta.blockers ++ external.blockers ++ lineage.blockers ++
            scratch.blockers ++ missingCheckpoints ++ missingSpill
        ),
        registry
      )
    }
      .adaptError { case NonFatal(cause) => AnalyticsError.KeyRetirementAuditUnverified(cause) }
      .flatMap { case (delta, registryBlockers) =>
        MongoKeyRetirementScan
          .scan[F](database, retiringKeyId, now, streams, paths.inventory.mongo.map(_.location))
          .map { mongo =>
            val blockers = keyBlockers ++ evidenceBlockers ++ delta.blockers ++ mongo.blockers ++ registryBlockers
            val errors = blockers.toList.toVector
            if (errors.nonEmpty) Left(NonEmptyChain.fromSeq(errors).get)
            else Right(AuditSummary(now, delta.count, mongo.count))
          }
      }
  }

  private def scanDelta(spark: SparkSession, paths: AnalyticsLakehousePaths, keyId: String): ScanResult = {
    // Permanent key verifiers intentionally retain the retiring key ID and are validated separately.
    val tables = paths.inventory.delta.map(_.location).filterNot(_ == paths.hmacKeyRegistry)
    tables.foldLeft(ScanResult(0L)) { (total, tablePath) =>
      val tableResult = try {
        val root = new Path(SparkPhysicalLocation.resolve(tablePath))
        val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        if (fs.exists(root)) {
          val files = visitFiles(fs, root) { file =>
            val pathText = file.getPath.toString
            if (pathText.contains(keyId + "_")) {
              ScanResult(0L, Chain.one("a retained Delta path references the retiring key"))
            } else if (pathText.endsWith(".parquet")) {
              // Checkpoint Parquet stores add-file statistics and is scanned as checkpoint metadata,
              // separately from ordinary table data files.
              val frame = spark.read.parquet(SparkPhysicalLocation.resolve(file.getPath.toUri.toASCIIString))
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
          if (DeltaTables.exists(spark, tablePath)) {
            val current = DeltaTables.read(spark, tablePath)
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

  private def scanCheckpoints(spark: SparkSession, inventory: AnalyticsStorageInventory, keyId: String): ScanResult =
    inventory.surfaces.filter(_.kind == AnalyticsStorageKind.SparkCheckpoint).foldLeft(ScanResult(0L)) {
      (total, surface) =>
        val root = new Path(SparkPhysicalLocation.resolve(surface.location))
        val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val scanned =
          if (!fs.exists(root)) ScanResult(0L, Chain.one("inventoried streaming checkpoint is unavailable"))
          else
            visitFiles(fs, root) { status =>
              val name = status.getPath.getName
              if (name.endsWith(".crc")) ScanResult(0L)
              else {
                val source = scala.io.Source.fromInputStream(fs.open(status.getPath), "UTF-8")
                try {
                  val unsafe = source.getLines().exists { line =>
                    line.contains(keyId + "_") || ForbiddenCheckpointFields.exists(line.contains)
                  }
                  if (unsafe || status.getPath.toString.contains("/state/"))
                    ScanResult(1L, Chain.one("streaming checkpoint contains subject material or unsupported state"))
                  else ScanResult(1L)
                } finally source.close()
              }
            }
        ScanResult(total.count + scanned.count, total.blockers ++ scanned.blockers)
    }

  private[analytics] def validLineageControl(name: String, body: String): Boolean = {
    val parts = body.split("\n", -1).toVector
    val streamId = parts.dropRight(4).mkString("\n")
    if (name.matches("[a-f0-9]{64}\\.query-id"))
      AccountSubjectId.from(body).isRight
    else if (name.matches("[a-f0-9]{64}\\.(identity|established)"))
      parts.size >= 5 && streamId.trim.nonEmpty && parts.takeRight(4).forall(_.matches("[a-f0-9]{64}")) &&
      com.example.hiring.analytics.domain.AnalyticsDigest
        .sha256Hex(streamId) == name.take(64)
    else false
  }

  private val MaximumLineageControlBytes = 4096
  private val ForbiddenCheckpointFields =
    Vector("\"payload\"", "\"rawValue\"", "\"actorId\"", "\"candidateId\"", "\"subjectToken\"", "\"subjectTokens\"")

  private def scanLineageControls(spark: SparkSession, paths: AnalyticsLakehousePaths): ScanResult = {
    val root = new Path(SparkPhysicalLocation.resolve(paths.streamingLineage))
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(root)) ScanResult(0L)
    else
      visitFiles(fs, root) { status =>
        val name = status.getPath.getName
        if (name.endsWith(".crc")) ScanResult(0L)
        else {
          val input = fs.open(status.getPath)
          val body = try
            new String(input.readNBytes(MaximumLineageControlBytes + 1), java.nio.charset.StandardCharsets.UTF_8)
          finally input.close()
          val identity = new Path(root, name.take(64) + StreamingLineageFiles.IdentitySuffix)
          val query = new Path(root, name.take(64) + StreamingLineageFiles.QueryIdSuffix)
          val complete =
            !name.endsWith(StreamingLineageFiles.EstablishedSuffix) || (fs.exists(identity) && fs.exists(query))
          val paired = !name.endsWith(StreamingLineageFiles.QueryIdSuffix) || fs.exists(identity)
          if (
            body
              .getBytes(java.nio.charset.StandardCharsets.UTF_8)
              .length <= MaximumLineageControlBytes && validLineageControl(name, body) && complete && paired
          )
            ScanResult(1L)
          else ScanResult(1L, Chain.one("permanent streaming lineage metadata is malformed or incomplete"))
        }
      }
  }

  private def validateRegistry(spark: SparkSession, paths: AnalyticsLakehousePaths, keyId: String): Chain[String] =
    try {
      if (!DeltaTables.exists(spark, paths.hmacKeyRegistry)) {
        Chain.one("permanent HMAC continuity registry is missing")
      } else {
        val registry = DeltaTables.read(spark, paths.hmacKeyRegistry)
        if (!Set(Columns.KeyId, Columns.Verifier).subsetOf(registry.columns.toSet))
          Chain.one("permanent HMAC continuity registry schema is malformed")
        else {
          val rows = registry.select(Columns.KeyId, Columns.Verifier).limit(1001).collect().toVector
          val sizeBlockers =
            if (rows.size > 1000) Chain.one("permanent HMAC continuity registry exceeds the audited key limit")
            else Chain.empty[String]
          val valid = validRegistryRows(rows.map(row => row.getString(0) -> row.getString(1)))
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
