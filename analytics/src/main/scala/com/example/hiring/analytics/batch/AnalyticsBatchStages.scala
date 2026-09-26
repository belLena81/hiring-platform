package com.example.hiring.analytics.batch

import com.example.hiring.analytics.*

import cats.effect.IO
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.time.Instant
import java.sql.Timestamp
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Reads a bounded operational range and commits its replayable Bronze representation. */
private[batch] final class AnalyticsBatchIngestionStage(runtime: AnalyticsBatchStageRuntime) {
  import runtime.*

  def ingest(
      spark: SparkSession,
      source: BoundedOperationalEventSource,
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame
  ): IO[AnalyticsBronzeInput] =
    for {
      raw <- source.read(spark, manifest)
      rawSchema <- blocking(raw.schema)
      _ <- KafkaRecordColumns.validate(rawSchema)
      _ <- source.verifyOffsets(raw, manifest)
      _ <- validateRunIdentity(spark, manifest)
      parsed <- blocking(OperationalEventTransforms.parseKafkaRecords(raw))
      valid <- blocking(OperationalEventTransforms.validEvents(parsed))
      pseudonymized <- blocking(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safeToPersist <- blocking(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(pseudonymized, markerTokens))
      startedAt <- currentTime()
      incoming <- blocking(
        withExpiry(OperationalEventTransforms.bronze(safeToPersist), startedAt, AnalyticsRetention.BronzeDays)
      )
      recordCount <- blocking(raw.count())
      _ <- persistManifest(spark, manifest, "STARTED", startedAt.toString)
      _ <- merge(
        incoming,
        paths.bronze,
        "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
      )
    } yield AnalyticsBronzeInput(parsed, startedAt, recordCount)

  private def validateRunIdentity(spark: SparkSession, manifest: AnalyticsRunManifest): IO[Unit] = blocking.either {
    if (DeltaTable.isDeltaTable(spark, paths.manifests)) {
      val existing = spark.read
        .format("delta")
        .load(paths.manifests)
        .filter(col("runId") === lit(manifest.runId.value))
        .select("topic", "partition", "startOffset", "endOffsetExclusive")
        .distinct()
        .collect()
        .toVector
        .map(row => (row.getString(0), row.getInt(1), row.getLong(2), row.getLong(3)))
        .toSet
      val expected = manifest.offsetRanges
        .map(range => (range.topic, range.partition, range.startOffset, range.endOffsetExclusive))
        .toSet
      Either.cond(existing.isEmpty || existing == expected, (), AnalyticsError.RunIdRangeConflict(manifest.runId.value))
    } else Right(())
  }
}

/** Validates incoming events, quarantines malformed/conflicting rows, and merges Silver facts. */
private[batch] final class AnalyticsBatchSilverStage(runtime: AnalyticsBatchStageRuntime) {
  import runtime.*

  def separateQuarantine(
      spark: SparkSession,
      bronze: AnalyticsBronzeInput,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean
  ): IO[AnalyticsPreparedEvents] = {
    val parsed = bronze.frame
    for {
      valid <- blocking(OperationalEventTransforms.validEvents(parsed))
      pseudonymizedValid <- blocking(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safeValid <- blocking(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(pseudonymizedValid, markerTokens))
      malformed <- blocking(
        withExpiry(
          AnalyticsSubjectPrivacy
            .withSubjectToken(OperationalEventTransforms.malformedEvents(parsed), pseudonymizer)
            .withColumn("quarantineId", quarantineId())
            .withColumn("quarantineReason", lit("INVALID_OPERATIONAL_EVENT_ENVELOPE")),
          bronze.startedAt,
          AnalyticsRetention.QuarantineDays
        )
      )
      malformedCount <- blocking(malformed.count())
      malformedToPersist <- if (activeMarkersPresent) blocking(malformed.limit(0)) else IO.pure(malformed)
      validRecords <- blocking(valid.count())
      safeValidRecords <- blocking(safeValid.count())
      suppressedRecords = validRecords - safeValidRecords
      newConflicts <- blocking(OperationalEventTransforms.conflictingEventIds(safeValid))
      incomingSilver <- blocking(OperationalEventTransforms.silver(safeValid, pseudonymizer, markerTokens))
      incomingSilverSchema <- blocking(incomingSilver.schema)
      storedSilver <- readOrEmpty(spark, paths.silver, incomingSilverSchema)
      historicalConflicts <- blocking(
        safeValid
          .select("eventId", "rawValue")
          .withColumn("incomingFingerprint", sha2(col("rawValue"), 256))
          .join(
            storedSilver
              .select("eventId", "eventFingerprint")
              .withColumnRenamed("eventFingerprint", "storedFingerprint"),
            Seq("eventId"),
            "inner"
          )
          .filter(col("incomingFingerprint") =!= col("storedFingerprint"))
          .select("eventId")
          .distinct()
      )
      conflicts <- blocking(newConflicts.unionByName(historicalConflicts).distinct())
      conflictingEventIds <- blocking(conflicts.count())
      conflictQuarantine <- blocking(
        withExpiry(
          AnalyticsSubjectPrivacy
            .withSubjectToken(safeValid.join(conflicts, Seq("eventId"), "inner"), pseudonymizer)
            .withColumn("quarantineId", quarantineId())
            .withColumn("quarantineReason", lit("CONFLICTING_EVENT_ID")),
          bronze.startedAt,
          AnalyticsRetention.QuarantineDays
        )
      )
      conflictingRecords <- blocking(conflictQuarantine.count())
      quarantine <- blocking(
        malformedToPersist
          .unionByName(conflictQuarantine)
          .withColumn("payloadHash", sha2(col("rawValue"), 256))
          .drop("rawValue", "actorId", "payload", "subjectToken")
          .select(
            "topic",
            "partition",
            "offset",
            "payloadHash",
            "subjectTokens",
            "quarantineId",
            "quarantineReason",
            "expiresAt"
          )
      )
      _ <- merge(quarantine, paths.quarantine, "target.quarantineId = source.quarantineId")
    } yield AnalyticsPreparedEvents(
      incomingSilver,
      conflicts,
      validRecords,
      suppressedRecords,
      malformedCount + conflictingRecords,
      conflictingEventIds
    )
  }

  def mergeSilver(prepared: AnalyticsPreparedEvents, startedAt: Instant): IO[DataFrame] =
    for {
      silver <- blocking(
        withExpiry(
          prepared.incomingSilver.join(prepared.conflicts, Seq("eventId"), "left_anti"),
          startedAt,
          AnalyticsRetention.SilverDays
        )
      )
      _ <- merge(silver, paths.silver, "target.eventId = source.eventId")
    } yield silver
}

/** Verifies the persisted HMAC key anchors and token continuity before reserving a report revision. */
private[batch] final class AnalyticsKeyContinuityStage(runtime: AnalyticsBatchStageRuntime) {
  import runtime.*

  private def ensurePrimaryTokenCompatibility(spark: SparkSession, at: Instant): IO[Unit] = blocking.either {
    if (!DeltaTable.isDeltaTable(spark, paths.silver)) Right(())
    else {
      val stored = spark.read.format("delta").load(paths.silver)
      val columns = stored.columns.toSet
      if (stored.limit(1).count() > 0L && !columns.contains("subjectToken"))
        Left(AnalyticsError.InvalidConfiguration("Silver data has no versioned subject tokens"))
      else if (columns.contains("subjectToken")) {
        val activeData =
          if (columns.contains("expiresAt")) col("expiresAt").isNull || col("expiresAt") > lit(Timestamp.from(at))
          else lit(true)
        val expectedPrefix = pseudonymizer.primaryKeyId + "_"
        val incompatible =
          stored
            .filter(activeData && (col("subjectToken").isNull || !col("subjectToken").startsWith(expectedPrefix)))
            .limit(1)
            .count()
        Either.cond(
          incompatible == 0L,
          (),
          AnalyticsError.InvalidConfiguration(
            "unexpired Silver rows use a different HMAC key; retain the old primary until Silver retention expires"
          )
        )
      } else Right(())
    }
  }

  def validateStoredTokenKeys(spark: SparkSession): IO[Unit] = blocking.either {
    val allowedKeyIds = pseudonymizer.keyIds.toVector.sorted.mkString("(?:", "|", ")")
    val tokenPattern = s"^${allowedKeyIds}_[A-Za-z0-9_-]{43}$$"
    Vector(paths.bronze, paths.quarantine, paths.silver).foldLeft[Either[AnalyticsError, Unit]](Right(())) {
      (result, path) =>
        result.flatMap { _ =>
          if (DeltaTable.isDeltaTable(spark, path)) {
            val frame = spark.read.format("delta").load(path)
            val tokenFrames = Vector(
              Option.when(frame.columns.contains("subjectTokens"))(
                frame.select(explode(col("subjectTokens")).as("token"))
              ),
              Option.when(frame.columns.contains("subjectToken"))(frame.select(col("subjectToken").as("token")))
            ).flatten
            val tokenValues = tokenFrames
              .reduceOption(_.unionByName(_))
              .getOrElse(
                frame.limit(0).select(lit(null).cast(StringType).as("token"))
              )
            if (tokenValues.filter(col("token").isNotNull && !col("token").rlike(tokenPattern)).limit(1).count() > 0L)
              Left(
                AnalyticsError.InvalidConfiguration("stored analytical rows require an HMAC key that is not configured")
              )
            else Right(())
          } else Right(())
        }
    }
  }

  def validateKeyMaterialContinuity(spark: SparkSession): IO[Unit] = blocking.either {
    val registryExists = DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry)
    val existingAnalyticsData = Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold,
      paths.manifests
    ).exists(DeltaTable.isDeltaTable(spark, _))
    val existingRows =
      if (registryExists)
        spark.read
          .format("delta")
          .load(paths.hmacKeyRegistry)
          .select("keyId", "verifier")
          .collect()
          .toVector
          .map(row => row.getString(0) -> row.getString(1))
      else Vector.empty
    val candidateKeys =
      if (registryExists)
        pseudonymizer.keyVerifiers.filterNot { case (keyId, _) => existingRows.exists(_._1 == keyId) }
      else Vector.empty
    val storedTokenKeys = candidateKeys.collect { case (keyId, _) if hasStoredTokenForKey(spark, keyId) => keyId }.toSet
    KeyMaterialContinuityDecision
      .evaluate(
        registryExists,
        existingAnalyticsData,
        existingRows,
        pseudonymizer.keyVerifiers,
        storedTokenKeys
      )
      .map { added =>
        if (!registryExists) {
          createVerifierFrame(spark, pseudonymizer.keyVerifiers).write
            .format("delta")
            .mode("errorifexists")
            .save(paths.hmacKeyRegistry)
        } else if (added.nonEmpty)
          createVerifierFrame(spark, added).write.format("delta").mode("append").save(paths.hmacKeyRegistry)
      }
  }

  def validateHmacConfiguration(spark: SparkSession): IO[Unit] =
    for {
      _ <- validateKeyMaterialContinuity(spark)
      keyCheckAt <- currentTime()
      _ <- validateStoredTokenKeys(spark)
      _ <- ensurePrimaryTokenCompatibility(spark, keyCheckAt)
    } yield ()

  private def hasStoredTokenForKey(spark: SparkSession, keyId: String): Boolean =
    Vector(paths.bronze, paths.quarantine, paths.silver).exists { path =>
      if (!DeltaTable.isDeltaTable(spark, path)) false
      else {
        val frame = spark.read.format("delta").load(path)
        val tokenFrames = Vector(
          Option.when(frame.columns.contains("subjectTokens"))(frame.select(explode(col("subjectTokens")).as("token"))),
          Option.when(frame.columns.contains("subjectToken"))(frame.select(col("subjectToken").as("token")))
        ).flatten
        tokenFrames
          .reduceOption(_.unionByName(_))
          .exists(_.filter(col("token").startsWith(keyId + "_")).limit(1).count() > 0L)
      }
    }

  private def createVerifierFrame(spark: SparkSession, values: Vector[(String, String)]): DataFrame =
    spark.createDataFrame(
      values.map { case (keyId, verifier) => Row(keyId, verifier) }.asJava,
      StructType(
        Seq(StructField("keyId", StringType, nullable = false), StructField("verifier", StringType, nullable = false))
      )
    )
}

/** Pure decision logic for fetched continuity-registry rows and stored-token observations. */
private[batch] object KeyMaterialContinuityDecision {
  def evaluate(
      registryExists: Boolean,
      analyticsDataExists: Boolean,
      existingRows: Vector[(String, String)],
      configured: Vector[(String, String)],
      storedTokenKeys: Set[String]
  ): Either[AnalyticsError, Vector[(String, String)]] = {
    if (!registryExists && analyticsDataExists)
      Left(
        AnalyticsError.InvalidConfiguration(
          "existing lakehouse has no HMAC key continuity registry; startup fails closed, reset or rebuild this local lakehouse explicitly before reuse"
        )
      )
    else if (!registryExists) Right(configured)
    else {
      val rowsAreValid = existingRows.forall { case (keyId, verifier) =>
        keyId != null && keyId.matches("[A-Za-z0-9-]{1,40}") && verifier != null && verifier.matches(
          "[A-Za-z0-9_-]{43}"
        )
      }
      val existing = existingRows.toMap
      val removedKey = existing.keys.find(keyId => !configured.exists(_._1 == keyId))
      val mismatched = configured.find { case (keyId, verifier) => existing.get(keyId).exists(_ != verifier) }
      val added = configured.filterNot { case (keyId, _) => existing.contains(keyId) }
      val unanchoredStoredKey = added.find { case (keyId, _) => storedTokenKeys.contains(keyId) }
      for {
        _ <- Either.cond(
          rowsAreValid && existingRows.map(_._1).distinct.size == existingRows.size,
          (),
          AnalyticsError.InvalidConfiguration("HMAC key continuity registry is malformed")
        )
        _ <- removedKey.fold[Either[AnalyticsError, Unit]](Right(()))(keyId =>
          Left(
            AnalyticsError.InvalidConfiguration(
              s"HMAC key '$keyId' cannot be removed: audited historical-data cleanup and writer-exclusion verification are not implemented"
            )
          )
        )
        _ <- mismatched.fold[Either[AnalyticsError, Unit]](Right(()))(entry =>
          Left(AnalyticsError.InvalidConfiguration(s"HMAC key material changed without a new key ID: ${entry._1}"))
        )
        _ <- unanchoredStoredKey.fold[Either[AnalyticsError, Unit]](Right(()))(keyId =>
          Left(
            AnalyticsError.InvalidConfiguration(
              s"stored rows use HMAC key ID '$keyId' without a continuity anchor; verify provenance before registering it"
            )
          )
        )
      } yield added
    }
  }
}

/** Owns erasure matching, Delta evidence capture, checkpointing, and physical-presence verification. */
private[batch] final class AnalyticsBatchErasureStage(runtime: AnalyticsBatchStageRuntime) {
  import runtime.*
  private val MaximumErasureEvidenceFiles = runtime.maximumErasureEvidenceFiles

  def verifyMarkedSubjectsAbsent(spark: SparkSession, markerTokens: DataFrame): IO[Unit] = blocking.either {
    val marker = BatchSubjectMatching.markerRows(markerTokens)
    Vector(paths.bronze, paths.quarantine, paths.silver).foldLeft[Either[AnalyticsError, Unit]](Right(())) {
      (result, path) =>
        result.flatMap { _ =>
          if (DeltaTable.isDeltaTable(spark, path)) {
            val matched = BatchSubjectMatching.matchedBySubject(spark.read.format("delta").load(path), marker)
            Either.cond(
              matched.limit(1).count() == 0L,
              (),
              AnalyticsError.LakehouseFailure(
                new IllegalStateException(s"marked subject remains in Delta dataset $path")
              )
            )
          } else Right(())
        }
    }
  }

  def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): IO[Long] = blocking {
    val marker = BatchSubjectMatching.markerRows(markerTokens)
    Vector(paths.bronze, paths.quarantine, paths.silver).foldLeft(0L) { (total, path) =>
      if (!DeltaTable.isDeltaTable(spark, path)) total
      else total + BatchSubjectMatching.matchedBySubject(spark.read.format("delta").load(path), marker).count()
    }
  }

  def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): IO[Vector[String]] =
    configureRawTablePrivacy(spark) *> blocking.either {
      val marker = BatchSubjectMatching.markerRows(markerTokens)
      val rawPaths = Set(paths.bronze, paths.quarantine)
      val files = Vector(paths.bronze, paths.quarantine, paths.silver).flatMap { path =>
        if (!DeltaTable.isDeltaTable(spark, path)) Vector.empty
        else {
          val frame = spark.read.format("delta").load(path)
          val columns = frame.columns.toSet
          val attributed = BatchSubjectMatching.matchedBySubject(frame, marker)
          val affected =
            if (rawPaths.contains(path) && !columns.contains("subjectTokens")) frame
            else if (rawPaths.contains(path) && columns.contains("subjectTokens")) {
              val unattributed = frame.filter(col("subjectTokens").isNull || size(col("subjectTokens")) === 0)
              attributed.unionByName(unattributed, allowMissingColumns = true)
            } else attributed
          val dataFiles = affected
            .select(input_file_name().as("filePath"))
            .distinct()
            .limit(MaximumErasureEvidenceFiles + 1)
            .collect()
            .toVector
            .map(_.getString(0))
            .distinct
          dataFiles ++ (if (rawPaths.contains(path)) rawLogFiles(spark, path, None) else Vector.empty)
        }
      }.distinct
      Either.cond(
        files.size <= MaximumErasureEvidenceFiles,
        files,
        AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
      )
    }

  def checkpointPurgedRawLogs(spark: SparkSession): IO[Vector[String]] = blocking.either {
    val retiredLogs = Vector(paths.bronze, paths.quarantine).flatMap { path =>
      if (!DeltaTable.isDeltaTable(spark, path)) Vector.empty
      else {
        val log = DeltaLog.forTable(spark, path)
        val tableIdentifier = path.replace("`", "``")
        spark.sql(
          s"ALTER TABLE delta.`$tableIdentifier` SET TBLPROPERTIES ('analytics.erasureCheckpointNonce' = '${java.util.UUID.randomUUID()}')"
        )
        val snapshot = log.update()
        val oldLogs = rawLogFiles(spark, path, Some(snapshot.version))
        log.checkpointAndCleanUpDeltaLog(snapshot, None)
        oldLogs
      }
    }.distinct
    Either.cond(
      retiredLogs.size <= MaximumErasureEvidenceFiles,
      retiredLogs,
      AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
    )
  }

  def verifyFilesAbsent(spark: SparkSession, files: Vector[String]): IO[Unit] = blocking.either {
    val configuration = spark.sparkContext.hadoopConfiguration
    val remaining = files.filter { value =>
      val path = new org.apache.hadoop.fs.Path(value)
      path.getFileSystem(configuration).exists(path)
    }
    Either.cond(remaining.isEmpty, (), AnalyticsError.PhysicalReclamationUnverified)
  }

  def checkpointRawTableLogs(spark: SparkSession): IO[Unit] = blocking {
    Vector(paths.bronze, paths.quarantine).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) {
        val log = DeltaLog.forTable(spark, path)
        log.checkpointAndCleanUpDeltaLog(log.update(), None)
      }
    }
  }

  def purgeMarkedSubjectRows(spark: SparkSession, path: String, markerTokens: DataFrame): IO[Unit] = blocking {
    if (DeltaTable.isDeltaTable(spark, path)) {
      val markedSubjects = markerTokens.select(col("subjectToken")).filter(col("subjectToken").isNotNull).distinct()
      val columns = spark.read.format("delta").load(path).columns.toSet
      val rawScope = path == paths.bronze || path == paths.quarantine
      if (rawScope) {
        val table = DeltaTable.forPath(spark, path)
        if (columns.contains("subjectTokens"))
          table.delete(col("subjectTokens").isNull || size(col("subjectTokens")) === 0)
        else table.delete()
      }
      val condition =
        if (columns.contains("subjectTokens"))
          "array_contains(target.subjectTokens, source.subjectToken)" +
            (if (columns.contains("subjectToken")) " OR target.subjectToken = source.subjectToken" else "")
        else if (columns.contains("subjectToken")) "target.subjectToken = source.subjectToken"
        else {
          DeltaTable.forPath(spark, path).delete()
          ""
        }
      if (condition.nonEmpty)
        DeltaTable
          .forPath(spark, path)
          .as("target")
          .merge(markedSubjects.as("source"), condition)
          .whenMatched()
          .delete()
          .execute()
    }
  }

  private def rawLogFiles(spark: SparkSession, tablePath: String, beforeVersion: Option[Long]): Vector[String] = {
    val logDirectory = new org.apache.hadoop.fs.Path(s"$tablePath/_delta_log")
    val fileSystem = logDirectory.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(logDirectory)) Vector.empty
    else
      Iterator
        .unfold(fileSystem.listFiles(logDirectory, false)) { entries =>
          if (entries.hasNext) Some(entries.next() -> entries) else None
        }
        .filter { status =>
          val prefix = status.getPath.getName.take(20)
          Try(prefix.toLong).exists(version => beforeVersion.forall(version < _))
        }
        .map(_.getPath.toString)
        .take(MaximumErasureEvidenceFiles + 1)
        .toVector
  }
}
