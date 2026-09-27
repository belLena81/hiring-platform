package com.example.hiring.analytics

import com.example.hiring.analytics.batch.{AnalyticsLakehousePaths, KafkaConnection}
import com.example.hiring.analytics.erasure.KafkaRetentionBarrier
import com.example.hiring.analytics.mongo.MongoAnalyticsLakehouseLock

import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.MongoDatabase
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.nio.file.{Files, LinkOption, Path}
import java.time.Instant
import scala.util.control.NonFatal

/** Local operator workflow. Both steps hold the shared mutex and independently verify Docker writer exclusion. */
private[analytics] final class HmacKeyRetirementCoordinator(
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    database: MongoDatabase,
    kafka: KafkaConnection,
    topic: String,
    writerSettings: LocalHmacKeyWriterExclusion.Settings,
    clock: Clock[IO] = Clock[IO]
) extends LakehouseOperation {
  private val mutex = new MongoAnalyticsLakehouseLock(database)
  private val preparations = new MongoHmacKeyRetirementPreparationStore(database)
  private val authorizations = new MongoHmacKeyRetirementAuthorizationStore(database)
  private val kafkaVolumeName = writerSettings.volumeName.stripSuffix("_hmac-rotation-analytics") +
    "_hmac-rotation-kafka"

  private def observeKafkaLineage: IO[HmacKeyRetirementKafkaLineage] =
    IO.raiseUnless(writerSettings.volumeName.endsWith("_hmac-rotation-analytics"))(
      AnalyticsError.InvalidConfiguration("retirement analytics volume does not identify its isolated Kafka volume")
    ) *> HmacKeyRetirementKafkaLineage.observe(kafka, topic, kafkaVolumeName)

  private def registryVerifier(keyId: String): IO[String] = lakehouseEither {
    if (!DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry))
      Left(
        AnalyticsError.InvalidConfiguration("permanent HMAC continuity registry is missing")
      )
    else {
      val rows = spark.read
        .format("delta")
        .load(paths.hmacKeyRegistry)
        .filter(org.apache.spark.sql.functions.col("keyId") === keyId)
        .select("verifier")
        .limit(2)
        .collect()
      for {
        _ <- Either.cond(
          rows.length == 1 && !rows.head.isNullAt(0),
          (),
          AnalyticsError.InvalidConfiguration("retiring key lacks one original continuity verifier")
        )
        verifier = rows.head.getString(0)
        _ <- Either.cond(
          verifier.matches("[A-Za-z0-9_-]{43}"),
          (),
          AnalyticsError.InvalidConfiguration("retiring key continuity verifier is malformed")
        )
      } yield verifier
    }
  }

  private def verifyPreparedFixture(keyId: String, barrier: KafkaRetentionBarrier): IO[Unit] = lakehouseEither {
    val fixturePath = paths.root.stripSuffix("/") + "/control/hmac_retirement_fixture"
    if (!DeltaTable.isDeltaTable(spark, paths.silver) || !DeltaTable.isDeltaTable(spark, fixturePath))
      Left(AnalyticsError.InvalidConfiguration("old-key retirement fixture is missing"))
    else {
      val oldRows = spark.read
        .format("delta")
        .load(paths.silver)
        .filter(org.apache.spark.sql.functions.col("subjectToken").startsWith(keyId + "_"))
        .limit(1)
        .count()
      val published = spark.read
        .format("delta")
        .load(fixturePath)
        .filter(org.apache.spark.sql.functions.col("stage") === "old-primary-event-published")
        .select("reference")
        .limit(2)
        .collect()
      val proof = published.headOption.map(_.getString(0).split(":", -1).toVector)
      val valid = published.length == 1 && proof.exists {
        case Vector(topicName, partition, end) =>
          topicName == barrier.topic && partition.toIntOption.exists(number =>
            end.toLongOption.exists(value =>
              value > 0L && barrier.partitions.exists(p => p.number == number && p.endOffsetExclusive >= value)
            )
          )
        case _ => false
      }
      for {
        _ <- Either.cond(
          oldRows == 1L,
          (),
          AnalyticsError.InvalidConfiguration("no old-key Silver row exists for the retirement fixture")
        )
        _ <- Either.cond(
          valid,
          (),
          AnalyticsError.InvalidConfiguration("captured Kafka barrier does not cover the fixture event")
        )
      } yield ()
    }
  }

  /** The fixture captures exact old-primary Silver data and log paths before the retention wait. */
  private def verifyCapturedPhysicalPaths(expectedPresent: Boolean): IO[Unit] = lakehouseEither {
    val fixturePath = paths.root.stripSuffix("/") + "/control/hmac_retirement_fixture"
    if (!DeltaTable.isDeltaTable(spark, fixturePath))
      Left(AnalyticsError.InvalidConfiguration("old-key physical path evidence is missing"))
    else {
      val references = spark.read
        .format("delta")
        .load(fixturePath)
        .filter(org.apache.spark.sql.functions.col("stage") === "old-primary-physical-path")
        .select("reference")
        .limit(1001)
        .collect()
        .toVector
        .map(row => if (row.isNullAt(0)) "" else row.getString(0))
      for {
        _ <- Either.cond(
          references.nonEmpty && references.size <= 1000 && references.distinct.size == references.size,
          (),
          AnalyticsError.InvalidConfiguration("old-key physical path evidence is empty, duplicated, or unbounded")
        )
        rootUri = new URI(paths.silver)
        _ <- Either.cond(
          rootUri.getScheme == "file" && rootUri.getAuthority == null,
          (),
          AnalyticsError.InvalidConfiguration("local retirement requires a file-backed Silver table")
        )
        silverRoot = Path.of(rootUri).toAbsolutePath.normalize()
        captured <- references.traverse { reference =>
          val uri = new URI(reference)
          if (uri.getScheme != "file" || uri.getAuthority != null || uri.normalize() != uri)
            Left(AnalyticsError.InvalidConfiguration("old-key physical path evidence is malformed"))
          else {
            val path = Path.of(uri).toAbsolutePath.normalize()
            Either.cond(
              path.startsWith(silverRoot) && path != silverRoot &&
                (path.getFileName.toString.endsWith(".parquet") ||
                  (path.getParent == silverRoot.resolve("_delta_log") && path.getFileName.toString
                    .endsWith(".json"))),
              path,
              AnalyticsError.InvalidConfiguration("old-key physical path evidence is outside Silver")
            )
          }
        }
        _ <- Either.cond(
          captured.exists(_.getFileName.toString.endsWith(".parquet")) &&
            captured.exists(_.getFileName.toString.endsWith(".json")),
          (),
          AnalyticsError.InvalidConfiguration("old-key Silver data and log evidence are both required")
        )
        present = captured.map(Files.exists(_, LinkOption.NOFOLLOW_LINKS))
        _ <- Either.cond(
          !expectedPresent || !present.exists(!_),
          (),
          AnalyticsError.InvalidConfiguration("a captured old-key Silver data or log path was absent at preparation")
        )
        _ <- Either.cond(
          expectedPresent || !present.exists(identity),
          (),
          AnalyticsError.InvalidConfiguration("a captured old-key Silver data or log path is still present")
        )
      } yield ()
    }
  }

  def prepare(keyId: String): IO[HmacKeyRetirementPreparation] = mutex.resource(paths.root).use { _ =>
    for {
      _ <- LocalHmacKeyWriterExclusion.verify(writerSettings, paths.root)
      lakehouseId <- IO.fromEither(HmacKeyRetirementAuthorization.lakehouseId(paths.root))
      verifier <- registryVerifier(keyId)
      lineageBefore <- observeKafkaLineage
      barrier <- KafkaRetentionBarrier.capture(kafka, topic).adaptError {
        case error: AnalyticsError => error
        case NonFatal(error)       =>
          AnalyticsError.InvalidConfiguration(
            s"Kafka retirement barrier capture failed (${error.getClass.getSimpleName})"
          )
      }
      lineageAfter <- observeKafkaLineage
      _ <- IO.raiseUnless(HmacKeyRetirementKafkaLineage.matches(lineageBefore, lineageAfter))(
        AnalyticsError.InvalidConfiguration("Kafka lineage changed during retirement preparation")
      )
      _ <- verifyPreparedFixture(keyId, barrier)
      _ <- verifyCapturedPhysicalPaths(expectedPresent = true)
      at <- clock.realTimeInstant
      preparation = HmacKeyRetirementPreparation(lakehouseId, keyId, verifier, at, barrier, lineageAfter)
      _ <- preparations.insert(paths.root, preparation)
    } yield preparation
  }

  /** Returns the persisted authorization only after actual broker, Delta, Mongo, and Docker observations pass. */
  def authorize(keyId: String): IO[HmacKeyRetirementAuthorization] = mutex.resource(paths.root).use { _ =>
    for {
      _ <- LocalHmacKeyWriterExclusion.verify(writerSettings, paths.root)
      preparation <- preparations
        .read(paths.root, keyId)
        .flatMap(
          _.liftTo[IO](
            AnalyticsError.InvalidConfiguration("HMAC key retirement preparation is missing")
          )
        )
      verifier <- registryVerifier(keyId)
      _ <- IO.raiseUnless(preparation.originalVerifier == verifier)(
        AnalyticsError.InvalidConfiguration("HMAC key continuity verifier changed after retirement preparation")
      )
      lineageBefore <- observeKafkaLineage
      _ <- IO.raiseUnless(HmacKeyRetirementKafkaLineage.matches(preparation.lineage, lineageBefore))(
        AnalyticsError.InvalidConfiguration("Kafka broker, topic, or volume lineage changed since preparation")
      )
      earliest <- HmacKeyRetirementKafkaOffsets.earliest(kafka, preparation.barrier)
      lineageAfter <- observeKafkaLineage
      _ <- IO.raiseUnless(HmacKeyRetirementKafkaLineage.matches(preparation.lineage, lineageAfter))(
        AnalyticsError.InvalidConfiguration("Kafka lineage changed during retirement authorization")
      )
      passed <- IO.fromEither(KafkaRetentionBarrier.hasExpired(preparation.barrier, earliest))
      _ <- IO.raiseUnless(passed)(
        AnalyticsError.InvalidConfiguration("Kafka retention has not passed the captured retirement barrier")
      )
      now <- clock.realTimeInstant
      dataDeadline = preparation.capturedAt.plusSeconds(7L * 86400L)
      finalDeadline = preparation.capturedAt.plusSeconds(30L * 86400L)
      _ <- IO.raiseWhen(now.isBefore(finalDeadline))(
        AnalyticsError.InvalidConfiguration("HMAC key retirement physical-retention horizon has not elapsed")
      )
      writers = AnalyticsKeyRetirement.WriterInventory(
        now,
        "docker-volume:" + writerSettings.volumeName,
        Vector(
          AnalyticsKeyRetirement.WriterRecord(
            writerSettings.oldImageId,
            AnalyticsKeyRetirement.WriterDisposition.AccessRevoked,
            "rw-volume-write-denied:uid=" + writerSettings.oldUid
          ),
          AnalyticsKeyRetirement.WriterRecord(
            writerSettings.newImageId,
            AnalyticsKeyRetirement.WriterDisposition.Stopped,
            "no-running-volume-mounts"
          )
        ),
        Vector.empty
      )
      retention = AnalyticsKeyRetirement.RetentionEvidence(
        AnalyticsKeyRetirement.KafkaRetentionEvidence(
          None,
          None,
          "actual-broker-earliest-offsets-for-every-captured-partition",
          preparation.barrier.partitions.map(partition =>
            (
              partition.number,
              partition.endOffsetExclusive,
              earliest(partition.number)
            )
          )
        ),
        AnalyticsKeyRetirement.RetentionHorizon(Some(dataDeadline), "persisted-retirement-preparation"),
        AnalyticsKeyRetirement.RetentionHorizon(Some(finalDeadline), "persisted-retirement-preparation"),
        AnalyticsKeyRetirement.RetentionHorizon(Some(finalDeadline), "persisted-retirement-preparation")
      )
      audited <- AnalyticsKeyRetirement.auditUnderLock(spark, paths, database, keyId, retention, writers, now)
      _ <- verifyCapturedPhysicalPaths(expectedPresent = false)
      summary <- IO.fromEither(
        audited.leftMap(blockers =>
          AnalyticsError.InvalidConfiguration(
            "HMAC key retirement audit blocked: " + blockers.toNonEmptyList.toList.mkString("; ")
          )
        )
      )
      facts = evidenceFacts(preparation, earliest, writerSettings, summary)
      record = HmacKeyRetirementAuthorization(
        preparation.lakehouseId,
        keyId,
        verifier,
        facts,
        HmacKeyRetirementAuthorization.digest(facts),
        now
      )
      existing <- authorizations.list(paths.root).map(_.find(_.keyId == keyId))
      result <- existing match {
        case Some(value) if value.lakehouseId == record.lakehouseId && value.originalVerifier == verifier =>
          IO.pure(value)
        case Some(_) =>
          IO.raiseError[HmacKeyRetirementAuthorization](
            AnalyticsError.InvalidConfiguration("existing HMAC key retirement authorization conflicts with registry")
          )
        case None =>
          authorizations.insert(paths.root, record) *>
            authorizations
              .list(paths.root)
              .flatMap(
                _.find(_.keyId == keyId)
                  .filter(_ == record)
                  .liftTo[IO](
                    AnalyticsError.InvalidConfiguration("HMAC key retirement authorization readback failed")
                  )
              )
      }
    } yield result
  }

  private def evidenceFacts(
      preparation: HmacKeyRetirementPreparation,
      earliest: Map[Int, Long],
      writers: LocalHmacKeyWriterExclusion.Settings,
      summary: AnalyticsKeyRetirement.AuditSummary
  ): String =
    Vector(
      preparation.lakehouseId,
      preparation.keyId,
      preparation.originalVerifier,
      preparation.capturedAt.toString,
      preparation.barrier.topic,
      preparation.lineage.clusterId,
      preparation.lineage.topicId,
      preparation.lineage.volumeName,
      preparation.lineage.volumeMountpoint,
      preparation.lineage.volumeCreatedAt,
      preparation.lineage.bootstrapEndpoint,
      preparation.barrier.partitions.sortBy(_.number).map(p => s"${p.number}:${p.endOffsetExclusive}").mkString(","),
      earliest.toVector.sortBy(_._1).map((partition, offset) => s"$partition:$offset").mkString(","),
      writers.volumeName,
      writers.oldImageId,
      writers.newImageId,
      summary.checkedAt.toString,
      summary.deltaFilesScanned.toString,
      summary.mongoDocumentsScanned.toString
    ).mkString("\n")
}
