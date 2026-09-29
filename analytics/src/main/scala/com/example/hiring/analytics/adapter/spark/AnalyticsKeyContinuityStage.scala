package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization

import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.time.Instant
import java.sql.Timestamp
import scala.jdk.CollectionConverters.*

/** Verifies the persisted HMAC key anchors and token continuity before reserving a report revision. */
private[analytics] final class AnalyticsKeyContinuityStage[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    execution: SparkExecution[F],
    retirementAuthorizations: KeyRetirementLookup[F],
    private[analytics] val nowOverride: Option[F[Instant]] = None
) {
  private val blocking = execution
  private val now = nowOverride.getOrElse(Async[F].realTimeInstant)

  private def ensurePrimaryTokenCompatibility(spark: SparkSession, at: Instant): F[Unit] = blocking.either {
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

  def validateStoredTokenKeys(spark: SparkSession): F[Unit] = blocking.either {
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

  def validateKeyMaterialContinuity(spark: SparkSession): F[Unit] =
    retirementAuthorizations.list(paths.root).flatMap { authorizations =>
      blocking.either {
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
        val storedTokenKeys = candidateKeys.collect {
          case (keyId, _) if hasStoredTokenForKey(spark, keyId) => keyId
        }.toSet
        val lakehouseId = MongoAnalyticsLakehouseLock.lockId(paths.root)
        lakehouseId
          .flatMap(
            KeyMaterialContinuityDecision
              .evaluate(
                registryExists,
                existingAnalyticsData,
                existingRows,
                pseudonymizer.keyVerifiers,
                storedTokenKeys,
                authorizations,
                _,
                pseudonymizer.primaryKeyId
              )
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
    }

  def validateHmacConfiguration(spark: SparkSession): F[Unit] =
    for {
      _ <- validateKeyMaterialContinuity(spark)
      keyCheckAt <- now
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
private[spark] object KeyMaterialContinuityDecision {
  def evaluate(
      registryExists: Boolean,
      analyticsDataExists: Boolean,
      existingRows: Vector[(String, String)],
      configured: Vector[(String, String)],
      storedTokenKeys: Set[String],
      authorizations: Vector[HmacKeyRetirementAuthorization] = Vector.empty,
      lakehouseId: String = "",
      primaryKeyId: String = ""
  ): Either[AnalyticsError, Vector[(String, String)]] = {
    if (!registryExists && analyticsDataExists)
      Left(
        AnalyticsError.InvalidConfiguration(
          "existing lakehouse has no HMAC key continuity registry; startup fails closed, reset or rebuild this local lakehouse explicitly before reuse"
        )
      )
    else if (!registryExists && authorizations.nonEmpty)
      Left(AnalyticsError.InvalidConfiguration("HMAC key retirement authorization has no continuity registry"))
    else if (!registryExists) Right(configured)
    else {
      val rowsAreValid = existingRows.forall { case (keyId, verifier) =>
        Option(keyId).exists(_.matches("[A-Za-z0-9-]{1,40}")) &&
        Option(verifier).exists(_.matches("[A-Za-z0-9_-]{43}"))
      }
      val existing = existingRows.toMap
      val authorizationsValid = authorizations.forall(record =>
        HmacKeyRetirementAuthorization.validate(record).isRight && record.lakehouseId == lakehouseId
      ) && authorizations.map(_.keyId).distinct.size == authorizations.size
      val authorizationByKey = authorizations.map(record => record.keyId -> record).toMap
      val removedKey = existing.find { case (keyId, verifier) =>
        !configured.exists(_._1 == keyId) &&
        !authorizationByKey.get(keyId).exists(record => record.originalVerifier == verifier)
      }
      val mismatched = configured.find { case (keyId, verifier) => existing.get(keyId).exists(_ != verifier) }
      val added = configured.filterNot { case (keyId, _) => existing.contains(keyId) }
      val unanchoredStoredKey = added.find { case (keyId, _) => storedTokenKeys.contains(keyId) }
      for {
        _ <- Either.cond(
          rowsAreValid && existingRows.map(_._1).distinct.size == existingRows.size,
          (),
          AnalyticsError.InvalidConfiguration("HMAC key continuity registry is malformed")
        )
        _ <- Either.cond(
          authorizationsValid && !authorizationByKey.contains(primaryKeyId),
          (),
          AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is invalid or its key is primary")
        )
        _ <- removedKey.fold[Either[AnalyticsError, Unit]](Right(()))(entry =>
          Left(
            AnalyticsError.InvalidConfiguration(
              s"HMAC key '${entry._1}' cannot be removed without durable cleanup and writer-exclusion authorization"
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
