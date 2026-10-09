package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization

import cats.effect.{Async, Clock}
import cats.syntax.all.*
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
    clock: Clock[F]
) {
  private val blocking = execution
  private val now = clock.realTimeInstant

  private[spark] def ensurePrimaryTokenCompatibility(spark: SparkSession, at: Instant): F[Unit] = blocking.either {
    val failureColumn = "_hiringPrimaryTokenFailure"
    // Priorities retain the original Silver-before-late-facts error order when several tables are invalid.
    val failures = Vector(paths.silver, paths.lateFacts).zipWithIndex.flatMap { case (path, index) =>
      if (!DeltaTables.exists(spark, path)) Vector.empty
      else {
        val stored = DeltaTables.read(spark, path)
        val columns = stored.columns.toSet
        if (!columns.contains(Columns.SubjectToken))
          Vector(stored.limit(1).select(lit(index * 2).as(failureColumn)))
        else {
          val activeData =
            if (columns.contains(Columns.ExpiresAt))
              col(Columns.ExpiresAt).isNull || col(Columns.ExpiresAt) > lit(Timestamp.from(at))
            else lit(true)
          val expectedPrefix = pseudonymizer.primaryKeyId + "_"
          Vector(
            stored
              .filter(
                activeData && (col(Columns.SubjectToken).isNull || !col(Columns.SubjectToken)
                  .startsWith(expectedPrefix))
              )
              .limit(1)
              .select(lit(index * 2 + 1).as(failureColumn))
          )
        }
      }
    }
    val firstFailure = failures.reduceOption(_.unionByName(_)).flatMap { frame =>
      Option(frame.agg(min(col(failureColumn))).head().getAs[java.lang.Integer](0)).map(_.intValue())
    }
    firstFailure match {
      case None                                => Right(())
      case Some(priority) if priority % 2 == 0 =>
        Left(AnalyticsError.InvalidConfiguration("retained analytical data has no versioned subject tokens"))
      case Some(_) =>
        Left(
          AnalyticsError.InvalidConfiguration(
            "unexpired analytical rows use a different HMAC key; retain the old primary until their retention expires"
          )
        )
    }
  }

  def validateStoredTokenKeys(spark: SparkSession): F[Unit] = blocking.either {
    val allowedKeyIds = pseudonymizer.keyIds.toVector.sorted.mkString("(?:", "|", ")")
    val tokenPattern = s"^${allowedKeyIds}_[A-Za-z0-9_-]{43}$$"
    val invalidTokens = Vector(paths.bronze, paths.quarantine, paths.silver, paths.lateFacts)
      .flatMap { path =>
        if (DeltaTables.exists(spark, path)) {
          val frame = DeltaTables.read(spark, path)
          val tokenFrames = Vector(
            Option.when(frame.columns.contains(Columns.SubjectTokens))(
              frame.select(explode(col(Columns.SubjectTokens)).as(Columns.Token))
            ),
            Option.when(frame.columns.contains(Columns.SubjectToken))(
              frame.select(col(Columns.SubjectToken).as(Columns.Token))
            )
          ).flatten
          val tokenValues = tokenFrames
            .reduceOption(_.unionByName(_))
            .getOrElse(
              frame.limit(0).select(lit(null).cast(StringType).as(Columns.Token))
            )
          Vector(tokenValues.filter(col(Columns.Token).isNotNull && !col(Columns.Token).rlike(tokenPattern)).limit(1))
        } else Vector.empty
      }
    val incompatible = invalidTokens.reduceOption(_.unionByName(_)).exists(_.limit(1).count() > 0L)
    Either.cond(
      !incompatible,
      (),
      AnalyticsError.InvalidConfiguration(
        "stored analytical rows require an HMAC key that is not configured"
      )
    )
  }

  def validateKeyMaterialContinuity(spark: SparkSession): F[Unit] =
    retirementAuthorizations.list(paths.root).flatMap { authorizations =>
      blocking.either {
        val registryExists = DeltaTables.exists(spark, paths.hmacKeyRegistry)
        val existingAnalyticsData = Vector(
          paths.bronze,
          paths.quarantine,
          paths.silver,
          paths.lateFacts,
          paths.funnelGold,
          paths.timeToHireGold,
          paths.skillsGold,
          paths.manifests
        ).exists(path => DeltaTables.exists(spark, path))
        val existingRows =
          if (registryExists)
            DeltaTables
              .read(spark, paths.hmacKeyRegistry)
              .select(Columns.KeyId, Columns.Verifier)
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
                .save(SparkPhysicalLocation.resolve(paths.hmacKeyRegistry))
            } else if (added.nonEmpty)
              createVerifierFrame(spark, added).write
                .format("delta")
                .mode("append")
                .save(SparkPhysicalLocation.resolve(paths.hmacKeyRegistry))
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
    Vector(paths.bronze, paths.quarantine, paths.silver, paths.lateFacts).exists { path =>
      DeltaTables.readIfExists(spark, path).exists { frame =>
        val tokenFrames = Vector(
          Option.when(frame.columns.contains(Columns.SubjectTokens))(
            frame.select(explode(col(Columns.SubjectTokens)).as(Columns.Token))
          ),
          Option.when(frame.columns.contains(Columns.SubjectToken))(
            frame.select(col(Columns.SubjectToken).as(Columns.Token))
          )
        ).flatten
        tokenFrames
          .reduceOption(_.unionByName(_))
          .exists(_.filter(col(Columns.Token).startsWith(keyId + "_")).limit(1).count() > 0L)
      }
    }

  private def createVerifierFrame(spark: SparkSession, values: Vector[(String, String)]): DataFrame =
    spark.createDataFrame(
      values.map { case (keyId, verifier) => Row(keyId, verifier) }.asJava,
      StructType(
        Seq(
          StructField(Columns.KeyId, StringType, nullable = false),
          StructField(Columns.Verifier, StringType, nullable = false)
        )
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
