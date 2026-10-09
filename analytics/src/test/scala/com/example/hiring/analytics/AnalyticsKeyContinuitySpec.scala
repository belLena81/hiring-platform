package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.AnalyticsTestClocks

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.AnalyticsTestSubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
import munit.CatsEffectSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.util.Comparator
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class AnalyticsKeyContinuitySpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes
  private val at = Instant.parse("2026-10-02T10:00:00Z")
  private val keys = AnalyticsTestSubjectPseudonymizer.fromKeyRing(
    "current-key",
    Array.fill[Byte](32)(1),
    Vector("previous-key" -> Array.fill[Byte](32)(2))
  )
  private val current = AnalyticsTestSubjectPseudonymizer.tokenValue(keys, "current-subject")
  private val previous = "previous-key_" + "a" * 43
  private val unknown = "unknown-key_" + "b" * 43
  private val unknownKeyError = AnalyticsError.InvalidConfiguration(
    "stored analytical rows require an HMAC key that is not configured"
  )
  private val primaryError = AnalyticsError.InvalidConfiguration(
    "unexpired analytical rows use a different HMAC key; retain the old primary until their retention expires"
  )
  private val missingTokenError = AnalyticsError.InvalidConfiguration(
    "retained analytical data has no versioned subject tokens"
  )
  private val lookup = new KeyRetirementLookup[IO] {
    override def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]] = IO.pure(Vector.empty)
  }

  private def withLakehouse(
      check: (SparkSession, SparkExecution[IO], AnalyticsLakehousePaths) => IO[Unit]
  ): IO[Unit] = {
    val root = Resource.make(IO.blocking(Files.createTempDirectory("hiring-key-continuity-")))(path =>
      IO.blocking {
        val entries = Files.walk(path)
        try
          entries.sorted(Comparator.reverseOrder()).forEach { entry =>
            val _ = Files.deleteIfExists(entry)
          }
        finally entries.close()
      }
    )
    root.use { path =>
      SparkBlockingExecution.resource[IO].use { execution =>
        Resource
          .make(execution {
            SparkSession
              .builder()
              .master("local[2]")
              .appName("AnalyticsKeyContinuitySpec")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.databricks.delta.snapshotPartitions", "2")
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .getOrCreate()
          })(spark => execution.blocking(spark.stop()))
          .use { spark =>
            for {
              _ <- execution.attachSparkContext(spark.sparkContext)
              paths <- IO.fromEither(
                AnalyticsLakehousePaths
                  .from(path.toUri.toString)
                  .toEither
                  .leftMap(errors => new IllegalArgumentException(errors.toString))
              )
              _ <- check(spark, execution, paths)
            } yield ()
          }
      }
    }
  }

  private def child(root: AnalyticsLakehousePaths, name: String): IO[AnalyticsLakehousePaths] =
    IO.fromEither(
      AnalyticsLakehousePaths
        .from(root.root + "/" + name)
        .toEither
        .leftMap(errors => new IllegalArgumentException(errors.toString))
    )

  private def stage(paths: AnalyticsLakehousePaths, execution: SparkExecution[IO]): AnalyticsKeyContinuityStage[IO] =
    new AnalyticsKeyContinuityStage(paths, keys, execution, lookup, AnalyticsTestClocks.fixed(at))

  private val tokenShape = StructType(
    Vector(
      StructField(Columns.SubjectToken, StringType, nullable = true),
      StructField(Columns.SubjectTokens, ArrayType(StringType, containsNull = true), nullable = true),
      StructField(Columns.ExpiresAt, TimestampType, nullable = true)
    )
  )

  private def tokens(
      spark: SparkSession,
      token: Option[String],
      associated: Vector[String],
      expiry: Option[Instant]
  ): DataFrame = spark.createDataFrame(
    Vector(Row(token.orNull, associated, expiry.map(Timestamp.from).orNull)).asJava,
    tokenShape
  )

  private def write(frame: DataFrame, path: String, execution: SparkExecution[IO]): IO[Unit] =
    execution(
      frame.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SparkPhysicalLocation.resolve(path))
    )

  test("unknown associated token keys are rejected in every retained raw and derived table") {
    withLakehouse { (spark, execution, root) =>
      Vector("bronze", "quarantine", "silver", "late").traverse_ { name =>
        for {
          paths <- child(root, name)
          path = name match {
            case "bronze"     => paths.bronze
            case "quarantine" => paths.quarantine
            case "silver"     => paths.silver
            case _            => paths.lateFacts
          }
          frame <- execution(tokens(spark, Some(current), Vector(current, unknown), Some(at.minusSeconds(1))))
          _ <- write(frame, path, execution)
          result <- stage(paths, execution).validateStoredTokenKeys(spark).attempt
          _ <- IO(assertEquals(result, Left(unknownKeyError)))
        } yield ()
      }
    }
  }

  test("configured previous tokens remain allowed and malformed known-key tokens still fail closed") {
    withLakehouse { (spark, execution, paths) =>
      for {
        valid <- execution(tokens(spark, Some(current), Vector(current, previous), None))
        _ <- write(valid, paths.bronze, execution)
        _ <- stage(paths, execution).validateStoredTokenKeys(spark)
        invalid <- execution(tokens(spark, Some("current-key_short"), Vector(current), None))
        _ <- write(invalid, paths.bronze, execution)
        result <- stage(paths, execution).validateStoredTokenKeys(spark).attempt
        _ <- IO(assertEquals(result, Left(unknownKeyError)))
      } yield ()
    }
  }

  test("primary compatibility retains expiry equality, absent expiry and null token semantics") {
    withLakehouse { (spark, execution, root) =>
      val cases = Vector(
        ("expired", Some(previous), Some(at.minusSeconds(1)), false),
        ("expiry-equality", Some(previous), Some(at), false),
        ("unexpired", Some(previous), Some(at.plusSeconds(1)), true),
        ("null-expiry", Some(previous), None, true),
        ("null-token", None, Some(at.plusSeconds(1)), true)
      )
      cases.traverse_ { case (name, token, expiry, rejected) =>
        for {
          paths <- child(root, name)
          frame <- execution(tokens(spark, token, Vector(current), expiry))
          _ <- write(frame, paths.silver, execution)
          result <- stage(paths, execution).ensurePrimaryTokenCompatibility(spark, at).attempt
          _ <- IO(assertEquals(result, if (rejected) Left(primaryError) else Right(())))
        } yield ()
      } *> (for {
        paths <- child(root, "absent-expiry-column")
        frame <- execution(tokens(spark, Some(previous), Vector(previous), None).drop(Columns.ExpiresAt))
        _ <- write(frame, paths.lateFacts, execution)
        result <- stage(paths, execution).ensurePrimaryTokenCompatibility(spark, at).attempt
        _ <- IO(assertEquals(result, Left(primaryError)))
      } yield ())
    }
  }

  private def legacy(spark: SparkSession, nonempty: Boolean): DataFrame = spark.createDataFrame(
    (if (nonempty) Vector(Row("legacy-value")) else Vector.empty[Row]).asJava,
    StructType(Vector(StructField("legacyValue", StringType, nullable = false)))
  )

  test("missing subject token columns are accepted only for empty retained tables") {
    withLakehouse { (spark, execution, root) =>
      Vector(false, true).traverse_ { nonempty =>
        for {
          paths <- child(root, if (nonempty) "nonempty-legacy" else "empty-legacy")
          frame <- execution(legacy(spark, nonempty))
          _ <- write(frame, paths.silver, execution)
          _ <- write(frame, paths.lateFacts, execution)
          result <- stage(paths, execution).ensurePrimaryTokenCompatibility(spark, at).attempt
          _ <- IO(assertEquals(result, if (nonempty) Left(missingTokenError) else Right(())))
        } yield ()
      }
    }
  }

  test("mixed primary failures preserve the first Silver-before-late-facts error") {
    withLakehouse { (spark, execution, root) =>
      Vector(false, true).traverse_ { silverMissing =>
        for {
          paths <- child(root, if (silverMissing) "silver-missing" else "silver-previous")
          missing <- execution(legacy(spark, true))
          incompatible <- execution(tokens(spark, Some(previous), Vector(previous), None))
          _ <- write(if (silverMissing) missing else incompatible, paths.silver, execution)
          _ <- write(if (silverMissing) incompatible else missing, paths.lateFacts, execution)
          result <- stage(paths, execution).ensurePrimaryTokenCompatibility(spark, at).attempt
          _ <- IO(assertEquals(result, Left(if (silverMissing) missingTokenError else primaryError)))
        } yield ()
      }
    }
  }

  test("token array type errors fail closed rather than being filtered away") {
    withLakehouse { (spark, execution, paths) =>
      for {
        wrongShape <- execution(
          spark.createDataFrame(
            Vector(Row(current)).asJava,
            StructType(Vector(StructField(Columns.SubjectTokens, StringType, nullable = false)))
          )
        )
        _ <- write(wrongShape, paths.quarantine, execution)
        result <- stage(paths, execution).validateStoredTokenKeys(spark).attempt
        _ <- IO(assert(result.isLeft))
      } yield ()
    }
  }

  test("every validation reads retirement authorizations and current key registry again") {
    withLakehouse { (spark, execution, paths) =>
      for {
        reads <- Ref.of[IO, Int](0)
        freshLookup = new KeyRetirementLookup[IO] {
          override def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]] =
            reads.update(_ + 1).as(Vector.empty)
        }
        validator = new AnalyticsKeyContinuityStage[IO](
          paths,
          keys,
          execution,
          freshLookup,
          AnalyticsTestClocks.fixed(at)
        )
        _ <- validator.validateHmacConfiguration(spark)
        _ <- validator.validateHmacConfiguration(spark)
        changed <- execution(
          spark.createDataFrame(
            Vector(
              Row(keys.primaryKeyId, "c" * 43),
              Row("previous-key", keys.keyVerifiers.toMap.apply("previous-key"))
            ).asJava,
            StructType(
              Vector(
                StructField("keyId", StringType, nullable = false),
                StructField("verifier", StringType, nullable = false)
              )
            )
          )
        )
        _ <- write(changed, paths.hmacKeyRegistry, execution)
        result <- validator.validateHmacConfiguration(spark).attempt
        count <- reads.get
        _ <- IO { assert(result.isLeft); assertEquals(count, 3) }
      } yield ()
    }
  }
}
