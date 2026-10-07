package com.example.hiring.analytics.adapter.spark

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.AnalyticsTestSubjectPseudonymizer
import com.example.hiring.analytics.domain.AnalyticsDigest
import munit.CatsEffectSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.Instant
import java.nio.charset.StandardCharsets

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class StreamingAdmissionCacheSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private def withSpark(check: (SparkSession, SparkExecution[IO]) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      Resource
        .make(execution {
          SparkSession
            .builder()
            .master("local[2]")
            .appName("StreamingAdmissionCacheSpec")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        })(spark => execution.blocking(spark.stop()))
        .use { spark =>
          execution.attachSparkContext(spark.sparkContext) *> check(spark, execution)
        }
    }

  private def incoming(spark: SparkSession): DataFrame = spark.createDataFrame(
    Vector(Row("59fcc6f9-5cc4-37f5-ab1f-f33f516582d2"), Row("5bbf2d67-25b2-3ad8-9db7-c3ab47c50930")).asJava,
    StructType(Vector(StructField(Columns.EventId, StringType, nullable = false)))
  )

  private def materialize(frames: Vector[DataFrame], execution: SparkExecution[IO]): IO[Unit] =
    execution(frames.foreach(_.count()))

  private def persistentIds(spark: SparkSession, execution: SparkExecution[IO]): IO[Set[Int]] =
    execution(spark.sparkContext.getPersistentRDDs.keySet.toSet)

  test("registering the cache before RDD consumers capture their plans evaluates incoming rows once") {
    withSpark { (spark, execution) =>
      for {
        input <- execution {
          val visits = spark.sparkContext.longAccumulator("admission-source-row-visits")
          val frame = incoming(spark)
          val counted = spark.createDataFrame(
            frame.rdd.mapPartitions { rows =>
              rows.map { row => visits.add(1L); row }
            },
            frame.schema
          )
          (counted, visits)
        }
        baseline <- persistentIds(spark, execution)
        counts <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(input._1), execution).use { cached =>
          execution {
            // Tokenization creates DataFrames from captured source RDDs through this same Spark boundary.
            val first = spark.createDataFrame(
              cached.head.rdd.mapPartitions(_.map(row => Row(row.getString(0)))),
              cached.head.schema
            )
            val second = spark.createDataFrame(
              cached.head.rdd.mapPartitions(_.map(row => Row(row.getString(0)))),
              cached.head.schema
            )
            Vector(first.count(), second.count())
          }
        }
        visits <- execution(input._2.value.longValue())
        after <- persistentIds(spark, execution)
        _ <- IO { assertEquals(counts, Vector(2L, 2L)); assertEquals(visits, 2L); assertEquals(after, baseline) }
      } yield ()
    }
  }

  test("prepared Silver reuse equals a fresh privacy projection with duplicates, conflicts and event times") {
    withSpark { (spark, execution) =>
      for {
        source <- execution {
          val keys = AnalyticsTestSubjectPseudonymizer.fromKeyRing("cache-key", Array.fill[Byte](32)(3), Vector.empty)
          val observed = Instant.parse("2026-10-02T12:00:00Z")
          def envelope(id: String, actor: String, skill: String, time: Instant): String =
            com.example.hiring.analytics.AnalyticsOperationalEventFixtures.complete(
              s"""{"eventId":"$id","eventType":"JOB_CREATED","occurredAt":"$time","aggregateType":"Job","aggregateId":"$id","actorId":"$actor","payload":{"job":{"skills":["$skill"]}}}"""
            )
          val retained =
            envelope("59fcc6f9-5cc4-37f5-ab1f-f33f516582d2", "c9be63e8-ea05-31cc-b925-5bbbae7d65e2", "Scala", observed)
          val future = envelope(
            "da907a1b-8f74-3692-ad93-b025eecfb852",
            "5f2f2b0f-0b0c-3450-9e50-4eea9d7058dc",
            "Scala",
            observed.plusSeconds(301)
          )
          val closed = envelope(
            "349e6863-3072-3975-902e-9ef4f939a5ac",
            "8de875cb-a6fd-3920-a118-844ce47a6eea",
            "Scala",
            observed.minusSeconds(172800)
          )
          val bodies = Vector(
            retained,
            retained,
            envelope("5bbf2d67-25b2-3ad8-9db7-c3ab47c50930", "9bd72a38-5f69-3277-8703-36fc02eef621", "Scala", observed),
            envelope("981f1875-7795-31e7-9585-a2ae43a196fb", "48b49325-d502-33d8-8491-18a1c1869124", "Scala", observed),
            envelope(
              "981f1875-7795-31e7-9585-a2ae43a196fb",
              "48b49325-d502-33d8-8491-18a1c1869124",
              "Kotlin",
              observed
            ),
            future,
            closed
          )
          val raw = spark.createDataFrame(
            bodies.zipWithIndex.map { case (body, offset) =>
              Row("hiring.cache.events", 0, offset.toLong, Timestamp.from(observed), body)
            }.asJava,
            StructType(
              Vector(
                StructField(Columns.Topic, StringType, nullable = false),
                StructField(Columns.Partition, IntegerType, nullable = false),
                StructField(Columns.Offset, LongType, nullable = false),
                StructField(Columns.Timestamp, TimestampType, nullable = false),
                StructField(Columns.Value, StringType, nullable = false)
              )
            )
          )
          val parsed = OperationalEventTransforms.parseKafkaRecords(raw)
          val tokenized = AnalyticsSubjectPrivacy.withSubjectToken(OperationalEventTransforms.validEvents(parsed), keys)
          val marked = AnalyticsTestSubjectPseudonymizer.tokenValue(keys, "9bd72a38-5f69-3277-8703-36fc02eef621")
          val markers = spark.createDataFrame(
            Vector(Row(marked)).asJava,
            StructType(Vector(StructField(Columns.SubjectToken, StringType, nullable = false)))
          )
          val fingerprints = Map(
            "59fcc6f9-5cc4-37f5-ab1f-f33f516582d2" -> retained,
            "da907a1b-8f74-3692-ad93-b025eecfb852" -> future,
            "349e6863-3072-3975-902e-9ef4f939a5ac" -> closed
          ).view
            .mapValues(body =>
              (
                AnalyticsDigest.sha256Hex(body.getBytes(StandardCharsets.UTF_8)),
                io.circe.parser
                  .parse(body)
                  .flatMap(_.hcursor.get[String]("actorId"))
                  .fold(error => fail(error.getMessage), identity)
              )
            )
            .toMap
          (keys, tokenized, markers, fingerprints)
        }
        safe <- execution.either(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(source._2, source._3))
        prepared <- execution.either(OperationalEventTransforms.silver(safe, source._1, source._3))
        baseline <- persistentIds(spark, execution)
        _ <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(safe), execution).use { _ =>
          for {
            fresh <- execution.either(OperationalEventTransforms.silver(safe, source._1, source._3))
            _ <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(fresh, prepared), execution).use { _ =>
              execution {
                assert(AnalyticsTableSchemas.matches(fresh.schema, AnalyticsTableSchemas.silver))
                assertEquals(fresh.columns.toVector, AnalyticsTableSchemas.silver.map(_._1))
                val freshRows = fresh.collect().toVector
                val preparedRows = prepared.collect().toVector
                assertEquals(freshRows.size, 3)
                assertEquals(preparedRows.size, 3)
                val expected = freshRows.toSet
                assertEquals(preparedRows.toSet, expected)
                assertEquals(
                  expected.map(_.getAs[String](Columns.EventId)),
                  Set(
                    "59fcc6f9-5cc4-37f5-ab1f-f33f516582d2",
                    "da907a1b-8f74-3692-ad93-b025eecfb852",
                    "349e6863-3072-3975-902e-9ef4f939a5ac"
                  )
                )
                expected.foreach { row =>
                  val id = row.getAs[String](Columns.EventId)
                  assertEquals(
                    row.getAs[String](Columns.SubjectToken),
                    AnalyticsTestSubjectPseudonymizer.tokenValue(
                      source._1,
                      source._4(id)._2
                    )
                  )
                  assertEquals(row.getAs[String](Columns.EventFingerprint), source._4(id)._1)
                }
              }
            }
          } yield ()
        }
        after <- persistentIds(spark, execution)
        _ <- IO(assertEquals(after, baseline))
      } yield ()
    }
  }

  test("successful admission releases every phase cache") {
    withSpark { (spark, execution) =>
      for {
        frame <- execution(incoming(spark))
        baseline <- persistentIds(spark, execution)
        during <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frame, frame.limit(1)), execution).use {
          cached => materialize(cached, execution) *> persistentIds(spark, execution)
        }
        after <- persistentIds(spark, execution)
        _ <- IO { assert(during.size > baseline.size); assertEquals(after, baseline) }
      } yield ()
    }
  }

  test("failed admission releases materialized caches before returning the original error") {
    val failure = new IllegalStateException("admission failed")
    withSpark { (spark, execution) =>
      for {
        frame <- execution(incoming(spark))
        baseline <- persistentIds(spark, execution)
        result <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(cached => materialize(cached, execution) *> IO.raiseError[Unit](failure))
          .attempt
        after <- persistentIds(spark, execution)
        _ <- IO { assertEquals(result, Left(failure)); assertEquals(after, baseline) }
      } yield ()
    }
  }

  test("cancelling admission releases materialized caches before the owner returns") {
    withSpark { (spark, execution) =>
      for {
        frame <- execution(incoming(spark))
        baseline <- persistentIds(spark, execution)
        entered <- Deferred[IO, Unit]
        operation = SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(cached => materialize(cached, execution) *> entered.complete(()).void *> IO.never[Unit])
        _ <- operation.background.use(_ => entered.get)
        after <- persistentIds(spark, execution)
        _ <- IO(assertEquals(after, baseline))
      } yield ()
    }
  }

  test("partial cache acquisition failure releases earlier owned frames") {
    val failure = new IllegalStateException("second cache acquisition failed")
    withSpark { (spark, execution) =>
      for {
        frames <- execution { val frame = incoming(spark); Vector(frame, frame.limit(1)) }
        calls <- Ref.of[IO, Int](0)
        failing = new SparkExecution[IO] {
          override def apply[A](work: => A): IO[A] = calls.getAndUpdate(_ + 1).flatMap {
            case 1 => IO.raiseError(failure)
            case _ => execution(work)
          }
          override def either[A](work: => Either[AnalyticsError, A]): IO[A] = apply(work).flatMap(IO.fromEither)
        }
        result <- SparkStreamingBatchStages.cacheAdmissionFrames(frames, failing).use(_ => IO.unit).attempt
        levels <- execution(frames.map(_.storageLevel))
        _ <- IO { assertEquals(result, Left(failure)); assertEquals(levels, Vector.fill(2)(StorageLevel.NONE)) }
      } yield ()
    }
  }

  test("a cache owned by an enclosing resource survives nested admission cleanup") {
    withSpark { (spark, execution) =>
      for {
        frame <- execution(incoming(spark))
        _ <- Resource
          .make(execution(frame.persist(StorageLevel.MEMORY_AND_DISK)))(cached =>
            execution(cached.unpersist(blocking = true)).void
          )
          .use { cached =>
            for {
              _ <- materialize(Vector(cached), execution)
              baseline <- persistentIds(spark, execution)
              equivalent <- execution(cached.select(col(Columns.EventId)))
              _ <- SparkStreamingBatchStages
                .cacheAdmissionFrames(Vector(cached, equivalent), execution)
                .use(frames => materialize(frames, execution))
              after <- persistentIds(spark, execution)
              level <- execution(cached.storageLevel)
              _ <- IO { assertEquals(after, baseline); assertEquals(level, StorageLevel.MEMORY_AND_DISK) }
            } yield ()
          }
      } yield ()
    }
  }

  test("a later admission phase materializes its fresh selection after the first phase is released") {
    withSpark { (spark, execution) =>
      for {
        source <- execution(
          incoming(spark)
            .withColumn(Columns.SubjectToken, col(Columns.EventId))
            .withColumn(Columns.SubjectTokens, array(col(Columns.EventId)))
        )
        frame <- execution.either(
          AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(
            source,
            AnalyticsSubjectPrivacy.emptyMarkers(source)
          )
        )
        first <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(cached => execution(cached.head.count()))
        firstLevel <- execution(frame.storageLevel)
        refreshed <- execution.either(
          AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(
            source,
            source.filter(col(Columns.EventId) === "5bbf2d67-25b2-3ad8-9db7-c3ab47c50930").select(Columns.SubjectToken)
          )
        )
        second <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(refreshed), execution)
          .use(cached => execution(cached.head.select(Columns.EventId).collect().toVector.map(_.getString(0))))
        after <- persistentIds(spark, execution)
        _ <- IO {
          assertEquals(first, 2L)
          assertEquals(firstLevel, StorageLevel.NONE)
          assertEquals(second, Vector("59fcc6f9-5cc4-37f5-ab1f-f33f516582d2"))
          assertEquals(after, Set.empty[Int])
        }
      } yield ()
    }
  }
}
