package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.AnalyticsTestSpark
import cats.effect.{IO, Resource}
import io.circe.Json
import io.circe.parser.parse
import munit.CatsEffectSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, lit}

import java.nio.file.{Files, Path}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** AC-04.1 and AC-04.2: data-skipping statistics are gathered only on the declared allow-list, and they let a MERGE on
  * Kafka coordinates read fewer files.
  */
final class DeltaFileSkippingSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-file-skipping-")))(path =>
          IO.blocking {
            val entries = Files.walk(path)
            try entries.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
            finally entries.close()
          }
        )
        spark <- Resource.make(execution(AnalyticsTestSpark.builder("DeltaFileSkippingSpec").getOrCreate()))(spark =>
          execution.blocking(spark.stop())
        )
        _ <- Resource.eval(execution.attachSparkContext(spark.sparkContext))
      } yield (spark, root)
      resource.use { case (spark, root) => check(spark, execution, root) }
    }

  private def append(spark: SparkSession, path: String, spec: DeltaTableSpec, first: Long, count: Int): Unit =
    DeltaSubjectTableFixtures
      .frame(spark, spec, first, count)
      .coalesce(1)
      .write
      .format("delta")
      .mode("append")
      .save(SparkPhysicalLocation.resolve(path))

  /** The `stats` JSON of every add action in the table's log. */
  private def addStats(path: String): Vector[Json] = {
    val log = Path.of(SparkPhysicalLocation.resolve(path)).resolve("_delta_log")
    val entries = Files.list(log)
    try
      entries
        .iterator()
        .asScala
        .toVector
        .filter(_.getFileName.toString.endsWith(".json"))
        .flatMap { commit =>
          Files.readAllLines(commit).asScala.toVector.flatMap { line =>
            parse(line).toOption.flatMap(_.hcursor.downField("add").downField("stats").as[String].toOption)
          }
        }
        .flatMap(parse(_).toOption)
    finally entries.close()
  }

  private def statKeys(stats: Json): Set[String] =
    Vector("minValues", "maxValues", "nullCount")
      .flatMap(field => stats.hcursor.downField(field).keys.toVector.flatten)
      .toSet

  private def executionMark(spark: SparkSession): Long =
    spark.sharedState.statusStore.executionsList().map(_.executionId).maxOption.getOrElse(-1L)

  /** The data files Spark scanned in the SQL executions started after `mark`: the sum of every scan's "number of files
    * read" metric.
    */
  private def filesReadSince(spark: SparkSession, mark: Long): Long = {
    val store = spark.sharedState.statusStore
    val deadline = System.nanoTime() + 20L * 1000000000L
    def finished = store.executionsList().filter(_.executionId > mark).forall(_.completionTime.isDefined)
    // The status store is fed asynchronously from the listener bus.
    while (!finished && System.nanoTime() < deadline) Thread.sleep(50L)
    store
      .executionsList()
      .filter(_.executionId > mark)
      .flatMap { execution =>
        val values = store.executionMetrics(execution.executionId)
        store
          .planGraph(execution.executionId)
          .allNodes
          .flatMap(_.metrics)
          .filter(_.name == "number of files read")
          .flatMap(metric => values.get(metric.accumulatorId))
      }
      .map(_.replace(",", "").trim.toLong)
      .sum
  }

  private def filesRead(spark: SparkSession)(body: => Unit): Long = {
    val mark = executionMark(spark)
    body
    filesReadSince(spark, mark)
  }

  test("AC-04.1 every subject-attributed table gathers statistics only on its allow-list") {
    withSpark { (spark, execution, root) =>
      execution {
        AnalyticsDeltaCatalog.all.filter(_.subjectAttributed).foreach { spec =>
          val path = root.resolve(spec.name).toString
          AnalyticsTableSchemas.createOrValidate(spark, path, spec)
          (0 until 3).foreach(batch => append(spark, path, spec, batch * 10L, 10))
          val stats = addStats(path)
          assert(stats.nonEmpty, s"${spec.name} wrote no statistics, so the property is not honored")
          stats.foreach { entry =>
            val keys = statKeys(entry)
            assert(keys.forall(spec.statsColumns.contains), s"${spec.name} indexed ${keys -- spec.statsColumns}")
            assert(!keys.exists(DeltaStatsPolicy.isForbidden), s"${spec.name} indexed a token or identifier column")
            assert(spec.statsColumns.forall(keys.contains), s"${spec.name} did not index its whole allow-list")
            assert(!entry.noSpaces.contains(DeltaSubjectTableFixtures.TokenPrefix))
          }
        }
      }
    }
  }

  private def withBronzeVariants(
      body: (SparkSession, String, String) => Unit
  )(spark: SparkSession, root: Path): Unit = {
    val spec = AnalyticsDeltaCatalog.Bronze
    val on = root.resolve("stats-on").toString
    val off = root.resolve("stats-off").toString
    AnalyticsTableSchemas.createOrValidate(spark, on, spec)
    // The baseline is the pre-S4 shape: the same columns with statistics disabled.
    val _ = io.delta.tables.DeltaTable
      .createIfNotExists(spark)
      .location(SparkPhysicalLocation.resolve(off))
      .addColumns(spec.schema)
      .property(DeltaStatsPolicy.NumIndexedColumnsProperty, "0")
      .execute()
    Vector(on, off).foreach(path => (0 until 8).foreach(batch => append(spark, path, spec, batch * 100L, 100)))
    body(spark, on, off)
  }

  private def mergeNewOffsets(spark: SparkSession, path: String, first: Long, bounded: Boolean): Unit = {
    val source = DeltaSubjectTableFixtures.frame(spark, AnalyticsDeltaCatalog.Bronze, first, 5)
    val condition =
      if (bounded) MergeKeyBounds.bounded(source, MergeKeyBounds.CoordinateCondition)
      else org.apache.spark.sql.functions.expr(MergeKeyBounds.CoordinateCondition)
    val _ = DeltaTables
      .forPath(spark, path)
      .as("target")
      .merge(source.as("source"), condition)
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  test("AC-04.2 a MERGE on Kafka coordinates reads fewer files with statistics on than with them off") {
    withSpark { (spark, execution, root) =>
      execution {
        withBronzeVariants { (session, on, off) =>
          assertEquals(addStats(on).size, 8)
          assertEquals(addStats(off).count(_.hcursor.downField("minValues").succeeded), 0)
          // Offsets 350 to 354 exist only in the fourth file (300 to 399), so they are duplicates the merge must not insert.
          val plainOn = filesRead(session)(mergeNewOffsets(session, on, 350L, bounded = false))
          val plainOff = filesRead(session)(mergeNewOffsets(session, off, 350L, bounded = false))
          val boundedOn = filesRead(session)(mergeNewOffsets(session, on, 360L, bounded = true))
          val boundedOff = filesRead(session)(mergeNewOffsets(session, off, 360L, bounded = true))
          println(
            s"FILE_SKIPPING_MEASUREMENT files=8 rowsPerFile=100 sourceRows=5 " +
              s"unboundedStatsOn=$plainOn unboundedStatsOff=$plainOff boundedStatsOn=$boundedOn boundedStatsOff=$boundedOff"
          )
          assertEquals(plainOn, plainOff)
          assert(boundedOn < boundedOff, s"statistics on read $boundedOn files, off read $boundedOff")
          assert(boundedOn <= 2L, s"bounded merge read $boundedOn files")
          assert(boundedOff >= 8L, s"without statistics the merge read $boundedOff files")
        }(spark, root)
      }
    }
  }
  test("AC-04.2 the production writer bounds a coordinate MERGE and stays idempotent") {
    withSpark { (spark, execution, root) =>
      val paths = com.example.hiring.analytics.TestAnalyticsLakehousePaths.unsafe(root.toUri.toString.stripSuffix("/"))
      val spec = AnalyticsDeltaCatalog.Bronze
      val writer = new DeltaBatchWriter[IO](paths, execution)
      for {
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, paths.bronze, spec)
          (0 until 8).foreach(batch => append(spark, paths.bronze, spec, batch * 100L, 100))
        }
        mark <- execution(executionMark(spark))
        // Five duplicates of existing offsets 350 to 354, then five new offsets in a later file's range.
        duplicates <- execution(DeltaSubjectTableFixtures.frame(spark, spec, 350L, 5))
        _ <- writer.merge(duplicates, paths.bronze, MergeKeyBounds.CoordinateCondition)
        duplicateFiles <- execution(filesReadSince(spark, mark))
        _ <- execution {
          assertEquals(DeltaTables.read(spark, paths.bronze).count(), 800L, "duplicates are not inserted")
        }
        fresh <- execution(DeltaSubjectTableFixtures.frame(spark, spec, 900L, 5))
        _ <- writer.merge(fresh, paths.bronze, MergeKeyBounds.CoordinateCondition)
        _ <- writer.merge(fresh, paths.bronze, MergeKeyBounds.CoordinateCondition)
        _ <- execution {
          assertEquals(DeltaTables.read(spark, paths.bronze).count(), 805L, "new offsets are inserted once")
          assert(duplicateFiles <= 2L, s"the bounded production merge read $duplicateFiles of 8 files")
        }
      } yield ()
    }
  }

  /** Rows for every topic and partition, each holding offsets `first` until `first + count`. */
  private def coordinateFrame(
      spark: SparkSession,
      topics: Vector[String],
      partitions: Vector[Int],
      first: Long,
      count: Int
  ) =
    (for { topic <- topics; partition <- partitions } yield DeltaSubjectTableFixtures
      .frame(spark, AnalyticsDeltaCatalog.Bronze, first, count)
      .withColumn(Columns.Topic, lit(topic))
      .withColumn(Columns.Partition, lit(partition))).reduce(_.unionByName(_))

  test("AC-04.2 the production writer merges several topics and partitions, quotes and backslashes included, once") {
    withSpark { (spark, execution, root) =>
      val paths = com.example.hiring.analytics.TestAnalyticsLakehousePaths.unsafe(root.toUri.toString.stripSuffix("/"))
      val writer = new DeltaBatchWriter[IO](paths, execution)
      // "its" differs from "it's" only by the quote that adjacent SQL string literals would silently drop.
      val topics = Vector("it's", "its", "back\\slash", "it\\'s", "plain")
      val partitions = Vector(0, 1)
      def count = DeltaTables.read(spark, paths.bronze).count()
      for {
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, paths.bronze, AnalyticsDeltaCatalog.Bronze)
          coordinateFrame(spark, topics, partitions, 0L, 5)
            .coalesce(1)
            .write
            .format("delta")
            .mode("append")
            .save(SparkPhysicalLocation.resolve(paths.bronze))
        }
        seeded <- execution(count)
        duplicates <- execution(coordinateFrame(spark, topics, partitions, 0L, 5))
        _ <- writer.merge(duplicates, paths.bronze, MergeKeyBounds.CoordinateCondition)
        afterDuplicates <- execution(count)
        fresh <- execution(
          coordinateFrame(spark, Vector("it's", "back\\slash", "it\\'s"), Vector(1), 5L, 2)
            .unionByName(coordinateFrame(spark, Vector("plain"), Vector(0), 3L, 4))
        )
        _ <- writer.merge(fresh, paths.bronze, MergeKeyBounds.CoordinateCondition)
        _ <- writer.merge(fresh, paths.bronze, MergeKeyBounds.CoordinateCondition)
        perTopic <- execution(
          DeltaTables
            .read(spark, paths.bronze)
            .groupBy(Columns.Topic, Columns.Partition)
            .count()
            .collect()
            .toVector
            .map(row => (row.getString(0), row.getInt(1)) -> row.getLong(2))
            .toMap
        )
        _ = assertEquals(seeded, 50L)
        _ = assertEquals(afterDuplicates, 50L, "existing coordinates of every topic are matched, not re-inserted")
        _ = assertEquals(perTopic(("it's", 1)), 7L)
        _ = assertEquals(perTopic(("its", 1)), 5L, "the bound of it's did not reach its")
        _ = assertEquals(perTopic(("back\\slash", 1)), 7L)
        _ = assertEquals(perTopic(("it\\'s", 1)), 7L)
        _ = assertEquals(perTopic(("plain", 0)), 7L, "offsets 5 and 6 are new, 3 and 4 already existed")
        _ = assertEquals(perTopic(("plain", 1)), 5L)
        _ = assertEquals(perTopic.values.sum, 50L + 3L * 2L + 2L, "each new coordinate is inserted exactly once")
      } yield ()
    }
  }

  test("AC-04.2 coordinate bounds are Column literals per topic and partition and leave other conditions alone") {
    withSpark { (spark, execution, _) =>
      execution {
        val source = coordinateFrame(spark, Vector("it's"), Vector(1), 5L, 5)
          .unionByName(coordinateFrame(spark, Vector("plain"), Vector(0), 100L, 1))
        val bounded = MergeKeyBounds.bounded(source, MergeKeyBounds.CoordinateCondition).toString
        assert(bounded.contains("'it\\'s'") && bounded.contains("'plain'"), bounded)
        assert(!bounded.contains("''"), "no SQL-escaped literal")
        assertEquals(
          MergeKeyBounds.bounded(source, "target.eventId = source.eventId").toString,
          expr("target.eventId = source.eventId").toString
        )
        assertEquals(
          MergeKeyBounds.bounded(source.limit(0), MergeKeyBounds.CoordinateCondition).toString,
          expr(MergeKeyBounds.CoordinateCondition).toString
        )
      }
    }
  }

  test("AC-04.2 more than the maximum number of ranges keeps the plain coordinate condition") {
    val plain = expr(MergeKeyBounds.CoordinateCondition).toString
    def ranges(size: Int) = (0 until size).toVector.map(index => ("topic", index, 0L, 9L))
    assertEquals(
      MergeKeyBounds
        .boundedBy(MergeKeyBounds.CoordinateCondition, ranges(MergeKeyBounds.MaximumRanges + 1))
        .toString,
      plain
    )
    assertNotEquals(
      MergeKeyBounds.boundedBy(MergeKeyBounds.CoordinateCondition, ranges(MergeKeyBounds.MaximumRanges)).toString,
      plain
    )
  }

  private def rawTable(
      spark: SparkSession,
      path: String,
      spec: DeltaTableSpec,
      properties: Map[String, String]
  ): Unit = {
    val builder = io.delta.tables.DeltaTable
      .createIfNotExists(spark)
      .location(SparkPhysicalLocation.resolve(path))
      .addColumns(spec.schema)
    val _ = properties.foldLeft(builder) { case (current, (key, value)) => current.property(key, value) }.execute()
  }

  test("AC-04.1 create-or-validate refuses a subject table whose properties would index a token column") {
    withSpark { (spark, execution, root) =>
      execution {
        val spec = AnalyticsDeltaCatalog.Silver
        val declared = DeltaStatsPolicy.render(spec.statsColumns)
        val unsafe = Vector(
          "positional index without an allow-list" -> Map(DeltaStatsPolicy.NumIndexedColumnsProperty -> "32"),
          "the earlier no-statistics shape" -> Map(DeltaStatsPolicy.NumIndexedColumnsProperty -> "0"),
          "no statistics property at all" -> Map.empty[String, String],
          "allow-list with a subject token" ->
            Map(DeltaStatsPolicy.StatsColumnsProperty -> s"$declared,${Columns.SubjectToken}"),
          "allow-list of only the token" ->
            Map(DeltaStatsPolicy.StatsColumnsProperty -> Columns.SubjectToken),
          "allow-list with a raw identifier" ->
            Map(DeltaStatsPolicy.StatsColumnsProperty -> s"$declared,${Columns.AggregateId}"),
          "allow-list plus a positional index" ->
            Map(
              DeltaStatsPolicy.StatsColumnsProperty -> declared,
              DeltaStatsPolicy.NumIndexedColumnsProperty -> "5"
            )
        )
        unsafe.zipWithIndex.foreach { case ((label, properties), index) =>
          val path = root.resolve(s"unsafe-$index").toString
          rawTable(spark, path, spec, properties)
          intercept[com.example.hiring.analytics.errors.AnalyticsError.DeltaSchemaMismatch](
            AnalyticsTableSchemas.createOrValidate(spark, path, spec)
          )
          assert(
            DeltaStatsPolicy
              .permits(spec, DeltaLogFactory.system(spark, path).update().metadata.configuration) == false,
            label
          )
        }
        val safe = root.resolve("safe").toString
        AnalyticsTableSchemas.createOrValidate(spark, safe, spec)
        AnalyticsTableSchemas.createOrValidate(spark, safe, spec)
        // Tables without subject tokens are not constrained.
        val control = root.resolve("control").toString
        rawTable(
          spark,
          control,
          AnalyticsDeltaCatalog.RunManifests,
          Map(DeltaStatsPolicy.NumIndexedColumnsProperty -> "7")
        )
        AnalyticsTableSchemas.createOrValidate(spark, control, AnalyticsDeltaCatalog.RunManifests)
      }
    }
  }

  test("AC-04.1 the catalog cannot declare a token or identifier statistics column") {
    AnalyticsDeltaCatalog.all.filter(_.subjectAttributed).foreach { spec =>
      assert(spec.statsColumns.nonEmpty, spec.name)
      assert(!spec.statsColumns.exists(DeltaStatsPolicy.isForbidden), spec.name)
      assertEquals(
        spec.properties.get(DeltaStatsPolicy.StatsColumnsProperty),
        Some(DeltaStatsPolicy.render(spec.statsColumns))
      )
      assertEquals(spec.properties.get(DeltaStatsPolicy.NumIndexedColumnsProperty), None)
    }
    AnalyticsDeltaCatalog.all
      .filterNot(_.subjectAttributed)
      .foreach(spec => assert(spec.statsColumns.isEmpty, spec.name))
    val silver = AnalyticsDeltaCatalog.Silver
    Vector(
      Columns.SubjectToken,
      Columns.SubjectTokens,
      Columns.AggregateId,
      Columns.JobId,
      Columns.ApplicationId,
      "missing"
    ).foreach { bad =>
      intercept[IllegalArgumentException](silver.copy(statsColumns = silver.statsColumns :+ bad))
    }
    intercept[IllegalArgumentException](silver.copy(statsColumns = Vector.empty))
  }

}
