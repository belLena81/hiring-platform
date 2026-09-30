package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.{DeltaPurgeRewrite, LakehouseOperation, SparkBlockingExecution}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem, RemoteIterator}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.nio.file.Files
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class DeltaPurgeRewriteSpec extends munit.FunSuite {
  override val munitTimeout: FiniteDuration = 3.minutes
  private val execution = new LakehouseOperation[IO](
    SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)
  )
  private val configuration = new Configuration()
  private lazy val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("DeltaPurgeRewriteSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def abandoned(root: java.nio.file.Path): java.nio.file.Path =
    Files.createDirectories(root.resolve("control").resolve("purge-rewrite-" + UUID.randomUUID()))

  private def recover(root: java.nio.file.Path, bound: Int = 1000, conf: Configuration = configuration): Unit =
    DeltaPurgeRewrite.recover[IO](root.toUri.toString, conf, bound, execution).unsafeRunSync()

  test("maintenance restart removes abandoned raw Delta copies and preserves unrelated control data") {
    val root = Files.createTempDirectory("delta-rewrite-restart")
    val orphan = abandoned(root)
    val unrelated = Files.createDirectories(root.resolve("control/run_manifests"))
    val retained = Files.writeString(unrelated.resolve("keep.json"), "retained manifest")
    spark
      .createDataFrame(
        Vector(Row("synthetic deleted subject raw payload")).asJava,
        StructType(Seq(StructField("value", StringType, nullable = false)))
      )
      .write
      .format("delta")
      .mode("overwrite")
      .save(orphan.toUri.toString)
    val paths = TestAnalyticsLakehousePaths.unsafe(root.toUri.toString)
    val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(1))
    // Reconstructed production maintenance acquires the mutex and recovers before continuity metadata is written.
    AnalyticsBatchTestSupport.newMaintenance(spark, paths, pseudonymizer).validateHmacConfiguration.unsafeRunSync()
    assert(!Files.exists(orphan))
    assertEquals(Files.readString(retained), "retained manifest")
    recover(root)
  }

  test("read-only audit rejects abandoned copies until recovery succeeds") {
    val root = Files.createTempDirectory("delta-rewrite-audit")
    val orphan = abandoned(root)
    Files.writeString(orphan.resolve("raw.json"), "synthetic subject")
    val audit = DeltaPurgeRewrite.verifyRecovered[IO](root.toUri.toString, configuration, 1000, execution)
    intercept[AnalyticsError](audit.unsafeRunSync())
    assert(Files.exists(orphan))
    recover(root)
    audit.unsafeRunSync()
  }

  test("malformed rewrite names block recovery before any recognized copy is deleted") {
    val root = Files.createTempDirectory("delta-rewrite-malformed")
    val valid = abandoned(root)
    Files.createDirectories(root.resolve("control/purge-rewrite-not-a-uuid"))
    intercept[AnalyticsError](recover(root))
    assert(Files.exists(valid))
  }

  test("bounded discovery fails before deletion on either control or subtree overflow") {
    val root = Files.createTempDirectory("delta-rewrite-bounded")
    val valid = abandoned(root)
    (1 to 3).foreach(i => Files.writeString(valid.resolve(s"$i.json"), "synthetic payload"))
    intercept[AnalyticsError](recover(root, bound = 3))
    assertEquals(Files.list(valid).useCount, 3L)
    val otherRoot = Files.createTempDirectory("delta-rewrite-control-bound")
    val other = abandoned(otherRoot)
    (1 to 3).foreach(i => Files.createDirectories(otherRoot.resolve(s"control/unrelated-$i")))
    intercept[AnalyticsError](recover(otherRoot, bound = 3))
    assert(Files.exists(other))
  }

  test("symlinks in abandoned copies cannot escape the selected lakehouse") {
    val root = Files.createTempDirectory("delta-rewrite-link")
    val outside = Files.createTempDirectory("delta-rewrite-outside")
    val retained = Files.writeString(outside.resolve("keep.json"), "unrelated payload")
    val orphan = abandoned(root)
    Files.createSymbolicLink(orphan.resolve("linked"), outside)
    intercept[AnalyticsError](recover(root))
    assertEquals(Files.readString(retained), "unrelated payload")
    assert(Files.exists(orphan))
  }

  test("nested rewrite discovery counts queued siblings against the bound before deleting anything") {
    val root = Files.createTempDirectory("delta-rewrite-nested-bound")
    val orphan = abandoned(root)
    val (_, retained) = (1 to 3).foldLeft(orphan -> Vector(orphan)) { case ((directory, paths), _) =>
      val siblings = (1 to 4).map(i => Files.writeString(directory.resolve(s"z-$i.json"), "synthetic payload"))
      val nested = Files.createDirectory(directory.resolve("a-descend"))
      nested -> (paths ++ siblings :+ nested)
    }
    val conf = new Configuration(configuration)
    conf.setClass(
      "fs.rewrite-count.impl",
      classOf[RewriteEnumerationCountingFileSystem],
      classOf[org.apache.hadoop.fs.FileSystem]
    )
    conf.setBoolean("fs.rewrite-count.impl.disable.cache", true)
    RewriteEnumerationCountingFileSystem.discovered.set(0)
    val bound = 10
    val remoteRoot = "rewrite-count:" + root.toUri.getPath
    intercept[AnalyticsError](DeltaPurgeRewrite.recover[IO](remoteRoot, conf, bound, execution).unsafeRunSync())
    assertEquals(RewriteEnumerationCountingFileSystem.discovered.get(), bound + 1)
    assert(retained.forall(Files.exists(_)))
  }

  test("symlinked control directories fail closed") {
    val root = Files.createTempDirectory("delta-rewrite-control-link")
    val outside = Files.createTempDirectory("delta-rewrite-control-outside")
    val retained = Files.createDirectories(outside.resolve("purge-rewrite-" + UUID.randomUUID()))
    Files.createSymbolicLink(root.resolve("control"), outside)
    intercept[AnalyticsError](recover(root))
    assert(Files.exists(retained))
  }

  test("dangling rewrite links and symlinked lakehouse ancestors fail closed") {
    val root = Files.createTempDirectory("delta-rewrite-dangling-link")
    val orphan = abandoned(root)
    Files.createSymbolicLink(orphan.resolve("missing"), root.resolve("absent"))
    intercept[AnalyticsError](recover(root))
    assert(Files.exists(orphan))
    val parent = Files.createTempDirectory("delta-rewrite-parent-link")
    val outside = Files.createTempDirectory("delta-rewrite-parent-target")
    val retained = abandoned(Files.createDirectories(outside.resolve("lakehouse")))
    val linked = Files.createSymbolicLink(parent.resolve("linked"), outside)
    intercept[AnalyticsError](recover(linked.resolve("lakehouse")))
    assert(Files.exists(retained))
  }

  test("cleanup failure prevents erasure absence verification from succeeding") {
    val root = Files.createTempDirectory("delta-rewrite-delete-failure")
    val orphan = abandoned(root)
    val denied = Files.writeString(orphan.resolve("deny-removal"), "synthetic payload")
    val conf = new Configuration(configuration)
    conf.setClass("fs.file.impl", classOf[RewriteDeletionDeniedFileSystem], classOf[org.apache.hadoop.fs.FileSystem])
    conf.setBoolean("fs.file.impl.disable.cache", true)
    intercept[AnalyticsError](recover(root, conf = conf))
    assert(Files.exists(denied))
    val paths = TestAnalyticsLakehousePaths.unsafe(root.toUri.toString)
    val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(1))
    val sparkConf = spark.sparkContext.hadoopConfiguration
    val previous = Option(sparkConf.get("fs.file.impl"))
    val previousCache = Option(sparkConf.get("fs.file.impl.disable.cache"))
    try {
      sparkConf.setClass(
        "fs.file.impl",
        classOf[RewriteDeletionDeniedFileSystem],
        classOf[org.apache.hadoop.fs.FileSystem]
      )
      sparkConf.setBoolean("fs.file.impl.disable.cache", true)
      intercept[AnalyticsError](
        AnalyticsBatchTestSupport
          .newMaintenance(spark, paths, pseudonymizer)
          .verifyMarkedSubjectsAbsent(Vector.empty)
          .unsafeRunSync()
      )
      assert(Files.exists(denied))
    } finally {
      previous.fold(sparkConf.unset("fs.file.impl"))(sparkConf.set("fs.file.impl", _))
      previousCache.fold(sparkConf.unset("fs.file.impl.disable.cache"))(sparkConf.set("fs.file.impl.disable.cache", _))
    }
  }

  extension (stream: java.util.stream.Stream[java.nio.file.Path])
    private def useCount: Long = try stream.count()
    finally stream.close()
}

class RewriteDeletionDeniedFileSystem extends RawLocalFileSystem {
  override def delete(path: Path, recursive: Boolean): Boolean =
    if (path.getName == "deny-removal") false else super.delete(path, recursive)
}

object RewriteEnumerationCountingFileSystem {
  val discovered = new java.util.concurrent.atomic.AtomicInteger(0)
}

class RewriteEnumerationCountingFileSystem extends RawLocalFileSystem {
  override def getUri: java.net.URI = java.net.URI.create("rewrite-count:///")
  override def listStatusIterator(path: Path): RemoteIterator[FileStatus] = {
    val entries = super.listStatus(path).sortBy(_.getPath.getName).iterator
    new RemoteIterator[FileStatus] {
      override def hasNext: Boolean = entries.hasNext
      override def next(): FileStatus = {
        RewriteEnumerationCountingFileSystem.discovered.incrementAndGet()
        entries.next()
      }
    }
  }
}
