package com.example.hiring.analytics.architecture

import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import cats.effect.IOApp
import munit.FunSuite
import java.nio.file.Paths
import java.nio.file.Files
import scala.jdk.CollectionConverters.*

final class AnalyticsLayeringSpec extends FunSuite {
  test("domain, service, and config do not depend on adapters or CLI entrypoints") {
    val analyticsClasses = new ClassFileImporter().importPackages("com.example.hiring.analytics")
    val rule = noClasses()
      .that()
      .resideInAnyPackage("..domain..", "..service..", "..config..")
      .should()
      .dependOnClassesThat()
      .resideInAnyPackage("..adapter..", "..cli..")

    rule.check(analyticsClasses)
  }

  test("config does not depend on service") {
    val analyticsClasses = new ClassFileImporter().importPackages("com.example.hiring.analytics")
    val rule = noClasses()
      .that()
      .resideInAnyPackage("..config..")
      .should()
      .dependOnClassesThat()
      .resideInAnyPackage("..service..")

    rule.check(analyticsClasses)
  }

  test("domain, service, config, and errors do not depend on Spark") {
    val analyticsClasses = new ClassFileImporter().importPackages("com.example.hiring.analytics")
    val rule = noClasses()
      .that()
      .resideInAnyPackage("..domain..", "..service..", "..config..", "..errors..")
      .should()
      .dependOnClassesThat()
      .resideInAnyPackage("org.apache.spark..")

    rule.check(analyticsClasses)
  }

  test("domain, service, and config use explicit internal analytics imports") {
    val sourceRoot = Paths.get("src/main/scala/com/example/hiring/analytics")
    List("domain", "service", "config").foreach { layer =>
      val layerRoot = sourceRoot.resolve(layer)
      assert(Files.isDirectory(layerRoot), s"missing production source directory: $layerRoot")
      val files = Files.walk(layerRoot)
      try
        files
          .iterator()
          .asScala
          .filter(path => path.toString.endsWith(".scala"))
          .foreach { path =>
            val wildcardImports = Files
              .readString(path)
              .linesIterator
              .map(_.trim)
              .filter(line => line.startsWith("import com.example.hiring.analytics") && line.contains("*"))
              .toList
            assert(
              wildcardImports.isEmpty,
              s"internal analytics wildcard import found in $path: ${wildcardImports.mkString(", ")}"
            )
          }
      finally files.close()
    }
  }

  test("runnable analytics entrypoints live in cli and adapters expose no Main objects") {
    val adapterMains = List(
      "com.example.hiring.analytics.adapter.spark.HiringAnalyticsBatchMain$",
      "com.example.hiring.analytics.adapter.spark.AnalyticsErasureWorkerMain$",
      "com.example.hiring.analytics.adapter.spark.AnalyticsErasureRepairMain$"
    )
    adapterMains.foreach { className =>
      intercept[ClassNotFoundException](Class.forName(className))
    }

    val cliEntrypoints = List(
      "com.example.hiring.analytics.cli.HiringAnalyticsBatchMain$",
      "com.example.hiring.analytics.cli.AnalyticsErasureWorkerMain$",
      "com.example.hiring.analytics.cli.AnalyticsErasureRepairMain$"
    )
    cliEntrypoints.foreach { className =>
      assert(classOf[IOApp].isAssignableFrom(Class.forName(className)))
    }
  }

  test("local HMAC retirement operator tooling stays in test scope") {
    val productionRoot = Paths.get("src/main/scala/com/example/hiring/analytics")
    val testRoot = Paths.get("src/test/scala/com/example/hiring/analytics")
    val files = List(
      "adapter/local/LocalHmacKeyWriterExclusion.scala",
      "adapter/local/LocalProcess.scala",
      "adapter/mongo/HmacKeyRetirementPreparation.scala",
      "adapter/spark/HmacKeyRetirementCoordinator.scala"
    )

    files.foreach { file =>
      assert(!Files.exists(productionRoot.resolve(file)), s"operator tooling leaked into Compile: $file")
      assert(Files.exists(testRoot.resolve(file)), s"test-scoped operator tooling is missing: $file")
    }
  }

  test("batch and erasure orchestration live in service, not the Spark adapter") {
    assertEquals(
      Class.forName("com.example.hiring.analytics.service.batch.HiringAnalyticsBatch").getName,
      "com.example.hiring.analytics.service.batch.HiringAnalyticsBatch"
    )
    assertEquals(
      Class.forName("com.example.hiring.analytics.service.erasure.AnalyticsErasureWorker").getName,
      "com.example.hiring.analytics.service.erasure.AnalyticsErasureWorker"
    )
    intercept[ClassNotFoundException](Class.forName("com.example.hiring.analytics.adapter.spark.HiringAnalyticsBatch"))
    intercept[ClassNotFoundException](
      Class.forName("com.example.hiring.analytics.adapter.spark.AnalyticsErasureWorker")
    )
  }
}
