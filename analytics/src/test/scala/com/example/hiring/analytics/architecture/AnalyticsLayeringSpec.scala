package com.example.hiring.analytics.architecture

import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import cats.effect.IOApp
import munit.FunSuite

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
}
