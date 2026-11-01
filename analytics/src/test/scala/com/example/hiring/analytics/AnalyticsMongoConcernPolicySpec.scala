package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.SparkRuntime
import com.example.hiring.analytics.config.AnalyticsMongoConcernPolicy

import com.mongodb.{ReadConcern, ReadPreference, WriteConcern}
import munit.FunSuite

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** DDR-05.2: every analytics Mongo client and transaction takes its concerns from one policy. */
final class AnalyticsMongoConcernPolicySpec extends FunSuite {
  private val majorityJournal =
    WriteConcern.MAJORITY.withJournal(true).withWTimeout(15000L, TimeUnit.MILLISECONDS)

  test("DDR-05.2 client settings override a URI that states nothing or a stronger value") {
    List(
      "mongodb://host/",
      "mongodb://host/?w=majority&journal=true&readPreference=primary&readConcernLevel=snapshot&wtimeoutMS=1"
    ).foreach { uri =>
      val settings = SparkRuntime.effectiveSettings(uri).fold(throw _, identity)
      assertEquals(settings.getWriteConcern, majorityJournal, uri)
      assertEquals(settings.getReadConcern, ReadConcern.MAJORITY, uri)
      assertEquals(settings.getReadPreference, ReadPreference.primary(), uri)
    }
  }

  test("DDR-05.2 client settings override even a weaker URI that bypassed configuration") {
    val settings = SparkRuntime
      .effectiveSettings("mongodb://host/?w=1&journal=false&readPreference=secondary&readConcernLevel=local")
      .fold(throw _, identity)
    assertEquals(settings.getWriteConcern, majorityJournal)
    assertEquals(settings.getReadConcern, ReadConcern.MAJORITY)
    assertEquals(settings.getReadPreference, ReadPreference.primary())
  }

  test("DDR-05.2 transaction options carry majority with journal, majority and primary") {
    val options = AnalyticsMongoConcernPolicy.transactionOptions
    assertEquals(options.getWriteConcern, majorityJournal)
    assertEquals(options.getReadConcern, ReadConcern.MAJORITY)
    assertEquals(options.getReadPreference, ReadPreference.primary())
  }

  private def sources: List[(String, String)] = {
    val root = Paths.get("src/main/scala")
    assume(Files.isDirectory(root), "run from the analytics project root")
    Using.resource(Files.walk(root))(
      _.iterator().asScala
        .filter(_.toString.endsWith(".scala"))
        .map(path => path.getFileName.toString -> Files.readString(path))
        .toList
    )
  }

  test("DDR-05.2 production code builds the data Mongo client in SparkRuntime only") {
    // MongoStartupCheck builds a short-lived liveness client that only pings the admin database and writes nothing.
    val allowed = Set("SparkRuntime.scala", "MongoStartupCheck.scala")
    val tokens = List("MongoClient.create", "MongoClients.create", "fromConnectionString")
    val offenders = sources.flatMap { case (name, text) =>
      tokens.filter(text.contains).filterNot(_ => allowed(name)).map(token => s"$name: $token")
    }
    assertEquals(offenders, Nil)
    val runtime = sources.find(_._1 == "SparkRuntime.scala").map(_._2).getOrElse(fail("SparkRuntime.scala missing"))
    assert(runtime.contains("MongoClient.create") && !runtime.contains("fromConnectionString"))
  }

  test("DDR-05.2 every transaction starts with explicit options") {
    val bare = "startTransaction(?!\\(AnalyticsMongoConcernPolicy\\.transactionOptions\\))".r
    val offenders = sources.collect { case (name, text) if bare.findFirstIn(text).isDefined => name }
    assertEquals(offenders, Nil)
    assert(sources.exists(_._2.contains("startTransaction(AnalyticsMongoConcernPolicy.transactionOptions)")))
  }

  test("DDR-05.2 analytics runs no mongot-served stage that would need read concern local") {
    val stages = List("$search", "$vectorSearch", "$listSearchIndexes", "$rankFusion", "$scoreFusion")
    val offenders = sources.collect {
      case (name, text) if name != "AnalyticsMongoConcernPolicy.scala" && stages.exists(text.contains) => name
    }
    assertEquals(offenders, Nil)
  }
}
