package com.example.graphQL.cats.repository.mongo

import munit.FunSuite

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** DDR-05.2: every application Mongo client takes its concerns from `MongoDatabaseProbe.effectiveSettings`. */
final class MongoClientConstructionGuardSpec extends FunSuite {
  private val forbidden = List("MongoClient.create", "MongoClients.create", "fromConnectionString")
  private val owner = "MongoDatabaseProbe.scala"

  test("DDR-05.2 no production source builds a Mongo client outside MongoDatabaseProbe") {
    val root = Paths.get("src/main/scala")
    assume(Files.isDirectory(root), "run from the project root")
    val offenders = Using.resource(Files.walk(root))(stream =>
      stream
        .iterator()
        .asScala
        .filter(path => path.toString.endsWith(".scala") && path.getFileName.toString != owner)
        .flatMap { path =>
          val text = Files.readString(path)
          forbidden.filter(text.contains).map(token => s"${relative(path)}: $token")
        }
        .toList
    )
    assertEquals(offenders, Nil)
  }

  test("DDR-05.2 every source that lists search indexes pins local read concern, which that stage requires") {
    val root = Paths.get("src/main/scala")
    assume(Files.isDirectory(root), "run from the project root")
    val offenders = Using.resource(Files.walk(root))(stream =>
      stream
        .iterator()
        .asScala
        .filter(_.toString.endsWith(".scala"))
        .map(path => path -> Files.readString(path))
        .filter { case (_, text) => text.contains("\"$listSearchIndexes\"") && !text.contains("ReadConcern.LOCAL") }
        .map { case (path, _) => relative(path) }
        .toList
    )
    assertEquals(offenders, Nil)
  }

  private def relative(path: Path): String = Paths.get("").toAbsolutePath.relativize(path.toAbsolutePath).toString
}
