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

  private val searchOwners = Set("MongoSemanticSearchRepository.scala", "MongoLexicalJobMatcher.scala")
  private val adminFile = "MongoAtlasSearchAdmin.scala"
  // Direct stage literals and every helper that builds a mongot-served stage; a file touching any of them is a search
  // file, so a stage built through a helper cannot escape the rule.
  private val searchTokens = List(
    "\"$search\"",
    "\"$searchMeta\"",
    "\"$vectorSearch\"",
    "\"$rankFusion\"",
    "\"$scoreFusion\"",
    "MongoLexicalSearch",
    "lexicalSearchStage",
    "lexicalJobStages",
    "lexicalCandidateStages",
    "vectorSearchStage"
  )
  private val handleAcquisition = """Mongo4catsCollections\.documents\(|getCollection\(|\.getCollection""".r
  // The one authoritative (majority) handle in a search file: the eligibility `$lookup` read over a variable parent.
  private val authoritativeEligibilityHandle = "Mongo4catsCollections.documents(database, parentCollection)"

  private def sources: List[(Path, String)] = {
    val root = Paths.get("src/main/scala")
    assume(Files.isDirectory(root), "run from the project root")
    Using.resource(Files.walk(root))(
      _.iterator().asScala.filter(_.toString.endsWith(".scala")).map(path => path -> Files.readString(path)).toList
    )
  }

  test(
    "DDR-05.2 a search file acquires every collection handle through the pinned helper, except the eligibility read"
  ) {
    val searchFiles = sources.filter { case (path, text) =>
      path.getFileName.toString != adminFile && searchTokens.exists(text.contains)
    }
    // The named search owners must be recognised, so a rename cannot make the rule vacuous.
    assertEquals(searchFiles.map(_._1.getFileName.toString).toSet.intersect(searchOwners), searchOwners)
    val offenders = searchFiles.flatMap { case (path, text) =>
      val unpinned = handleAcquisition.findAllIn(text.replace(authoritativeEligibilityHandle, "")).toList
      val missingPin =
        if (text.contains("searchCollection(") || text.contains("jobsSearchHandle")) Nil else List("no pin")
      (unpinned ++ missingPin).map(token => s"${relative(path)}: $token")
    }
    assertEquals(offenders, Nil)
  }

  test("DDR-05.2 the pinned search handles are used only by the search repository, matcher and admin") {
    val allowed = searchOwners + adminFile
    val offenders = sources.collect {
      case (path, text)
          if !allowed(path.getFileName.toString) &&
            List("searchCollection(", "searchMetadataCollection(").exists(text.contains) =>
        relative(path)
    }
    assertEquals(offenders, Nil)
  }

  private def relative(path: Path): String = Paths.get("").toAbsolutePath.relativize(path.toAbsolutePath).toString
}
