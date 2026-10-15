package com.example.graphQL.cats.api.http

import cats.effect.IO
import com.example.graphQL.cats.api.admission.PersistedQueryPolicy
import com.example.graphQL.cats.api.graphql.TestGraphQLSupport
import com.example.graphQL.cats.config.{PersistedQueryConfig, PersistedQueryMode}
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.duration.*

/** AC-12: only listed full documents run in allowlist mode; introspection-only documents are exempt. */
final class PersistedQueryAllowlistSpec extends CatsEffectSuite {
  private val probe = new DatabaseProbe[IO] {
    def check(requestId: Option[String]): IO[ProbeResult] = IO.pure(ProbeResult.Ready)
  }

  private val listed = "{ me { id } }"

  private def build(policy: PersistedQueryPolicy, schemaExposure: Boolean = true): IO[HttpApp[IO]] =
    TestGraphQLSupport.dependencies(schemaExposure = schemaExposure).allocated.flatMap { case (dependencies, _) =>
      new HiringApiRoutes(
        new HealthService(probe, Diagnostics.noop),
        Diagnostics.noop,
        dependencies.copy(persistedQueries = policy)
      ).httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
    }

  private val allowlist = PersistedQueryPolicy.Allowlist(Set(PersistedQueryPolicy.hash(listed)))

  private def post(query: String): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql")).withEntity(Json.obj("query" -> Json.fromString(query)))

  private def code(body: Json): Option[String] =
    body.hcursor.downField("errors").downArray.downField("extensions").get[String]("code").toOption

  test("AC-12.4 off mode runs any valid document as before") {
    for {
      http <- build(PersistedQueryPolicy.Off)
      body <- http(post(listed)).flatMap(_.as[Json])
      other <- http(post("{  me { id } }")).flatMap(_.as[Json])
    } yield {
      assert(body.hcursor.downField("data").downField("me").succeeded, body.noSpaces)
      assert(other.hcursor.downField("data").downField("me").succeeded, other.noSpaces)
    }
  }

  test("AC-12.1 a listed document runs in allowlist mode") {
    for {
      http <- build(allowlist)
      response <- http(post(listed))
      body <- response.as[Json]
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(body.hcursor.downField("data").downField("me").succeeded, body.noSpaces)
    }
  }

  test("AC-12.2 an unlisted document gets 400 PERSISTED_QUERY_NOT_FOUND without echoing it, one extra space included") {
    val altered = "{ me  { id } }"
    for {
      http <- build(allowlist)
      response <- http(post(altered))
      text <- response.as[String]
      body <- IO.fromEither(io.circe.parser.parse(text))
    } yield {
      assertEquals(response.status, Status.BadRequest)
      assertEquals(code(body), Some("PERSISTED_QUERY_NOT_FOUND"))
      assert(!text.contains("{ me") && !text.contains("id }"), text)
    }
  }

  test("AC-12.2 an unlisted document that is not even valid GraphQL is refused as not found, not parsed") {
    for {
      http <- build(allowlist)
      body <- http(post("{ not valid")).flatMap(_.as[Json])
    } yield assertEquals(code(body), Some("PERSISTED_QUERY_NOT_FOUND"))
  }

  test("AC-12.5 a request carrying only a hash and no query is rejected without lookup") {
    val request = Request[IO](Method.POST, Uri.unsafeFromString("/graphql")).withEntity(
      Json.obj(
        "extensions" -> Json.obj(
          "persistedQuery" -> Json.obj("sha256Hash" -> Json.fromString(PersistedQueryPolicy.hash(listed)))
        )
      )
    )
    for {
      http <- build(allowlist)
      response <- http(request)
    } yield assertEquals(response.status, Status.BadRequest)
  }

  test("AC-12.6 an introspection-only document runs without being listed") {
    for {
      http <- build(allowlist)
      schema <- http(post("{ __schema { queryType { name } } }")).flatMap(_.as[Json])
      typeName <- http(post("{ __typename }")).flatMap(_.as[Json])
      typed <- http(post("""{ __type(name: "User") { name } }""")).flatMap(_.as[Json])
    } yield {
      assertEquals(code(schema), None)
      assert(schema.hcursor.downField("data").downField("__schema").succeeded, schema.noSpaces)
      assertEquals(typeName.hcursor.downField("data").get[String]("__typename"), Right("Query"))
      assertEquals(code(typed), None)
    }
  }

  private val hostile: List[(String, String)] = List(
    "duplicate operation names" -> "query A { me { id } } query A { __typename }",
    "anonymous plus named" -> "{ __typename } query B { me { id } }",
    "two anonymous operations" -> "{ __typename } { __typename }",
    "real field in the second operation" -> "query A { __typename } query B { me { id } }",
    "mixed root selections" -> "{ __schema { queryType { name } } me { id } }",
    "fragment spread" -> "query { ...F } fragment F on Query { __schema { queryType { name } } }",
    "inline fragment" -> "{ ... { __schema { queryType { name } } } }",
    "unused fragment definition" -> "{ __typename } fragment F on Query { me { id } }",
    "unparseable" -> "{ __typename"
  )

  hostile.foreach { case (name, query) =>
    test(s"AC-12.6 rejects $name") {
      build(allowlist).flatMap(http => http(post(query)).flatMap(_.as[Json])).map { body =>
        assertEquals(code(body), Some("PERSISTED_QUERY_NOT_FOUND"))
      }
    }
  }

  test("AC-12.6 an operationName selecting a real operation does not rescue a multi-operation document") {
    val request = Request[IO](Method.POST, Uri.unsafeFromString("/graphql")).withEntity(
      Json.obj(
        "query" -> Json.fromString("query A { me { id } } query B { __typename }"),
        "operationName" -> Json.fromString("A")
      )
    )
    build(allowlist).flatMap(http => http(request).flatMap(_.as[Json])).map { body =>
      assertEquals(code(body), Some("PERSISTED_QUERY_NOT_FOUND"))
    }
  }

  test("AC-12.6 exempt forms: several introspection operations, aliases, directives, and mutation/subscription roots") {
    List(
      "query A { __typename } query B { __schema { queryType { name } } }",
      "{ alias: __typename }",
      "{ __typename @include(if: true) }",
      "mutation { __typename }",
      "subscription { __typename }"
    ).foldLeft(IO.unit) { (acc, query) =>
      acc *> build(allowlist).flatMap(http => http(post(query)).flatMap(_.as[Json])).map { body =>
        assertNotEquals(code(body), Some("PERSISTED_QUERY_NOT_FOUND"), query)
      }
    }
  }

  test("AC-12.6 a hostile deeply nested document ends in a controlled 400 and never escapes the request") {
    val depth = 10000 // stays under the 64 KiB request limit
    val nested = "{ __typename " + ("{ a " * depth) + ("}" * depth) + " }"
    // Whether the parser copes or exhausts the stack, the request ends in a controlled 400, never a 500 or a crash.
    build(allowlist)
      .flatMap(http => http(post(nested)))
      .map(response => assertEquals(response.status, Status.BadRequest))
  }

  test("AC-12.6 the introspection exemption stays governed by schema exposure") {
    for {
      http <- build(allowlist, schemaExposure = false)
      body <- http(post("{ __schema { queryType { name } } }")).flatMap(_.as[Json])
    } yield assertEquals(code(body), Some("FORBIDDEN"))
  }

  test("AC-12.1 the hash is lowercase hex SHA-256 over the exact UTF-8 bytes") {
    assertEquals(
      PersistedQueryPolicy.hash("abc"),
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
    assertNotEquals(PersistedQueryPolicy.hash("{ a }"), PersistedQueryPolicy.hash("{ a } "))
  }

  test("AC-12.3 manifest parsing accepts comments and blanks and rejects empty or malformed manifests") {
    val hash = PersistedQueryPolicy.hash(listed)
    assertEquals(
      PersistedQueryPolicy.parseManifest(s"# note\n\n$hash\n"),
      Right(PersistedQueryPolicy.Allowlist(Set(hash)))
    )
    assertEquals(
      PersistedQueryPolicy.parseManifest("# only a comment\n"),
      Left(PersistedQueryPolicy.ManifestError.Empty)
    )
    assertEquals(
      PersistedQueryPolicy.parseManifest(s"$hash\nNOT-A-HASH\n"),
      Left(PersistedQueryPolicy.ManifestError.MalformedEntry(2))
    )
    assertEquals(
      PersistedQueryPolicy.parseManifest(hash.toUpperCase + "\n"),
      Left(PersistedQueryPolicy.ManifestError.MalformedEntry(1))
    )
  }

  test("AC-12.3 allowlist mode with a missing, empty or unreadable manifest fails to load; off needs no file") {
    val allowlistAt = (path: Option[String]) => PersistedQueryConfig(PersistedQueryMode.Allowlist, path)
    for {
      missing <- PersistedQueryPolicy.load(allowlistAt(Some("nonexistent-dir/persisted-queries.manifest")))
      absent <- PersistedQueryPolicy.load(allowlistAt(None))
      file <- IO.blocking(Files.createTempFile("persisted-queries", ".manifest"))
      empty <- PersistedQueryPolicy.load(allowlistAt(Some(file.toString)))
      _ <- IO.blocking(Files.writeString(file, PersistedQueryPolicy.hash(listed) + "\n", StandardCharsets.UTF_8))
      loaded <- PersistedQueryPolicy.load(allowlistAt(Some(file.toString)))
      _ <- IO.blocking(Files.delete(file))
      off <- PersistedQueryPolicy.load(PersistedQueryConfig())
    } yield {
      assertEquals(missing, Left(PersistedQueryPolicy.ManifestError.Unreadable))
      assertEquals(absent, Left(PersistedQueryPolicy.ManifestError.Unreadable))
      assertEquals(empty, Left(PersistedQueryPolicy.ManifestError.Empty))
      assertEquals(loaded, Right(PersistedQueryPolicy.Allowlist(Set(PersistedQueryPolicy.hash(listed)))))
      assertEquals(off, Right(PersistedQueryPolicy.Off))
    }
  }

  test("AC-12.1 the committed manifest equals a fresh generation from the operation fixtures") {
    val committed = Files.readString(PersistedQueryManifestCapture.ManifestPath, StandardCharsets.UTF_8)
    assertEquals(
      committed,
      PersistedQueryManifestCapture.manifestText(PersistedQueryManifestCapture.FixtureDirectory),
      "regenerate with: sbt \"Test / runMain com.example.graphQL.cats.api.http.PersistedQueryManifestCapture\""
    )
  }

  test("AC-12.1 every operation fixture is admitted by the committed manifest") {
    val manifest = PersistedQueryPolicy.parseManifest(
      Files.readString(PersistedQueryManifestCapture.ManifestPath, StandardCharsets.UTF_8)
    )
    val policy = manifest.fold(error => fail(error.toString), identity)
    PersistedQueryManifestCapture
      .operationFixtures(PersistedQueryManifestCapture.FixtureDirectory)
      .foreach(path => assert(policy.admits(Files.readString(path, StandardCharsets.UTF_8)), path.toString))
  }
}
