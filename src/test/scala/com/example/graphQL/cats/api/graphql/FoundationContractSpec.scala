package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.application.{DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite

final class FoundationContractSpec extends CatsEffectSuite {
  private def fixture(name: String): IO[String] = IO.blocking {
    val stream = Option(getClass.getResourceAsStream(s"/graphql/$name"))
      .getOrElse(throw new IllegalArgumentException("Missing contract fixture"))
    try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    finally stream.close()
  }

  private val probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
  private val service = new HealthService(probe, Diagnostics.noop)

  test("served SDL matches the deterministic contract fixture") {
    fixture("foundation.graphql").map(expected => assertEquals(FoundationSchema.sdl, expected))
  }

  List("health.graphql", "readiness.graphql", "introspection.graphql").foreach { name =>
    test(s"execute consumer fixture $name") {
      for {
        query <- fixture(name)
        parsed <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
          new IllegalArgumentException("Fixture exceeds request contract"))
        result <- FoundationSchema.execute(parsed, service, "00000000-0000-0000-0000-000000000001")
      } yield {
        val json = result.fold(failure => fail(failure.toString), identity)
        assert(json.hcursor.downField("data").succeeded)
        assert(!json.hcursor.downField("errors").succeeded)
      }
    }
  }

}
