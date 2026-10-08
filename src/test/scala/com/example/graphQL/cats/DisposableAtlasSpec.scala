package com.example.graphQL.cats

import com.example.hiring.testing.DisposableAtlas
import munit.FunSuite

final class DisposableAtlasSpec extends FunSuite {
  private val uri = "mongodb://test-user:synthetic-secret@test.example:27017/?authSource=admin"
  private val environment =
    Map("ATLAS_TEST_URI" -> uri, "ATLAS_TEST_DISPOSABLE" -> "true", "ATLAS_TEST_ALLOWED_HOSTS" -> "test.example:27017")
  test("exact operator authorization permits a database-free disposable endpoint") {
    assertEquals(DisposableAtlas.authorizedUri(environment), Right(uri))
    assertEquals(
      DisposableAtlas.authorizedUri(
        environment
          .updated("ATLAS_TEST_URI", "mongodb+srv://test.example/")
          .updated("ATLAS_TEST_ALLOWED_HOSTS", "test.example")
      ),
      Right("mongodb+srv://test.example/")
    )
  }
  test("labels loopback deployments as local containers and everything else as atlas") {
    assertEquals(DisposableAtlas.deployment("mongodb://127.0.0.1:27018/?directConnection=true"), "local-container")
    assertEquals(DisposableAtlas.deployment("mongodb://localhost:27018/"), "local-container")
    assertEquals(DisposableAtlas.deployment("mongodb://test.example:27017/"), "atlas")
    assertEquals(DisposableAtlas.deployment("mongodb://127.0.0.1:27018,test.example:27017/"), "atlas")
    assertEquals(DisposableAtlas.deployment("mongodb+srv://cluster.example.mongodb.net/"), "atlas")
    assertEquals(DisposableAtlas.deployment("not-a-uri"), "atlas")
  }
  test("rejects missing authorization, host ambiguity and an application database before connection") {
    val rejected = List(
      environment - "ATLAS_TEST_DISPOSABLE",
      environment - "ATLAS_TEST_ALLOWED_HOSTS",
      environment.updated("ATLAS_TEST_DISPOSABLE", "false"),
      environment.updated("ATLAS_TEST_ALLOWED_HOSTS", "*.example"),
      environment.updated("ATLAS_TEST_ALLOWED_HOSTS", "other.example:27017"),
      environment.updated("ATLAS_TEST_ALLOWED_HOSTS", "test.example:27017,other.example:27017"),
      environment.updated("ATLAS_TEST_URI", uri.replace("/?", "/application?")),
      environment.updated("ATLAS_TEST_URI", "invalid-synthetic-secret")
    )
    rejected.foreach { value =>
      val result = DisposableAtlas.authorizedUri(value)
      assert(result.isLeft)
      assert(!result.swap.toOption.getOrElse("").contains("synthetic-secret"))
    }
  }
}
