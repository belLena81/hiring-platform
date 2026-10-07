package com.example.graphQL.cats

import com.example.hiring.testing.LocalTestServices
import java.nio.file.Path
import munit.FunSuite

final class LocalTestServicesSpec extends FunSuite {
  private val workspace = Path.of(System.getProperty("user.dir")).toRealPath()
  private val hash = java.util.HexFormat
    .of()
    .formatHex(
      java.security.MessageDigest
        .getInstance("SHA-256")
        .digest(workspace.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    )
    .take(12)
  private val nonce = "a" * 32
  private val manifest = LocalTestServices.Manifest(
    1,
    workspace.toString,
    nonce,
    "hiring-tests-" + hash + "-" + nonce.take(8),
    s"mongodb://127.0.0.1:27018/?replicaSet=hiring_test_$nonce&directConnection=true",
    "hiring_test_" + nonce,
    "127.0.0.1:19092",
    "a" * 22,
    "a" * 48,
    "b" * 48,
    "c" * 48,
    "d" * 48,
    "e" * 48,
    "f" * 48,
    "0" * 48
  )

  test("isolated service manifest accepts its exact workspace, identity and loopback endpoints") {
    assertEquals(LocalTestServices.validateManifest(manifest, workspace), Right(manifest))
  }

  test("isolated service manifest rejects application endpoints, remote hosts and ambiguous Mongo topology") {
    val invalid = List(
      manifest.copy(mongoUri = manifest.mongoUri.replace(":27018", ":27017")),
      manifest.copy(kafkaBootstrap = "127.0.0.1:9092"),
      manifest.copy(kafkaBootstrap = "production.example:19092"),
      manifest.copy(mongoUri = manifest.mongoUri.replace("127.0.0.1", "production.example")),
      manifest.copy(mongoUri = manifest.mongoUri.replace("hiring_test_" + nonce, "rs0")),
      manifest.copy(mongoUri = manifest.mongoUri.replace("directConnection=true", "directConnection=false")),
      manifest.copy(mongoUri = manifest.mongoUri.replace("127.0.0.1:27018", "127.0.0.1:27018,127.0.0.1:27019")),
      manifest.copy(project = "hiring"),
      manifest.copy(nonce = "invalid"),
      manifest.copy(schema = 2)
    )
    invalid.foreach(value => assert(LocalTestServices.validateManifest(value, workspace).isLeft))
  }
}
