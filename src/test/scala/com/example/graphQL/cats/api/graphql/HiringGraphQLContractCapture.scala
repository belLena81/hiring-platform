package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, IOApp}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** Explicit local capture for the one active public schema; contract tests independently compare it. */
object HiringGraphQLContractCapture extends IOApp.Simple {
  def run: IO[Unit] = IO.blocking {
    val _ = Files.writeString(
      Path.of("src/test/resources/graphql/hiring.graphql"),
      HiringGraphQLSchema.sdl,
      StandardCharsets.UTF_8
    )
  }
}
