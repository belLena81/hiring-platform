package com.example.graphQL.cats.infrastructure.logging

import cats.effect.IO
import com.example.graphQL.cats.service.Diagnostics

/** Test-only access to the real diagnostics sink, whose factory is visible to this package only. */
object RecordingDiagnostics {
  def apply(sink: String => IO[Unit]): Diagnostics[IO] = SafeDiagnostics.withSink(sink)
}
