package com.example.hiring.analytics

import cats.effect.{IO, IOApp}
import com.example.hiring.analytics.app.{AppModule, StreamingDurableBoundary, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.StreamingBatchIdentity

import java.nio.file.{Files, Path}

/** Test-only streaming process: writes a marker file at the chosen durable boundary and then blocks until killed. */
object StreamingProcessChild extends IOApp.Simple {
  override def run: IO[Unit] =
    for {
      settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
      marker = Path.of(sys.props("hiring.child.marker"))
      boundary = StreamingDurableBoundary.valueOf(sys.props("hiring.child.boundary"))
      observer = new StreamingDurableBoundaryObserver[IO] {
        override def completed(reached: StreamingDurableBoundary, identity: StreamingBatchIdentity): IO[Unit] =
          IO.whenA(reached == boundary)(
            IO.blocking(Files.writeString(marker, reached.toString)).void *> IO.never[Unit]
          )
      }
      _ <- AppModule.streamingObserved[IO](settings, Some(observer)).use(identity)
    } yield ()
}
