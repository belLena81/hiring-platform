package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.{AnalyticsDigest, AnalyticsTopic}
import com.example.hiring.analytics.errors.AnalyticsError

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, Paths}
import scala.concurrent.duration.*

/** Exercises the production startup path; only the exact checkpoint owner's typed failure passes. */
object StreamingCheckpointStartupProofMain extends IOApp {
  private[cli] val CheckpointFailure =
    "analytics streaming checkpoint is missing, malformed, or bound to another stream identity"
  private[cli] val CorruptionSentinel = "corrupted synthetic checkpoint identity\n"

  private[cli] def isCheckpointFailure(error: Throwable): Boolean = error match {
    case AnalyticsError.InvalidConfiguration(CheckpointFailure) => true
    case _                                                      => false
  }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      mode <- IO.fromOption(args match {
        case List(value @ ("missing" | "corrupt-identity")) => Some(value)
        case _                                              => None
      })(new IllegalArgumentException("checkpoint proof mode is invalid"))
      settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
      _ <- IO.fromEither(
        StreamingProofIsolation
          .validate(
            settings.common.mongoDatabase,
            AnalyticsTopic.unwrap(settings.topic),
            settings.common.lakehouseRoot,
            settings.streaming.checkpointLocation,
            settings.common.sparkLocalDirectory
          )
          .leftMap(new IllegalArgumentException(_))
      )
      _ <- IO.blocking {
        val root = Paths.get(new URI(settings.common.lakehouseRoot))
        val checkpoint = Paths.get(new URI(settings.streaming.checkpointLocation))
        val streamHash = AnalyticsDigest.sha256Hex(settings.streaming.streamId.getBytes(StandardCharsets.UTF_8))
        val established = root.resolve("control/streaming_lineage").resolve(streamHash + ".established")
        require(Files.isRegularFile(established, LinkOption.NOFOLLOW_LINKS), "established lineage is required")
        if (mode == "missing")
          require(!Files.exists(checkpoint, LinkOption.NOFOLLOW_LINKS), "checkpoint must be absent")
        else {
          val identity = checkpoint.resolve("_hiring_stream_identity")
          require(Files.isRegularFile(identity, LinkOption.NOFOLLOW_LINKS), "identity fixture is required")
          require(Files.size(identity) <= 1024L, "identity fixture must be bounded")
          require(
            Files.readString(identity, StandardCharsets.UTF_8) == CorruptionSentinel,
            "exact corruption fixture required"
          )
        }
      }
      result <- AppModule.streaming[IO](settings).use(_.run).timeout(90.seconds).attempt
      _ <- IO.raiseUnless(result.left.exists(isCheckpointFailure))(
        new IllegalStateException("specific checkpoint failure was not observed")
      )
      _ <- IO.println(s"STREAMING_CHECKPOINT_STARTUP_REJECTED mode=$mode category=CHECKPOINT_IDENTITY")
    } yield ExitCode.Success).handleErrorWith(error =>
      IO.println(s"checkpoint startup proof failed (${error.getClass.getSimpleName})").as(ExitCode.Error)
    )
}
