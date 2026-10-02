package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.adapter.spark.SparkPhysicalLocation
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.{AnalyticsDigest, AnalyticsTopic}
import com.example.hiring.analytics.errors.AnalyticsError
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path as HadoopPath}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, Paths}
import scala.concurrent.duration.*

/** Exercises the production startup path; only the exact checkpoint owner's typed failure passes. */
object StreamingCheckpointStartupProofMain extends IOApp {
  private[cli] val CheckpointFailure =
    "analytics streaming checkpoint is missing, malformed, or bound to another stream identity"
  private[cli] val CorruptionSentinel = "corrupted synthetic checkpoint identity\n"

  private[cli] def foreignIdentity(original: String, expectedStreamId: String): Option[String] = {
    val fields = original.split("\n", -1).toVector
    Option.when(
      fields.size == 5 && fields.forall(value => value.trim.nonEmpty && !value.contains('\r')) &&
        fields.headOption.contains(expectedStreamId)
    )((Vector(expectedStreamId + "-foreign") ++ fields.tail).mkString("\n"))
  }

  private[cli] def isCheckpointFailure(error: Throwable): Boolean = error match {
    case AnalyticsError.InvalidConfiguration(CheckpointFailure) => true
    case _                                                      => false
  }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      mode <- IO.fromOption(args match {
        case List(value @ ("missing" | "corrupt-identity" | "foreign-identity")) => Some(value)
        case _                                                                   => None
      })(new IllegalArgumentException("checkpoint proof mode is invalid"))
      settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
      namespace <- IO.fromEither(
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
          val content = Files.readString(identity, StandardCharsets.UTF_8)
          if (mode == "corrupt-identity") require(content == CorruptionSentinel, "exact corruption fixture required")
          else {
            val repo = namespace.getParent.getParent.getParent
            val configDirectory = repo.resolve(".local/config").resolve(namespace.getFileName)
            val backup = configDirectory.resolve("checkpoint-identity.backup")
            val identityLog = repo.resolve(".local/logs").resolve(namespace.getFileName).resolve("runtime-identity.log")
            Vector(backup, identityLog).foreach { file =>
              require(Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS), "original identity binding is required")
              require(Files.size(file) <= 1024L, "original identity binding must be bounded")
            }
            val original = Files.readString(backup, StandardCharsets.UTF_8)
            val digest = AnalyticsDigest.sha256Hex(original.getBytes(StandardCharsets.UTF_8))
            require(
              Files.readString(identityLog, StandardCharsets.UTF_8).trim == "IDENTITY_DIGEST=" + digest,
              "original checkpoint identity must match the staged runtime"
            )
            require(content == original, "original checkpoint identity must precede the foreign fixture")
            val foreign = foreignIdentity(original, settings.streaming.streamId)
              .getOrElse(throw new IllegalArgumentException("original canonical stream identity is required"))
            val checkpointPath = new HadoopPath(SparkPhysicalLocation.resolve(settings.streaming.checkpointLocation))
            val fileSystem = FileSystem.newInstance(checkpointPath.toUri, new Configuration())
            try {
              val identityPath = new HadoopPath(checkpointPath, "_hiring_stream_identity")
              val output = fileSystem.create(identityPath, true)
              try {
                output.write(foreign.getBytes(StandardCharsets.UTF_8))
                output.hflush()
                output.hsync()
              } finally output.close()
              val input = fileSystem.open(identityPath)
              try
                require(
                  new String(input.readNBytes(1025), StandardCharsets.UTF_8) == foreign,
                  "foreign identity must be readable with native checksum validation"
                )
              finally input.close()
            } finally fileSystem.close()
          }
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
