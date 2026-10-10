package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsDigest, StreamingActivationIdentity}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.syntax.all.*
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, Path}

import java.nio.charset.StandardCharsets
import scala.util.Using

/** Synchronous Hadoop-file control for a stream's checkpoint identity and its write-once lineage files; callers run
  * these inside the Spark execution boundary.
  */
private[analytics] object StreamingCheckpointControl {
  private val CheckpointProblem =
    "analytics streaming checkpoint is missing, malformed, or bound to another stream identity"
  val invalidCheckpoint: AnalyticsError.InvalidConfiguration = AnalyticsError.InvalidConfiguration(CheckpointProblem)
  def checkpointFailure(cause: Throwable): AnalyticsError =
    AnalyticsError.InvalidConfiguration(CheckpointProblem, Some(cause))

  def lineageDirectory(lakehouseRoot: String): Path =
    new Path(new Path(SparkPhysicalLocation.resolve(lakehouseRoot)), "control/streaming_lineage")

  def lineagePath(directory: Path, identity: StreamingActivationIdentity): Path =
    controlPath(directory, identity, StreamingLineageFiles.IdentitySuffix)

  def establishedPath(directory: Path, identity: StreamingActivationIdentity): Path =
    controlPath(directory, identity, StreamingLineageFiles.EstablishedSuffix)

  def queryIdentityPath(directory: Path, identity: StreamingActivationIdentity): Path =
    controlPath(directory, identity, StreamingLineageFiles.QueryIdSuffix)

  private def controlPath(directory: Path, identity: StreamingActivationIdentity, suffix: String): Path =
    new Path(directory, s"${AnalyticsDigest.sha256Hex(identity.streamId)}$suffix")

  /** Decides whether the checkpoint is new (`false`) or established (`true`), creating its identity file when new. */
  def validateIdentity(
      fileSystem: FileSystem,
      checkpoint: Path,
      lakehouseRoot: String,
      activationIdentity: StreamingActivationIdentity
  ): Either[AnalyticsError, Boolean] = {
    val lineageDirectory = this.lineageDirectory(lakehouseRoot)
    val identityFile = new Path(checkpoint, "_hiring_stream_identity")
    val expected = activationIdentity.canonical
    val lineageFile = lineagePath(lineageDirectory, activationIdentity)
    val establishedFile = establishedPath(lineageDirectory, activationIdentity)
    val queryIdentityFile = queryIdentityPath(lineageDirectory, activationIdentity)

    def writeNewIdentity(): Either[AnalyticsError, Unit] = writeOnceOrVerify(fileSystem, identityFile, expected)

    def validateEstablishedCheckpoint(): Either[AnalyticsError, Unit] =
      establishedCheckpoint(fileSystem, checkpoint)

    val registeredIdentities = Either
      .catchNonFatal {
        if (!fileSystem.exists(lineageDirectory)) Vector.empty
        else
          fileSystem
            .listStatus(lineageDirectory)
            .filter(_.isFile)
            .filter(_.getPath.getName.endsWith(StreamingLineageFiles.IdentitySuffix))
            .toVector
            .map(status => readText(fileSystem, status.getPath))
      }
      .leftMap(checkpointFailure)
    registeredIdentities.flatMap { identities =>
      val previousIdentityMatches = identities.forall(_ == expected)
      val previouslyRegistered =
        fileSystem.exists(lineageFile) || identities.nonEmpty || fileSystem.exists(establishedFile)
      if (!previousIdentityMatches) Left(invalidCheckpoint)
      else if (!fileSystem.exists(checkpoint)) {
        if (fileSystem.exists(establishedFile)) Left(invalidCheckpoint)
        else if (!fileSystem.mkdirs(checkpoint)) Left(invalidCheckpoint)
        else writeNewIdentity().map(_ => false)
      } else if (previouslyRegistered && !fileSystem.exists(identityFile)) Left(invalidCheckpoint)
      else if (!fileSystem.exists(identityFile)) {
        if (fileSystem.listStatus(checkpoint).nonEmpty) Left(invalidCheckpoint)
        else writeNewIdentity().map(_ => false)
      } else
        writeOnceOrVerify(fileSystem, identityFile, expected).flatMap { _ =>
          val commits = new Path(checkpoint, "commits")
          val hasCommittedBatches = fileSystem.exists(commits) && fileSystem.listStatus(commits).exists(_.isFile)
          val established = fileSystem.exists(establishedFile) || hasCommittedBatches
          val checkpointState = if (!established) Right(()) else validateEstablishedCheckpoint()
          checkpointState
            .flatMap(_ => Either.cond(!established || fileSystem.exists(queryIdentityFile), (), invalidCheckpoint))
            .map(_ => established)
        }
    }
  }

  /** Write-once control file under the lakehouse lineage directory; an existing file must hold the same value. */
  def persist(
      fileSystem: FileSystem,
      directory: Path,
      file: Path => Path,
      expected: Either[AnalyticsError, String]
  ): Either[AnalyticsError, Unit] =
    for {
      value <- expected
      ready <- Either
        .catchNonFatal(fileSystem.mkdirs(directory) || fileSystem.exists(directory))
        .leftMap(checkpointFailure)
      _ <- Either.cond(ready, (), invalidCheckpoint)
      _ <- writeOnceOrVerify(fileSystem, file(directory), value)
    } yield ()

  private def readText(fileSystem: FileSystem, file: Path): String =
    Using.resource(fileSystem.open(file))(input => new String(input.readAllBytes(), StandardCharsets.UTF_8))

  /** Creates the file exclusively; when it already exists, its content must equal the expected value. */
  private def writeOnceOrVerify(fileSystem: FileSystem, file: Path, expected: String): Either[AnalyticsError, Unit] =
    Either
      .catchNonFatal {
        try {
          Using.resource(fileSystem.create(file, false)) { output =>
            output.write(expected.getBytes(StandardCharsets.UTF_8))
            output.hflush()
            output.hsync()
          }
          true
        } catch { case _: FileAlreadyExistsException => readText(fileSystem, file) == expected }
      }
      .leftMap(checkpointFailure)
      .flatMap(Either.cond(_, (), invalidCheckpoint))

  def establishedCheckpoint(fileSystem: FileSystem, checkpoint: Path): Either[AnalyticsError, Unit] =
    Either
      .catchNonFatal(
        fileSystem.getFileStatus(new Path(checkpoint, "metadata")).isFile &&
          fileSystem.getFileStatus(new Path(checkpoint, "offsets")).isDirectory &&
          fileSystem.getFileStatus(new Path(checkpoint, "commits")).isDirectory
      )
      .leftMap(checkpointFailure)
      .flatMap(Either.cond(_, (), invalidCheckpoint))
}
