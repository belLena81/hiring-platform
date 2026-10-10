package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.StreamingActivationIdentity
import com.example.hiring.analytics.errors.AnalyticsError

import munit.FunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.file.Files

final class StreamingCheckpointControlSpec extends FunSuite {
  private val identity = StreamingActivationIdentity("stream-a", "source", "lakehouse", "contract", "settings")
  private val other = identity.copy(settingsFingerprint = "other-settings")
  private val fileSystem = FileSystem.getLocal(new Configuration()).getRaw

  private final case class Layout(root: Path, checkpoint: Path, lineage: Path)
  private def layout(): Layout = {
    val root = new Path(Files.createTempDirectory("checkpoint-control").toString)
    Layout(
      root,
      new Path(root, "checkpoint"),
      StreamingCheckpointControl.lineageDirectory(new Path(root, "lake").toString)
    )
  }
  private def validate(l: Layout, id: StreamingActivationIdentity = identity) =
    StreamingCheckpointControl.validateIdentity(fileSystem, l.checkpoint, new Path(l.root, "lake").toString, id)
  private def persist(l: Layout, file: (Path, StreamingActivationIdentity) => Path, value: String) =
    StreamingCheckpointControl.persist(fileSystem, l.lineage, dir => file(dir, identity), Right(value))
  private def establish(l: Layout): Unit = {
    fileSystem.mkdirs(new Path(l.checkpoint, "offsets"))
    fileSystem.mkdirs(new Path(l.checkpoint, "commits"))
    fileSystem.create(new Path(l.checkpoint, "metadata")).close()
    fileSystem.create(new Path(l.checkpoint, "commits/0")).close()
  }

  test("a new checkpoint is claimed once and a different identity is rejected afterwards") {
    val l = layout()
    assertEquals(validate(l), Right(false))
    assertEquals(validate(l), Right(false))
    assertEquals(persist(l, StreamingCheckpointControl.lineagePath, identity.canonical), Right(()))
    assertEquals(validate(l, other), Left(StreamingCheckpointControl.invalidCheckpoint))
  }

  test("an established checkpoint requires its query identity file") {
    val l = layout()
    assertEquals(validate(l), Right(false))
    establish(l)
    assertEquals(validate(l), Left(StreamingCheckpointControl.invalidCheckpoint))
    assertEquals(persist(l, StreamingCheckpointControl.queryIdentityPath, "query-id"), Right(()))
    assertEquals(validate(l), Right(true))
  }

  test("an incomplete checkpoint cannot be established") {
    val l = layout()
    assertEquals(validate(l), Right(false))
    fileSystem.mkdirs(new Path(l.checkpoint, "commits"))
    fileSystem.create(new Path(l.checkpoint, "commits/0")).close()
    assert(validate(l).left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
  }

  test("control files are write-once and must match on retry") {
    val l = layout()
    assertEquals(persist(l, StreamingCheckpointControl.establishedPath, "value"), Right(()))
    assertEquals(persist(l, StreamingCheckpointControl.establishedPath, "value"), Right(()))
    assertEquals(
      persist(l, StreamingCheckpointControl.establishedPath, "changed"),
      Left(StreamingCheckpointControl.invalidCheckpoint)
    )
    val failure = StreamingCheckpointControl.checkpointFailure(new RuntimeException("x"))
    assert(failure.isInstanceOf[AnalyticsError.InvalidConfiguration])
  }
}
