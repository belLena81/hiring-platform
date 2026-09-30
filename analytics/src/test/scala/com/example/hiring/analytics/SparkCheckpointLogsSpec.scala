package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.SparkCheckpointLogs
import com.example.hiring.analytics.domain.StreamingBatchId
import munit.FunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.nio.file.Files

final class SparkCheckpointLogsSpec extends FunSuite {
  test("offset and commit contents are decoded as bounded Kafka checkpoint evidence") {
    val root = Files.createTempDirectory("spark-checkpoint-logs-")
    val checkpoint = new Path(root.toUri)
    val fs = checkpoint.getFileSystem(new Configuration())
    val offsets = new Path(checkpoint, "offsets")
    val commits = new Path(checkpoint, "commits")
    fs.mkdirs(offsets)
    fs.mkdirs(commits)
    write(fs, new Path(offsets, "7"), "v1\n{\"batchWatermarkMs\":0}\n{\"hiring.events\":{\"0\":13}}\n")
    write(fs, new Path(commits, "7"), "v1\n{\"nextBatchWatermarkMs\":0,\"stateUniqueIds\":null}\n")

    val result = SparkCheckpointLogs.read(fs, checkpoint, "hiring.events", Set(0))
    assertEquals(
      result.map(_.map(batch => (batch.batchId, batch.endOffsets, batch.committed))),
      Right(Vector((StreamingBatchId.from(7L).toOption.get, Map(("hiring.events", 0) -> 13L), true)))
    )
  }

  test("a commit without its offset log and malformed offset payloads fail closed") {
    val root = Files.createTempDirectory("spark-checkpoint-invalid-")
    val checkpoint = new Path(root.toUri)
    val fs = checkpoint.getFileSystem(new Configuration())
    val offsets = new Path(checkpoint, "offsets")
    val commits = new Path(checkpoint, "commits")
    fs.mkdirs(offsets)
    fs.mkdirs(commits)
    write(fs, new Path(commits, "2"), "v1\n")
    assert(SparkCheckpointLogs.read(fs, checkpoint, "hiring.events", Set(0)).isLeft)

    fs.delete(new Path(commits, "2"), false)
    write(fs, new Path(offsets, "2"), "v1\nnot-json\n")
    assert(SparkCheckpointLogs.read(fs, checkpoint, "hiring.events", Set(0)).isLeft)
  }

  test("partition-set drift and decreasing retained offsets fail closed") {
    val root = Files.createTempDirectory("spark-checkpoint-partitions-")
    val checkpoint = new Path(root.toUri)
    val fs = checkpoint.getFileSystem(new Configuration())
    val offsets = new Path(checkpoint, "offsets")
    val commits = new Path(checkpoint, "commits")
    fs.mkdirs(offsets)
    fs.mkdirs(commits)
    write(fs, new Path(offsets, "8"), "v1\n{}\n{\"hiring.events\":{\"0\":13,\"1\":4}}\n")
    assert(SparkCheckpointLogs.read(fs, checkpoint, "hiring.events", Set(0)).isLeft)

    fs.delete(new Path(offsets, "8"), false)
    write(fs, new Path(offsets, "8"), "v1\n{}\n{\"hiring.events\":{\"0\":13}}\n")
    write(fs, new Path(offsets, "9"), "v1\n{}\n{\"hiring.events\":{\"0\":12}}\n")
    assert(SparkCheckpointLogs.read(fs, checkpoint, "hiring.events", Set(0)).isLeft)
  }

  test("noncanonical numeric partition keys fail closed") {
    val root = Files.createTempDirectory("spark-checkpoint-canonical-partition-")
    val checkpoint = new Path(root.toUri)
    val fs = checkpoint.getFileSystem(new Configuration())
    val offsets = new Path(checkpoint, "offsets")
    val commits = new Path(checkpoint, "commits")
    fs.mkdirs(offsets)
    fs.mkdirs(commits)
    write(fs, new Path(offsets, "0"), "v1\n{}\n{\"hiring.events\":{\"00\":13}}\n")
    assert(SparkCheckpointLogs.read(fs, checkpoint, "hiring.events", Set(0)).isLeft)
  }

  private def write(fs: org.apache.hadoop.fs.FileSystem, path: Path, value: String): Unit = {
    val output = fs.create(path, false)
    try output.write(value.getBytes(StandardCharsets.UTF_8))
    finally output.close()
  }
}
