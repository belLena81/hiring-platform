package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.StreamingBatchId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.streaming.StreamingCheckpointBatch

import cats.syntax.all.*
import io.circe.parser.parse
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

/** Reads the durable Structured Streaming offset and commit logs used by recovery reconciliation. */
private[analytics] object SparkCheckpointLogs {
  private val invalidCheckpoint = AnalyticsError.InvalidConfiguration(
    "analytics streaming checkpoint is missing, malformed, or bound to another stream identity"
  )

  def read(
      fileSystem: FileSystem,
      checkpoint: Path,
      expectedTopic: String,
      expectedPartitions: Set[Int]
  ): Either[AnalyticsError, Vector[StreamingCheckpointBatch]] =
    try {
      val offsetsDirectory = new Path(checkpoint, "offsets")
      val commitsDirectory = new Path(checkpoint, "commits")
      val offsetFiles = numericFiles(fileSystem, offsetsDirectory)
      val commitFiles = numericFiles(fileSystem, commitsDirectory)
      val offsetIds = offsetFiles.map(_._1).toSet
      val commitIds = commitFiles.map(_._1).toSet
      if (
        !commitIds.subsetOf(offsetIds) ||
        !continuous(offsetFiles.map(_._1)) ||
        !continuous(commitFiles.map(_._1))
      ) Left(invalidCheckpoint)
      else
        offsetFiles
          .traverse { case (batchId, path) =>
            for {
              id <- StreamingBatchId.from(batchId).left.map(_ => invalidCheckpoint)
              endOffsets <- readOffsets(fileSystem, path, expectedTopic, expectedPartitions)
              _ <-
                if (commitIds.contains(batchId))
                  validateCommit(fileSystem, new Path(commitsDirectory, batchId.toString))
                else Right(())
            } yield StreamingCheckpointBatch(id, endOffsets, commitIds.contains(batchId))
          }
          .flatMap { batches =>
            val monotonic = batches.sliding(2).forall {
              case Vector(previous, current) =>
                current.endOffsets.forall { case (partition, offset) =>
                  previous.endOffsets.get(partition).exists(_ <= offset)
                }
              case _ => true
            }
            Either.cond(monotonic, batches, invalidCheckpoint)
          }
    } catch {
      case NonFatal(_) => Left(invalidCheckpoint)
    }

  private def numericFiles(fileSystem: FileSystem, directory: Path): Vector[(Long, Path)] =
    if (!fileSystem.exists(directory)) Vector.empty
    else
      fileSystem
        .listStatus(directory)
        .filter(_.isFile)
        .filterNot(_.getPath.getName.startsWith("."))
        .toVector
        .map { status =>
          val name = status.getPath.getName
          val id = name.toLong
          if (id < 0L || id.toString != name) throw new IllegalArgumentException("invalid batch log name")
          id -> status.getPath
        }
        .sortBy(_._1)

  private def continuous(batchIds: Vector[Long]): Boolean =
    batchIds.sliding(2).forall {
      case Vector(previous, next) => previous < Long.MaxValue && next == previous + 1L
      case _                      => true
    }

  private def readOffsets(
      fileSystem: FileSystem,
      path: Path,
      expectedTopic: String,
      expectedPartitions: Set[Int]
  ): Either[AnalyticsError, Map[(String, Int), Long]] =
    for {
      lines <- readLines(fileSystem, path)
      _ <- Either.cond(lines.length >= 2 && lines.head == "v1", (), invalidCheckpoint)
      jsonLines <- lines.tail.traverse(line => parse(line).left.map(_ => invalidCheckpoint))
      offsets <- jsonLines.lastOption.toRight(invalidCheckpoint).flatMap { json =>
        json.asObject.toRight(invalidCheckpoint).flatMap { topics =>
          Either
            .cond(topics.keys.toSet == Set(expectedTopic), topics(expectedTopic), invalidCheckpoint)
            .flatMap(_.flatMap(_.asObject).toRight(invalidCheckpoint))
            .flatMap { partitions =>
              partitions.toVector
                .traverse { case (partitionName, value) =>
                  for {
                    partition <- partitionName.toIntOption
                      .filter(partition => partition >= 0 && partition.toString == partitionName)
                      .toRight(invalidCheckpoint)
                    offset <- value.asNumber.flatMap(_.toLong).filter(_ >= 0L).toRight(invalidCheckpoint)
                  } yield (expectedTopic -> partition) -> offset
                }
                .map(_.toMap)
                .flatMap { offsets =>
                  val actualPartitions = offsets.keysIterator.map(_._2).toSet
                  Either.cond(actualPartitions == expectedPartitions, offsets, invalidCheckpoint)
                }
            }
        }
      }
    } yield offsets

  private def validateCommit(fileSystem: FileSystem, path: Path): Either[AnalyticsError, Unit] =
    readLines(fileSystem, path).flatMap { lines =>
      val validMetadata = lines match {
        case Vector("v1")       => true
        case Vector("v1", json) =>
          parse(json).toOption.flatMap(_.asObject).exists { metadata =>
            metadata("nextBatchWatermarkMs").flatMap(_.asNumber).flatMap(_.toLong).nonEmpty &&
            metadata.keys.forall(Set("nextBatchWatermarkMs", "stateUniqueIds"))
          }
        case _ => false
      }
      Either.cond(validMetadata, (), invalidCheckpoint)
    }

  private def readLines(fileSystem: FileSystem, path: Path): Either[AnalyticsError, Vector[String]] =
    Either
      .catchNonFatal {
        val input = fileSystem.open(path)
        try
          new String(input.readAllBytes(), StandardCharsets.UTF_8)
            .split("\\r?\\n")
            .toVector
            .map(_.trim)
            .filter(_.nonEmpty)
        finally input.close()
      }
      .left
      .map(_ => invalidCheckpoint)
}
