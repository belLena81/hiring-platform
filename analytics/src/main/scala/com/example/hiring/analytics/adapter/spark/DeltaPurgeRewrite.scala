package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AccountSubjectId
import cats.effect.{Async, Resource}
import cats.syntax.all.*

import com.example.hiring.analytics.errors.AnalyticsError
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, LinkOption, NoSuchFileException, Paths}
import java.nio.file.attribute.BasicFileAttributes
import scala.jdk.CollectionConverters.*

/** Owns the temporary Delta path used while rewriting retained data files. */
private[analytics] object DeltaPurgeRewrite {
  private val Prefix = "purge-rewrite-"
  private def rejected = AnalyticsError.InvalidConfiguration("temporary Delta rewrite recovery is incomplete or unsafe")

  private final case class Entry(isDirectory: Boolean, isFile: Boolean, isSymlink: Boolean)

  /** Hadoop's local checksum filesystem can follow links even through getFileLinkStatus. */
  private def entry(fileSystem: FileSystem, path: Path): Entry = {
    val qualified = fileSystem.makeQualified(path)
    if (qualified.toUri.getScheme == "file") {
      val attributes = Files.readAttributes(
        Paths.get(qualified.toUri),
        classOf[BasicFileAttributes],
        LinkOption.NOFOLLOW_LINKS
      )
      Entry(attributes.isDirectory, attributes.isRegularFile, attributes.isSymbolicLink)
    } else {
      val status = fileSystem.getFileLinkStatus(path)
      Entry(status.isDirectory, status.isFile, status.isSymlink)
    }
  }

  private def existingEntry(fileSystem: FileSystem, path: Path): Option[Entry] =
    try Some(entry(fileSystem, path))
    catch {
      case _: java.io.FileNotFoundException => None
      case _: NoSuchFileException           => None
    }

  /** The caller owns the lakehouse mutex. A crash requires the documented operator lock recovery first. */
  def recover[F[_]](
      root: String,
      configuration: Configuration,
      maximumEntries: Int,
      execution: SparkExecution[F]
  ): F[Unit] = execution.either {
    inventory(root, configuration, maximumEntries).flatMap { case (fileSystem, paths) =>
      // Delete leaves first without recursive traversal. All names, links and bounds were checked before deletion.
      paths.reverse.traverse_ { path =>
        existingEntry(fileSystem, path) match {
          // Hadoop may remove an inventoried checksum sidecar when its data file is deleted.
          case None                             => Right(())
          case Some(status) if status.isSymlink => Left(rejected)
          case Some(_)                          =>
            fileSystem.delete(path, false)
            Either.cond(existingEntry(fileSystem, path).isEmpty, (), rejected)
        }
      }
    }
  }

  /** Read-only audits reject abandoned copies; maintenance must recover them before a retirement verdict. */
  def verifyRecovered[F[_]](
      root: String,
      configuration: Configuration,
      maximumEntries: Int,
      execution: SparkExecution[F]
  ): F[Unit] = execution.either {
    inventory(root, configuration, maximumEntries).flatMap { case (_, paths) =>
      Either.cond(paths.isEmpty, (), rejected)
    }
  }

  private def inventory(
      root: String,
      configuration: Configuration,
      maximumEntries: Int
  ): Either[AnalyticsError, (FileSystem, Vector[Path])] = {
    val rootPath = new Path(SparkPhysicalLocation.resolve(root))
    val fileSystem = rootPath.getFileSystem(configuration)
    val qualifiedRoot = fileSystem.makeQualified(rootPath)
    val control = new Path(qualifiedRoot, "control")
    val ancestors = Iterator.unfold(Option(control))(_.map(path => path -> Option(path.getParent))).toVector.reverse
    val validRoot = rootPath.isAbsolute && !rootPath.toUri.getPath.split("/").contains("..")
    val safeAncestors = ancestors.forall(path =>
      existingEntry(fileSystem, path)
        .forall(status => !status.isSymlink && status.isDirectory)
    )
    if (maximumEntries <= 0 || maximumEntries == Int.MaxValue || !validRoot || !safeAncestors) Left(rejected)
    else if (!fileSystem.exists(control)) Right(fileSystem -> Vector.empty)
    else {
      def children(directory: Path, remaining: Int): Either[AnalyticsError, Vector[Path]] = {
        val qualified = fileSystem.makeQualified(directory)
        val entries = if (qualified.toUri.getScheme == "file") {
          // Hadoop's local iterator first allocates the entire directory; NIO keeps enumeration bounded.
          val stream = Files.newDirectoryStream(Paths.get(qualified.toUri))
          try
            stream
              .iterator()
              .asScala
              .take(remaining + 1)
              .map(path => new Path(qualified, path.getFileName.toString))
              .toVector
          finally stream.close()
        } else
          Iterator
            .unfold(fileSystem.listStatusIterator(directory)) { iterator =>
              if (iterator.hasNext) Some(iterator.next().getPath -> iterator) else None
            }
            .take(remaining + 1)
            .toVector
        Either.cond(entries.size <= remaining, entries, rejected)
      }
      def recognized(path: Path): Boolean = {
        val name = path.getName
        name.startsWith(Prefix) && AccountSubjectId.from(name.stripPrefix(Prefix)).isRight
      }
      @annotation.tailrec
      def walk(pending: List[Path], remaining: Int, observed: Vector[Path]): Either[AnalyticsError, Vector[Path]] =
        pending match {
          case Nil          => Right(observed)
          case path :: rest =>
            val status = entry(fileSystem, path)
            if (status.isSymlink || (!status.isDirectory && !status.isFile)) Left(rejected)
            else if (status.isDirectory) children(path, remaining) match {
              case Left(error)                                           => Left(error)
              case Right(entries) if entries.exists(_.getParent != path) => Left(rejected)
              case Right(entries) => walk(entries.toList ::: rest, remaining - entries.size, observed :+ path)
            }
            else walk(rest, remaining, observed :+ path)
        }
      for {
        entries <- children(control, maximumEntries)
        candidates = entries.filter(_.getName.startsWith(Prefix))
        _ <- Either.cond(
          entries.forall(_.getParent == control) && candidates.forall(path =>
            recognized(path) && {
              val status = entry(fileSystem, path)
              !status.isSymlink && status.isDirectory
            }
          ),
          (),
          rejected
        )
        // Charge the discovery budget when enqueueing, including siblings waiting behind a nested directory.
        paths <- walk(candidates.toList, maximumEntries - entries.size, Vector.empty)
      } yield fileSystem -> paths
    }
  }

  def temporaryPath[F[_]: Async](spark: SparkSession, temporaryPath: String): Resource[F, Unit] =
    DeltaPurgeRewrite.temporaryPath[F](
      spark,
      temporaryPath,
      SparkBlockingExecution.forTests[F](scala.concurrent.ExecutionContext.parasitic)
    )

  def temporaryPath[F[_]: Async](
      spark: SparkSession,
      temporaryPath: String,
      sparkExecution: SparkExecution[F]
  ): Resource[F, Unit] =
    Resource
      .make(
        sparkExecution {
          val path = new org.apache.hadoop.fs.Path(SparkPhysicalLocation.resolve(temporaryPath))
          (path.getFileSystem(spark.sparkContext.hadoopConfiguration), path)
        }
      ) { case (fileSystem, path) =>
        sparkExecution {
          val removed = fileSystem.delete(path, true)
          if (!removed && fileSystem.exists(path))
            throw new java.io.IOException("temporary purge rewrite path remains")
        }
      }
      .void
}
