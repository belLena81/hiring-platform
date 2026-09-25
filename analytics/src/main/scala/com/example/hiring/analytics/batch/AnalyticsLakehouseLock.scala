package com.example.hiring.analytics.batch

import com.example.hiring.analytics.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import java.nio.channels.{FileChannel, FileLock, OverlappingFileLockException}
import java.nio.file.{Path, Paths, StandardOpenOption}
import java.net.URI
import scala.concurrent.duration.*
import scala.util.control.NonFatal

/** Cross-process lock for the local Delta lakehouse. It serializes runs with deletion and VACUUM maintenance. */
private[analytics] object AnalyticsLakehouseLock {
  def resource(root: String): Resource[IO, Unit] =
    Resource
      .make(openChannel(root))(channel => IO.blocking(channel.close()))
      .flatMap { channel =>
        Resource
          .make(acquire(channel))(lock => IO.blocking(lock.release()))
          .void
      }

  private def openChannel(root: String): IO[FileChannel] =
    IO.blocking {
      val rootPath = localPath(root)
      val lockPath = rootPath.resolve("control").resolve("batch.lock")
      java.nio.file.Files.createDirectories(lockPath.getParent)
      FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    }.adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
    }

  private def localPath(root: String): Path = {
    val uri = URI.create(root)
    if (uri.getScheme == null) Paths.get(root)
    else if (uri.getScheme == "file") Paths.get(uri)
    else throw AnalyticsError.InvalidConfiguration("the local analytics lakehouse requires a file URI")
  }

  private def acquire(channel: FileChannel): IO[FileLock] =
    IO.blocking {
      try Option(channel.tryLock())
      catch {
        case _: OverlappingFileLockException => None
      }
    }.adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
    }.flatMap {
      case Some(lock) => IO.pure(lock)
      case None       => IO.sleep(100.millis) *> acquire(channel)
    }
}
