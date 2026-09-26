package com.example.hiring.analytics.batch

import com.example.hiring.analytics.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import java.nio.channels.{FileChannel, FileLock, OverlappingFileLockException}
import java.nio.file.{Path, Paths, StandardOpenOption}
import java.net.URI
import retry.{RetryPolicies, retryingOnFailures}
import retry.implicits.*
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
    IO.fromEither(localPath(root))
      .flatMap { rootPath =>
        IO.blocking {
          val lockPath = rootPath.resolve("control").resolve("batch.lock")
          java.nio.file.Files.createDirectories(lockPath.getParent)
          FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        }
      }
      .adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
      }

  private def localPath(root: String): Either[AnalyticsError, Path] =
    Either
      .catchNonFatal(URI.create(root))
      .leftMap(_ => AnalyticsError.InvalidConfiguration("the local analytics lakehouse requires a file URI"))
      .flatMap { uri =>
        if (uri.getScheme == null)
          Either
            .catchNonFatal(Paths.get(root))
            .leftMap(_ => AnalyticsError.InvalidConfiguration("the local analytics lakehouse path is invalid"))
        else if (uri.getScheme == "file")
          Either
            .catchNonFatal(Paths.get(uri))
            .leftMap(_ => AnalyticsError.InvalidConfiguration("the local analytics lakehouse path is invalid"))
        else Left(AnalyticsError.InvalidConfiguration("the local analytics lakehouse requires a file URI"))
      }

  private val InitialRetryDelay = 100.millis
  private val MaximumRetryDelay = 5.seconds
  private val AcquisitionTimeout = 2.minutes

  private def acquire(channel: FileChannel): IO[FileLock] = {
    val attempt = IO
      .blocking {
        try Option(channel.tryLock())
        catch {
          case _: OverlappingFileLockException => None
        }
      }
      .adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
      }
    val backoff = RetryPolicies.capDelay(
      MaximumRetryDelay,
      RetryPolicies.fullJitter[IO](InitialRetryDelay)
    )
    val policy = RetryPolicies.limitRetriesByCumulativeDelay(AcquisitionTimeout, backoff)
    retryingOnFailures[Option[FileLock]](
      policy,
      _.isDefined.pure[IO],
      (_, _) => IO.unit
    )(attempt)
      .flatMap {
        case Some(lock) => IO.pure(lock)
        case None       => IO.raiseError(AnalyticsError.LakehouseLockTimeout)
      }
      .timeoutTo(AcquisitionTimeout, IO.raiseError(AnalyticsError.LakehouseLockTimeout))
  }
}
