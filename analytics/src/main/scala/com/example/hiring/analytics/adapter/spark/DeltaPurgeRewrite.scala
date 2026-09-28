package com.example.hiring.analytics.adapter.spark

import cats.effect.{Async, Resource}
import cats.syntax.all.*

import org.apache.spark.sql.SparkSession

/** Owns the temporary Delta path used while rewriting retained data files. */
private[analytics] object DeltaPurgeRewrite {
  def temporaryPath[F[_]: Async](
      spark: SparkSession,
      temporaryPath: String,
      sparkExecution: SparkBlockingExecution[F] =
        SparkBlockingExecution.forTests[F](scala.concurrent.ExecutionContext.parasitic)
  ): Resource[F, Unit] =
    Resource
      .make(
        sparkExecution {
          val path = new org.apache.hadoop.fs.Path(temporaryPath)
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
