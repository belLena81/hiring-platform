package com.example.hiring.analytics.adapter.spark

import cats.effect.{Async, Resource}

import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/** Owns the bounded driver-side execution context used for synchronous Spark and Delta calls. */
private[analytics] final class SparkBlockingExecution[F[_]] private (
    private val executionContext: ExecutionContext
) {
  def apply[A](work: => A)(using Async[F]): F[A] =
    Async[F].evalOn(Async[F].delay(work), executionContext)

  private[analytics] def isShutdown: Boolean = executionContext match {
    case service: ExecutionContextExecutorService => service.isShutdown
    case _                                        => false
  }
}

private[analytics] object SparkBlockingExecution {
  private[analytics] def forTests[F[_]](executionContext: ExecutionContext): SparkBlockingExecution[F] =
    new SparkBlockingExecution[F](executionContext)

  def resource[F[_]: Async]: Resource[F, SparkBlockingExecution[F]] =
    Resource
      .make {
        Async[F].delay {
          val threadFactory = new ThreadFactory {
            override def newThread(runnable: Runnable): Thread = {
              val thread = new Thread(runnable, "analytics-spark-driver")
              thread.setDaemon(true)
              thread
            }
          }
          scala.concurrent.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor(threadFactory))
        }
      }(executionContext => Async[F].delay(executionContext.shutdown()))
      .map(new SparkBlockingExecution[F](_))
}
