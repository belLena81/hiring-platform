package com.example.hiring.analytics.adapter.spark

import cats.effect.{Async, Resource}
import com.example.hiring.analytics.errors.AnalyticsError

import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/** Owns the bounded driver-side execution context used for synchronous Spark and Delta calls. */
private[analytics] final class SparkBlockingExecution[F[_]] private (
    private val executionContext: ExecutionContext
)(using async: Async[F])
    extends SparkExecution[F] {
  override def apply[A](work: => A): F[A] =
    async.evalOn(async.delay(work), executionContext)

  override def either[A](work: => Either[AnalyticsError, A]): F[A] =
    async.flatMap(apply(work))(async.fromEither)

  private[analytics] def isShutdown: Boolean = executionContext match {
    case service: ExecutionContextExecutorService => service.isShutdown
    case _                                        => false
  }
}

private[analytics] object SparkBlockingExecution {
  private[analytics] def forTests[F[_]: Async](executionContext: ExecutionContext): SparkBlockingExecution[F] =
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
