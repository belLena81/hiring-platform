package com.example.hiring.analytics.adapter.spark

import cats.effect.{Async, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.errors.AnalyticsError
import org.apache.spark.SparkContext

import java.util.concurrent.{CompletableFuture, Executors, ThreadFactory}
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.UUID
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/** Owns the bounded driver-side execution context used for synchronous Spark, Delta, and Kafka client calls. */
private[analytics] final class SparkBlockingExecution[F[_]] private (
    private val executionContext: ExecutionContext
)(using async: Async[F])
    extends SparkExecution[F] {
  private val sparkContext = new AtomicReference[Option[SparkContext]](None)

  override def apply[A](work: => A): F[A] =
    async.defer {
      sparkContext.get() match {
        case Some(context) => runSpark(context, work)
        case None          => blocking(work)
      }
    }

  /** Evaluates a synchronous non-Spark driver call on the same owned executor. */
  private[analytics] def blocking[A](work: => A): F[A] =
    submit(work, () => ())

  /** Registers Spark's driver context once it has been acquired on this resource. */
  private[analytics] def attachSparkContext(context: SparkContext): F[Unit] =
    async
      .delay(sparkContext.compareAndSet(None, Some(context)) || sparkContext.get().contains(context))
      .ifM(
        async.unit,
        async.raiseError(
          AnalyticsError.InvalidConfiguration("Spark context is already attached to this driver executor")
        )
      )

  private def runSpark[A](context: SparkContext, work: => A): F[A] = {
    val groupId = UUID.randomUUID().toString
    val state = new SparkJobGroupState(context, groupId)
    submit(
      {
        if (state.start())
          try work
          finally state.finish()
        else throw new CancellationException("Spark operation was cancelled before it started")
      },
      () => state.cancel()
    )
  }

  /** Cancellation prevents queued work from starting and retains resource ownership until running work exits. */
  private def submit[A](work: => A, cancelWork: () => Unit): F[A] =
    async.defer {
      val completed = new CompletableFuture[Unit]()
      async.async[A] { callback =>
        val cancelled = new AtomicBoolean(false)
        val workerLock = new Object
        var worker: Thread = null
        val task = new Runnable {
          override def run(): Unit = {
            val current = Thread.currentThread()
            val shouldRun = workerLock.synchronized {
              if (cancelled.get()) false
              else {
                worker = current
                true
              }
            }
            try {
              if (!shouldRun) callback(Left(new CancellationException("Driver operation was cancelled")))
              else {
                val result =
                  try scala.util.Try(work).toEither
                  catch { case interrupted: InterruptedException => Left(interrupted) }
                callback(result)
              }
            } finally {
              Thread.interrupted()
              workerLock.synchronized {
                worker = null
                val _ = completed.complete(())
              }
            }
          }
        }
        try executionContext.execute(task)
        catch { case error: Throwable => callback(Left(error)) }
        val cancel = async
          .delay {
            workerLock.synchronized {
              cancelled.set(true)
              Option(worker).nonEmpty
            }
          }
          .flatMap {
            case false => async.unit
            case true  =>
              async
                .delay(cancelWork())
                .guarantee(async.delay {
                  workerLock.synchronized { Option(worker).foreach(_.interrupt()) }
                })
                .guarantee(async.fromCompletableFuture(async.pure(completed)))
          }
        async.pure(Some(cancel))
      }
    }

  override def either[A](work: => Either[AnalyticsError, A]): F[A] =
    async.flatMap(apply(work))(async.fromEither)

  private[analytics] def isShutdown: Boolean = executionContext match {
    case service: ExecutionContextExecutorService => service.isShutdown
    case _                                        => false
  }

  private final class SparkJobGroupState(context: SparkContext, groupId: String) {
    private var phase: Phase = Phase.Queued

    def start(): Boolean = synchronized {
      phase match {
        case Phase.Queued =>
          context.setJobGroup(groupId, "analytics driver operation", interruptOnCancel = true)
          phase = Phase.Running
          true
        case _ => false
      }
    }

    def finish(): Unit = synchronized {
      try context.clearJobGroup()
      finally if (phase == Phase.Running) phase = Phase.Finished
    }

    def cancel(): Unit = synchronized {
      phase match {
        case Phase.Queued  => phase = Phase.Cancelled
        case Phase.Running =>
          phase = Phase.Cancelled
          context.cancelJobGroup(groupId)
        case Phase.Finished | Phase.Cancelled => ()
      }
    }
  }

  private enum Phase { case Queued, Running, Finished, Cancelled }
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
