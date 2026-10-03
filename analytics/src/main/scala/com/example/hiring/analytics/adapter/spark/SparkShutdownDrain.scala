package com.example.hiring.analytics.adapter.spark

import cats.effect.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.errors.AnalyticsError
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.*

import java.util.UUID
import java.util.concurrent.CompletableFuture
import scala.concurrent.duration.*

/** Bounded drain of observed task attempts, not a guarantee that Spark's internal queues are empty. Spark can drop
  * listener events. Actual process proofs therefore also reject ERROR/drop logs.
  */
private[analytics] final class SparkShutdownDrain private (
    context: SparkContext,
    private[analytics] val tracker: SparkShutdownDrain.Tracker,
    listener: SparkListener,
    marker: String
) {
  import SparkShutdownDrain.*

  private[analytics] def await[F[_]: Async](execution: SparkBlockingExecution[F]): F[Unit] = {
    val control = execution {
      withBarrierProperty(context, marker) {
        // One real, in-memory task: no SQL, Kafka, Delta, Mongo, or external I/O.
        val count = context.parallelize(Vector(1), 1).count()
        if (count != 1L) throw new IllegalStateException("Spark shutdown control task returned an invalid count")
      }
    }
    awaitKnownTasks(tracker, control, Deadline)
  }

  private[analytics] def remove(): Unit = context.removeSparkListener(listener)
}

private[analytics] object SparkShutdownDrain {
  private[analytics] val BarrierProperty = "hiring.analytics.shutdown.control"
  private val Deadline = 30.seconds
  private val MaximumActiveAttempts = 65536

  private[analytics] final case class Attempt(stageId: Int, stageAttemptId: Int, taskId: Long)

  private def failure(category: String): AnalyticsError =
    AnalyticsError.LakehouseFailure(new IllegalStateException(s"Spark shutdown drain failed: $category"))

  /** Synchronized mutable state is confined to Spark's listener callback boundary. */
  private[analytics] final class Tracker(maximumAttempts: Int) {
    require(maximumAttempts > 0)
    private var active = Set.empty[Attempt]
    private var barrierStarted = false
    private var barrierJob = Option.empty[Int]
    private var barrierStages = Set.empty[Int]
    private var barrierTasks = 0
    private var barrierEnded = false
    private var problem = Option.empty[AnalyticsError]
    private var changed = new CompletableFuture[Unit]()

    private def updated(): Unit = {
      changed.complete(())
      changed = new CompletableFuture[Unit]()
    }
    private def reject(category: String): Unit =
      if (problem.isEmpty) problem = Some(failure(category))

    def begin(): Unit = synchronized {
      if (barrierStarted) reject("duplicate-control")
      barrierStarted = true
      updated()
    }

    def taskStarted(attempt: Attempt): Unit = synchronized {
      if (active.contains(attempt)) reject("duplicate-task-start")
      else if (active.size >= maximumAttempts) reject("attempt-bound")
      else active = active + attempt
      if (barrierStages.contains(attempt.stageId)) {
        barrierTasks += 1
        if (barrierTasks > 1) reject("control-task-count")
      }
      updated()
    }

    def taskEnded(attempt: Attempt): Unit = synchronized {
      if (!active.contains(attempt)) reject("unmatched-task-end")
      else active = active - attempt
      updated()
    }

    def controlStarted(jobId: Int, stages: Set[Int]): Unit = synchronized {
      if (!barrierStarted || barrierJob.nonEmpty || stages.size != 1) reject("control-job-identity")
      else {
        barrierJob = Some(jobId)
        barrierStages = stages
      }
      updated()
    }

    def jobEnded(jobId: Int, succeeded: Boolean): Unit = synchronized {
      if (barrierJob.contains(jobId)) {
        if (barrierEnded || !succeeded || barrierTasks != 1) reject("control-job-result")
        barrierEnded = true
      }
      updated()
    }

    def snapshot(): (Either[AnalyticsError, Boolean], CompletableFuture[Unit]) = synchronized {
      (problem.toLeft(barrierEnded && active.isEmpty), changed)
    }
  }

  private[analytics] def withBarrierProperty[A](context: SparkContext, marker: String)(work: => A): A = {
    val previous = context.getLocalProperty(BarrierProperty)
    context.setLocalProperty(BarrierProperty, marker)
    try work
    finally context.setLocalProperty(BarrierProperty, previous)
  }

  private[analytics] def awaitKnownTasks[F[_]: Async](
      tracker: Tracker,
      control: F[Unit],
      deadline: FiniteDuration
  ): F[Unit] = {
    val F = Async[F]
    def awaitObserved: F[Unit] = F.defer {
      val (status, changed) = tracker.snapshot()
      status match {
        case Left(error)  => F.raiseError(error)
        case Right(true)  => F.unit
        case Right(false) => F.fromCompletableFuture(F.pure(changed)) *> awaitObserved
      }
    }
    (F.delay(tracker.begin()) *> control *> awaitObserved).timeoutTo(deadline, F.raiseError(failure("deadline")))
  }

  /** Installed before application Spark jobs; retained until the original context has stopped. */
  private[analytics] def install(context: SparkContext): SparkShutdownDrain = {
    val marker = UUID.randomUUID().toString
    val tracker = new Tracker(MaximumActiveAttempts)
    val listener = new SparkListener {
      override def onTaskStart(event: SparkListenerTaskStart): Unit =
        tracker.taskStarted(Attempt(event.stageId, event.stageAttemptId, event.taskInfo.taskId))
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
        tracker.taskEnded(Attempt(event.stageId, event.stageAttemptId, event.taskInfo.taskId))
      override def onJobStart(event: SparkListenerJobStart): Unit =
        if (Option(event.properties).exists(_.getProperty(BarrierProperty) == marker))
          tracker.controlStarted(event.jobId, event.stageInfos.iterator.map(_.stageId).toSet)
      override def onJobEnd(event: SparkListenerJobEnd): Unit =
        tracker.jobEnded(event.jobId, event.jobResult == JobSucceeded)
    }
    context.addSparkListener(listener)
    new SparkShutdownDrain(context, tracker, listener, marker)
  }

  /** Stop/remove are attempted even after a drain failure; every failure remains observable. */
  private[analytics] def close[F[_]: Async](drain: F[Unit], stop: F[Unit], remove: F[Unit]): F[Unit] =
    Async[F].uncancelable { _ =>
      for {
        drained <- drain.attempt
        stopped <- stop.attempt
        removed <- remove.attempt
        _ <-
          (stopped.left.toOption.toVector ++ drained.left.toOption.toVector ++ removed.left.toOption.toVector).headOption
            .fold(Async[F].unit) { primary =>
              val others =
                (stopped.left.toOption.toVector ++ drained.left.toOption.toVector ++ removed.left.toOption.toVector)
                  .filterNot(_ eq primary)
              Async[F].delay(others.foreach(primary.addSuppressed)) *> Async[F].raiseError[Unit](primary)
            }
      } yield ()
    }
}
