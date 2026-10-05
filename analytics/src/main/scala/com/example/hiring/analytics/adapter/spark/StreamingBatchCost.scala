package com.example.hiring.analytics.adapter.spark

import cats.Applicative
import cats.effect.{Async, Outcome, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.domain.{StreamingBatchId, StreamingBatchIdentity, StreamingLineage, SubjectToken}
import com.example.hiring.analytics.service.batch.{
  AnalyticsLakehouseLock,
  AnalyticsReportPublicationReceipt,
  AnalyticsReportReservation
}
import com.example.hiring.analytics.service.streaming.*
import org.apache.spark.scheduler.*
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

/** Optional diagnostics only: a single aggregate counter, never event payloads or per-task history. */
final class StreamingBatchCost[F[_]: Async] private[analytics] (
    snapshot: F[StreamingBatchCost.Counters],
    emit: StreamingBatchCost.Summary => F[Unit]
) {
  import StreamingBatchCost.*
  private val F = Async[F]

  def timed[A](stage: Stage)(action: => F[A]): F[A] =
    (F.monotonic, snapshot).tupled.flatMap { case (started, before) =>
      F.defer(action).guaranteeCase { outcome =>
        val result = outcome match {
          case Outcome.Succeeded(_) => Result.Succeeded
          case Outcome.Errored(_)   => Result.Failed
          case Outcome.Canceled()   => Result.Cancelled
        }
        (F.monotonic, snapshot).tupled.flatMap { case (ended, after) =>
          // Diagnostic output must never mask the owning operation's result or cancellation.
          emit(Summary(stage, result, ended - started, after.minus(before))).attempt.void
        }
      }
    }

  /** Measure only acquisition; the underlying release remains owned by the returned Resource. */
  def wrapLock(delegate: AnalyticsLakehouseLock[F]): AnalyticsLakehouseLock[F] = new AnalyticsLakehouseLock[F] {
    override def resource(root: String): Resource[F, Unit] =
      Resource
        .makeFull[F, (Unit, F[Unit])] { poll =>
          timed(Stage.MutexWait)(poll(delegate.resource(root).allocated))
        }(_._2)
        .void
  }

  def wrapJournal(delegate: StreamingBatchJournal[F]): StreamingBatchJournal[F] = new StreamingBatchJournal[F] {
    override def load(identity: StreamingBatchIdentity): F[Option[StreamingJournalState]] =
      timed(Stage.JournalLoad)(delegate.load(identity))
    override def latestWatermark(lineage: StreamingLineage): F[Option[java.time.Instant]] =
      timed(Stage.JournalWatermark)(delegate.latestWatermark(lineage))
    override def hasLineageState(lineage: StreamingLineage): F[Boolean] =
      timed(Stage.JournalLineage)(delegate.hasLineageState(lineage))
    override def reconciliationStates(
        lineage: StreamingLineage,
        retainedBatchIds: Set[StreamingBatchId]
    ): F[Vector[StreamingJournalState]] =
      timed(Stage.JournalReconciliation)(delegate.reconciliationStates(lineage, retainedBatchIds))
    override def prepare(preparation: StreamingInputPreparation): F[Unit] =
      timed(Stage.JournalPrepare)(delegate.prepare(preparation))
    override def markIngestionCommitted(identity: StreamingBatchIdentity): F[Unit] =
      timed(Stage.JournalIngestion)(delegate.markIngestionCommitted(identity))
    override def appendDecision(decision: StreamingDecisionRevision): F[Unit] =
      timed(Stage.JournalDecision)(delegate.appendDecision(decision))
    override def complete(
        identity: StreamingBatchIdentity,
        outcome: StreamingTerminalOutcome,
        completedAt: java.time.Instant
    ): F[Unit] =
      timed(Stage.JournalComplete)(delegate.complete(identity, outcome, completedAt))
    override def commitPublished(decision: StreamingDecisionRevision, completedAt: java.time.Instant): F[Unit] =
      timed(Stage.JournalPublication)(delegate.commitPublished(decision, completedAt))
  }

  def wrap(stages: StreamingBatchStages[F]): StreamingBatchStages[F] = new StreamingBatchStages[F] {
    override def reservePublication(
        preparation: StreamingInputPreparation,
        revision: Long
    ): F[AnalyticsReportReservation] =
      timed(Stage.Reserve)(stages.reservePublication(preparation, revision))

    override def publicationReceipt(preparation: StreamingInputPreparation, decision: StreamingDecisionRevision)(using
        Applicative[F]
    ): F[AnalyticsReportPublicationReceipt] = timed(Stage.Receipt)(stages.publicationReceipt(preparation, decision))

    override def assess(
        preparation: StreamingInputPreparation,
        activeTokens: Vector[SubjectToken],
        isRecoveryAttempt: Boolean
    ): F[StreamingIngestionResult] = timed(Stage.Assess)(stages.assess(preparation, activeTokens, isRecoveryAttempt))

    override def ingest(
        preparation: StreamingInputPreparation,
        activeTokens: Vector[SubjectToken],
        assessment: StreamingIngestionResult,
        decision: StreamingDecisionRevision,
        isRecoveryAttempt: Boolean
    ): F[Unit] =
      timed(Stage.Ingest)(stages.ingest(preparation, activeTokens, assessment, decision, isRecoveryAttempt))

    override def publish(
        preparation: StreamingInputPreparation,
        decision: StreamingDecisionRevision,
        activeTokens: Vector[SubjectToken]
    ): F[StreamingPublicationResult] = timed(Stage.Publish)(stages.publish(preparation, decision, activeTokens))
  }
}

object StreamingBatchCost {
  enum Stage {
    case Reserve, Receipt, Assess, Ingest, Publish, Prepare, Maintenance, MutexWait, Callback, HmacValidation,
      JournalLoad, JournalWatermark, JournalLineage, JournalReconciliation, JournalPrepare, JournalIngestion,
      JournalDecision, JournalComplete, JournalPublication
  }
  enum Result { case Succeeded, Failed, Cancelled }

  final case class Counters(
      jobs: Long = 0L,
      stages: Long = 0L,
      tasks: Long = 0L,
      stageMillis: Long = 0L,
      executorMillis: Long = 0L,
      jobMillis: Long = 0L,
      untrackedJobs: Long = 0L,
      inputBytes: Long = 0L,
      inputRows: Long = 0L,
      shuffleReadBytes: Long = 0L,
      shuffleWriteBytes: Long = 0L,
      memorySpillBytes: Long = 0L,
      diskSpillBytes: Long = 0L
  ) {
    def minus(previous: Counters): Counters = Counters(
      jobs - previous.jobs,
      stages - previous.stages,
      tasks - previous.tasks,
      stageMillis - previous.stageMillis,
      executorMillis - previous.executorMillis,
      jobMillis - previous.jobMillis,
      untrackedJobs - previous.untrackedJobs,
      inputBytes - previous.inputBytes,
      inputRows - previous.inputRows,
      shuffleReadBytes - previous.shuffleReadBytes,
      shuffleWriteBytes - previous.shuffleWriteBytes,
      memorySpillBytes - previous.memorySpillBytes,
      diskSpillBytes - previous.diskSpillBytes
    )
  }
  final case class Summary(stage: Stage, result: Result, elapsed: FiniteDuration, work: Counters)

  /** Listener delivery is asynchronous; differences are context-wide diagnostics, not exact stage attribution. */
  def resource[F[_]: Async](spark: SparkSession, emit: Summary => F[Unit]): Resource[F, StreamingBatchCost[F]] = {
    val F = Async[F]
    Resource
      .make(F.delay {
        val totals = new AtomicReference(Counters())
        val activeJobs = new AtomicReference(Map.empty[Int, Long])
        val maximumTrackedJobs = 64
        val listener = new SparkListener {
          override def onJobStart(event: SparkListenerJobStart): Unit = {
            val previous = activeJobs.get()
            if (previous.size < maximumTrackedJobs) activeJobs.set(previous.updated(event.jobId, event.time))
            else { totals.updateAndGet(value => value.copy(untrackedJobs = value.untrackedJobs + 1L)); () }
          }
          override def onJobEnd(event: SparkListenerJobEnd): Unit = {
            val previous = activeJobs.getAndUpdate(_.removed(event.jobId))
            val duration = previous.get(event.jobId).fold(0L)(start => event.time - start)
            totals.updateAndGet(value => value.copy(jobs = value.jobs + 1L, jobMillis = value.jobMillis + duration))
            ()
          }
          override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
            val info = event.stageInfo
            val duration =
              (for { start <- info.submissionTime; end <- info.completionTime } yield end - start).getOrElse(0L)
            totals
              .updateAndGet(value => value.copy(stages = value.stages + 1L, stageMillis = value.stageMillis + duration))
            ()
          }
          override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
            Option(event.taskMetrics).foreach { metrics =>
              totals.updateAndGet(value =>
                value.copy(
                  tasks = value.tasks + 1L,
                  executorMillis = value.executorMillis + metrics.executorRunTime,
                  inputBytes = value.inputBytes + metrics.inputMetrics.bytesRead,
                  inputRows = value.inputRows + metrics.inputMetrics.recordsRead,
                  shuffleReadBytes = value.shuffleReadBytes + metrics.shuffleReadMetrics.totalBytesRead,
                  shuffleWriteBytes = value.shuffleWriteBytes + metrics.shuffleWriteMetrics.bytesWritten,
                  memorySpillBytes = value.memorySpillBytes + metrics.memoryBytesSpilled,
                  diskSpillBytes = value.diskSpillBytes + metrics.diskBytesSpilled
                )
              )
              ()
            }
          }
        }
        spark.sparkContext.addSparkListener(listener)
        (listener, new StreamingBatchCost[F](F.delay(totals.get()), emit))
      }) { case (listener, _) => F.delay(spark.sparkContext.removeSparkListener(listener)) }
      .map(_._2)
  }
}
