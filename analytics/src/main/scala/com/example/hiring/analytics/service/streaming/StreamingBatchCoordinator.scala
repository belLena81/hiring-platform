package com.example.hiring.analytics.service.streaming

import cats.effect.Async
import cats.data.NonEmptyChain
import cats.syntax.all.*
import com.example.hiring.analytics.domain.{
  AnalyticsDigest,
  AnalyticsEventTimePolicy,
  RangeFingerprint,
  StreamingBatchId,
  StreamingBatchIdentity,
  StreamingLineage,
  StreamingPartitionEndOffset,
  StreamingPartitionSummary,
  SubjectToken
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.ActiveDeletionMarkerSource

import java.time.Instant
import java.nio.charset.StandardCharsets

/** Immutable evidence captured once for a Spark batch, before any sink is changed. */
final case class StreamingInputPreparation(
    identity: StreamingBatchIdentity,
    observedAt: Instant,
    priorWatermark: Option[Instant],
    inputFingerprint: RangeFingerprint,
    sourceEndOffsets: Vector[StreamingPartitionEndOffset],
    deliveredOffsets: Vector[StreamingPartitionSummary]
)

/** A retry-specific, sanitized decision. Marker identities are never persisted. */
final case class StreamingDecisionRevision(
    identity: StreamingBatchIdentity,
    revision: Long,
    deletionMarkerFingerprint: String,
    candidateWatermark: Option[Instant]
)

enum StreamingIngestionResult {
  case Ready(admittedUnsuppressedEventTimes: Vector[Instant])
  case QualityBlocked
  case ErasurePending
}

enum StreamingPublicationResult {
  case Published
  case ErasurePending
}

enum StreamingTerminalOutcome {
  case QualityBlocked
  case ErasurePending
  case Published
}

/** Durable coordinator journal. `commitPublished` atomically records terminal state and watermark. */
trait StreamingBatchJournal[F[_]] {
  def load(identity: StreamingBatchIdentity): F[Option[StreamingJournalState]]
  def latestWatermark(lineage: StreamingLineage): F[Option[Instant]]
  def hasLineageState(lineage: StreamingLineage): F[Boolean]
  def reconciliationStates(
      lineage: StreamingLineage,
      retainedBatchIds: Set[StreamingBatchId]
  ): F[Vector[StreamingJournalState]]
  def prepare(preparation: StreamingInputPreparation): F[Unit]
  def markIngestionCommitted(identity: StreamingBatchIdentity): F[Unit]
  def appendDecision(decision: StreamingDecisionRevision): F[Unit]
  def complete(identity: StreamingBatchIdentity, outcome: StreamingTerminalOutcome, completedAt: Instant): F[Unit]
  def commitPublished(decision: StreamingDecisionRevision, completedAt: Instant): F[Unit]
}

final case class StreamingJournalState(
    preparation: StreamingInputPreparation,
    ingestionCommitted: Boolean,
    latestDecision: Option[StreamingDecisionRevision],
    terminalOutcome: Option[StreamingTerminalOutcome]
)

trait StreamingBatchStages[F[_]] {

  /** Read-only admission pass. It must preserve source event times across durable-sink retries. */
  def assess(
      preparation: StreamingInputPreparation,
      activeTokens: Vector[SubjectToken],
      isRecoveryAttempt: Boolean
  ): F[StreamingIngestionResult]

  /** Must be idempotent by batch identity and write according to the previously persisted decision. */
  def ingest(
      preparation: StreamingInputPreparation,
      activeTokens: Vector[SubjectToken],
      assessment: StreamingIngestionResult,
      decision: StreamingDecisionRevision,
      isRecoveryAttempt: Boolean
  ): F[Unit]

  /** Rebuilds the shared Gold report and publishes it through the existing generation guard. */
  def publish(
      preparation: StreamingInputPreparation,
      decision: StreamingDecisionRevision,
      activeTokens: Vector[SubjectToken]
  ): F[StreamingPublicationResult]
}

trait StreamingCheckpointAcknowledgement[F[_]] {

  /** Called before foreachBatch returns successfully; it must require a durable terminal journal row. */
  def callbackMayAcknowledge(identity: StreamingBatchIdentity): F[Unit]

  /** Reconciles Spark's actual committed batch IDs against durable terminal journal rows. */
  def reconcile(
      lineage: StreamingLineage,
      checkpointBatches: Vector[StreamingCheckpointBatch],
      checkpointEstablished: Boolean
  ): F[Unit]
}

/** Parsed Spark offset/commit evidence for one micro-batch. Kafka end offsets are exclusive positions. */
final case class StreamingCheckpointBatch(
    batchId: StreamingBatchId,
    endOffsets: Map[(String, Int), Long],
    committed: Boolean
)

final case class StreamingCoordinatorResult(
    outcome: StreamingTerminalOutcome,
    candidateWatermark: Option[Instant]
)

/** Spark-free orchestration for one at-least-once micro-batch. */
final class StreamingBatchCoordinator[F[_]: Async](
    journal: StreamingBatchJournal[F],
    deletionMarkers: ActiveDeletionMarkerSource[F],
    stages: StreamingBatchStages[F],
    checkpoint: StreamingCheckpointAcknowledgement[F]
) {
  private val F = Async[F]

  def process(preparation: StreamingInputPreparation): F[StreamingCoordinatorResult] =
    process(preparation, F.unit)

  def process(
      preparation: StreamingInputPreparation,
      authorizeAcknowledgement: F[Unit]
  ): F[StreamingCoordinatorResult] =
    for {
      existing <- journal.load(preparation.identity)
      stable = existing.fold(preparation)(_.preparation)
      _ <- validateRetry(preparation, existing)
      _ <- existing.fold(journal.prepare(preparation))(_ => F.unit)
      result <- existing.flatMap(_.terminalOutcome) match {
        case Some(outcome) => F.pure(resultFor(outcome, existing.flatMap(_.latestDecision)))
        case None          => processUnfinished(stable, existing.nonEmpty)
      }
      _ <- authorizeAcknowledgement *> checkpoint.callbackMayAcknowledge(preparation.identity)
    } yield result

  private def processUnfinished(
      preparation: StreamingInputPreparation,
      isRecoveryAttempt: Boolean
  ): F[StreamingCoordinatorResult] =
    for {
      markers <- deletionMarkers.activeSubjectTokens
      assessment <- stages.assess(preparation, markers, isRecoveryAttempt)
      decision <- decisionFor(preparation, assessment, markers)
      _ <- stages.ingest(preparation, markers, assessment, decision, isRecoveryAttempt)
      _ <- journal.markIngestionCommitted(preparation.identity)
      result <- assessment match {
        case StreamingIngestionResult.QualityBlocked =>
          complete(preparation.identity, StreamingTerminalOutcome.QualityBlocked)
        case StreamingIngestionResult.ErasurePending =>
          complete(preparation.identity, StreamingTerminalOutcome.ErasurePending)
        case StreamingIngestionResult.Ready(_) =>
          deletionMarkers.activeSubjectTokens.flatMap { refreshedMarkers =>
            if (refreshedMarkers.nonEmpty)
              complete(preparation.identity, StreamingTerminalOutcome.ErasurePending)
            else if (
              fingerprint(refreshedMarkers.map(_.value).sorted.mkString("\n")) == decision.deletionMarkerFingerprint
            )
              publish(preparation, refreshedMarkers, decision)
            else
              stages.assess(preparation, refreshedMarkers, isRecoveryAttempt).flatMap {
                case StreamingIngestionResult.QualityBlocked =>
                  val refreshedDecision =
                    decisionFor(preparation, StreamingIngestionResult.QualityBlocked, refreshedMarkers)
                  refreshedDecision.flatMap(decision =>
                    stages.ingest(
                      preparation,
                      refreshedMarkers,
                      StreamingIngestionResult.QualityBlocked,
                      decision,
                      isRecoveryAttempt
                    )
                  ) *>
                    journal.markIngestionCommitted(preparation.identity) *>
                    complete(preparation.identity, StreamingTerminalOutcome.QualityBlocked)
                case StreamingIngestionResult.ErasurePending =>
                  val refreshedDecision =
                    decisionFor(preparation, StreamingIngestionResult.ErasurePending, refreshedMarkers)
                  refreshedDecision.flatMap(decision =>
                    stages.ingest(
                      preparation,
                      refreshedMarkers,
                      StreamingIngestionResult.ErasurePending,
                      decision,
                      isRecoveryAttempt
                    )
                  ) *>
                    journal.markIngestionCommitted(preparation.identity) *>
                    complete(preparation.identity, StreamingTerminalOutcome.ErasurePending)
                case refreshedAssessment @ StreamingIngestionResult.Ready(_) =>
                  decisionFor(preparation, refreshedAssessment, refreshedMarkers).flatMap { refreshedDecision =>
                    stages.ingest(
                      preparation,
                      refreshedMarkers,
                      refreshedAssessment,
                      refreshedDecision,
                      isRecoveryAttempt
                    ) *>
                      journal.markIngestionCommitted(preparation.identity) *>
                      publish(preparation, refreshedMarkers, refreshedDecision)
                  }
              }
          }
      }
    } yield result

  private def decisionFor(
      preparation: StreamingInputPreparation,
      assessment: StreamingIngestionResult,
      markers: Vector[SubjectToken]
  ): F[StreamingDecisionRevision] = {
    val markerFingerprint = fingerprint(markers.map(_.value).sorted.mkString("\n"))
    journal.load(preparation.identity).flatMap { current =>
      current.flatMap(_.latestDecision) match {
        case Some(existing) if existing.deletionMarkerFingerprint == markerFingerprint => F.pure(existing)
        case _                                                                         =>
          val eventTimes = assessment match {
            case StreamingIngestionResult.Ready(times) => times
            case _                                     => Vector.empty
          }
          for {
            prior <- journal.latestWatermark(preparation.identity.lineage)
            candidate =
              if (markers.nonEmpty) None
              else AnalyticsEventTimePolicy.candidateWatermark(prior, eventTimes, preparation.observedAt)
            revision <- F.fromEither(current.flatMap(_.latestDecision).fold[Either[Throwable, Long]](Right(0L)) {
              previous =>
                Either.cond(
                  previous.revision < Long.MaxValue,
                  previous.revision + 1L,
                  AnalyticsError.InvalidInput(
                    NonEmptyChain.one("streaming decision revision is exhausted")
                  )
                )
            })
            decision = StreamingDecisionRevision(preparation.identity, revision, markerFingerprint, candidate)
            _ <- journal.appendDecision(decision)
          } yield decision
      }
    }
  }

  private def publish(
      preparation: StreamingInputPreparation,
      markers: Vector[SubjectToken],
      decision: StreamingDecisionRevision
  ): F[StreamingCoordinatorResult] =
    for {
      publication <- stages.publish(preparation, decision, markers)
      result <- publication match {
        case StreamingPublicationResult.ErasurePending =>
          complete(preparation.identity, StreamingTerminalOutcome.ErasurePending)
        case StreamingPublicationResult.Published =>
          F.realTimeInstant.flatMap { completedAt =>
            journal
              .commitPublished(decision, completedAt)
              .as(StreamingCoordinatorResult(StreamingTerminalOutcome.Published, decision.candidateWatermark))
          }
      }
    } yield result

  private def complete(
      identity: StreamingBatchIdentity,
      outcome: StreamingTerminalOutcome
  ): F[StreamingCoordinatorResult] =
    F.realTimeInstant.flatMap { completedAt =>
      journal.complete(identity, outcome, completedAt).as(StreamingCoordinatorResult(outcome, None))
    }

  private def validateRetry(
      incoming: StreamingInputPreparation,
      existing: Option[StreamingJournalState]
  ): F[Unit] = existing match {
    case None        => F.unit
    case Some(state) =>
      val stableIncoming = incoming.copy(
        observedAt = state.preparation.observedAt,
        priorWatermark = state.preparation.priorWatermark
      )
      F.raiseWhen(stableIncoming != state.preparation)(
        AnalyticsError.InvalidInput(
          NonEmptyChain.one("streaming batch identity was reused with different immutable input evidence")
        )
      )
  }

  private def resultFor(
      outcome: StreamingTerminalOutcome,
      decision: Option[StreamingDecisionRevision]
  ): StreamingCoordinatorResult =
    StreamingCoordinatorResult(
      outcome,
      if (outcome == StreamingTerminalOutcome.Published) decision.flatMap(_.candidateWatermark) else None
    )

  private def fingerprint(value: String): String =
    AnalyticsDigest.sha256Hex(value.getBytes(StandardCharsets.UTF_8))
}
