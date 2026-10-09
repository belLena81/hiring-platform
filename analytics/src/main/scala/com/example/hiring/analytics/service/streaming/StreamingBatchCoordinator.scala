package com.example.hiring.analytics.service.streaming

import cats.effect.Async
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
import com.example.hiring.analytics.service.batch.{AnalyticsReportPublicationReceipt, AnalyticsReportReservation}

import java.time.Instant

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
    candidateWatermark: Option[Instant],
    publicationReservation: AnalyticsReportReservation
)

enum StreamingIngestionResult {
  case Ready(admittedUnsuppressedEventTimes: Vector[Instant])
  case QualityBlocked
  case ErasurePending
}

enum StreamingPublicationResult {
  case Published
  case ErasurePending
  case Superseded
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

  /** Pins the report generation before marker admission or dataset writes for this decision revision. */
  def reservePublication(preparation: StreamingInputPreparation, revision: Long): F[AnalyticsReportReservation]

  /** Detects a report transaction committed before the matching Delta journal completion. */
  def publicationReceipt(
      preparation: StreamingInputPreparation,
      decision: StreamingDecisionRevision
  ): F[AnalyticsReportPublicationReceipt]

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
      _ <- authorizeAcknowledgement *> existing.fold(journal.prepare(preparation))(_ => F.unit)
      result <- existing.flatMap(_.terminalOutcome) match {
        case Some(outcome) => F.pure(resultFor(outcome, existing.flatMap(_.latestDecision)))
        case None          =>
          existing match {
            case Some(state) if state.ingestionCommitted =>
              state.latestDecision
                .traverse(decision => stages.publicationReceipt(stable, decision).map(decision -> _))
                .flatMap {
                  case Some((decision, AnalyticsReportPublicationReceipt.CurrentGeneration)) =>
                    deletionMarkers.activeSubjectTokens.flatMap { markers =>
                      if (
                        markers.isEmpty && fingerprint(markers.map(_.value).sorted.mkString("\n")) ==
                          decision.deletionMarkerFingerprint
                      )
                        F.realTimeInstant.flatMap { completedAt =>
                          (authorizeAcknowledgement *> journal
                            .commitPublished(decision, completedAt))
                            .as(
                              StreamingCoordinatorResult(
                                StreamingTerminalOutcome.Published,
                                decision.candidateWatermark
                              )
                            )
                        }
                      else
                        processUnfinished(
                          stable,
                          isRecoveryAttempt = true,
                          forceDecisionRevision = true,
                          authorize = authorizeAcknowledgement
                        )
                    }
                  case Some((_, AnalyticsReportPublicationReceipt.Superseded)) =>
                    processUnfinished(
                      stable,
                      isRecoveryAttempt = true,
                      forceDecisionRevision = true,
                      authorize = authorizeAcknowledgement
                    )
                  case _ => processUnfinished(stable, existing.nonEmpty, authorize = authorizeAcknowledgement)
                }
            case _ => processUnfinished(stable, existing.nonEmpty, authorize = authorizeAcknowledgement)
          }
      }
      _ <- authorizeAcknowledgement *> checkpoint.callbackMayAcknowledge(preparation.identity)
    } yield result

  private def processUnfinished(
      preparation: StreamingInputPreparation,
      isRecoveryAttempt: Boolean,
      forceDecisionRevision: Boolean = false,
      attempt: Int = 0,
      authorize: F[Unit]
  ): F[StreamingCoordinatorResult] =
    for {
      _ <- authorize
      _ <- F.raiseWhen(attempt >= MaximumDecisionAttempts)(
        AnalyticsError.InvalidConfiguration("streaming publication generation did not stabilize")
      )
      current <- journal.load(preparation.identity)
      previous = current.flatMap(_.latestDecision)
      revision <- F.fromEither(previous.fold[Either[Throwable, Long]](Right(0L)) { value =>
        if (!forceDecisionRevision) Right(value.revision)
        else
          Either.cond(
            value.revision < Long.MaxValue,
            value.revision + 1L,
            AnalyticsError.InvalidInput.one("streaming decision revision is exhausted")
          )
      })
      reservation <- previous
        .filter(_.revision == revision)
        .fold(stages.reservePublication(preparation, revision))(value => F.pure(value.publicationReservation))
      markers <- deletionMarkers.activeSubjectTokens
      assessment <- stages.assess(preparation, markers, isRecoveryAttempt)
      prior <- journal.latestWatermark(preparation.identity.lineage)
      eventTimes = assessment match {
        case StreamingIngestionResult.Ready(times) => times
        case _                                     => Vector.empty
      }
      markerFingerprint = fingerprint(markers.map(_.value).sorted.mkString("\n"))
      // A retry may find its original facts already merged. Keep the durable admission only under
      // the same deletion view and pinned attempt; publication still fences its current generation.
      recoveredCandidate = previous
        .filter(value =>
          isRecoveryAttempt && !forceDecisionRevision && value.revision == revision &&
            value.deletionMarkerFingerprint == markerFingerprint && value.publicationReservation == reservation
        )
        .flatMap(_.candidateWatermark)
      candidate = assessment match {
        case StreamingIngestionResult.Ready(_) if markers.isEmpty =>
          recoveredCandidate.orElse(
            AnalyticsEventTimePolicy.candidateWatermark(prior, eventTimes, preparation.observedAt)
          )
        case _ => None
      }
      decision = StreamingDecisionRevision(
        preparation.identity,
        revision,
        markerFingerprint,
        candidate,
        reservation
      )
      result <- previous match {
        case Some(value) if value.revision == revision && value != decision =>
          processUnfinished(
            preparation,
            isRecoveryAttempt = true,
            forceDecisionRevision = true,
            attempt = attempt + 1,
            authorize = authorize
          )
        case _ =>
          for {
            _ <- authorize *> (if (previous.contains(decision)) F.unit else journal.appendDecision(decision))
            _ <- stages.ingest(preparation, markers, assessment, decision, isRecoveryAttempt)
            _ <- authorize *> journal.markIngestionCommitted(preparation.identity)
            result <- assessment match {
              case StreamingIngestionResult.QualityBlocked =>
                complete(preparation.identity, StreamingTerminalOutcome.QualityBlocked, authorize)
              case StreamingIngestionResult.ErasurePending =>
                complete(preparation.identity, StreamingTerminalOutcome.ErasurePending, authorize)
              case StreamingIngestionResult.Ready(_) =>
                deletionMarkers.activeSubjectTokens.flatMap { refreshed =>
                  if (refreshed.nonEmpty)
                    complete(preparation.identity, StreamingTerminalOutcome.ErasurePending, authorize)
                  else if (
                    fingerprint(refreshed.map(_.value).sorted.mkString("\n")) != decision.deletionMarkerFingerprint
                  )
                    processUnfinished(
                      preparation,
                      isRecoveryAttempt = true,
                      forceDecisionRevision = true,
                      attempt = attempt + 1,
                      authorize = authorize
                    )
                  else publish(preparation, refreshed, decision, attempt, authorize)
                }
            }
          } yield result
      }
    } yield result

  private val MaximumDecisionAttempts = 3

  private def publish(
      preparation: StreamingInputPreparation,
      markers: Vector[SubjectToken],
      decision: StreamingDecisionRevision,
      attempt: Int,
      authorize: F[Unit]
  ): F[StreamingCoordinatorResult] =
    for {
      publication <- stages.publish(preparation, decision, markers)
      result <- publication match {
        case StreamingPublicationResult.Superseded =>
          processUnfinished(
            preparation,
            isRecoveryAttempt = true,
            forceDecisionRevision = true,
            attempt = attempt + 1,
            authorize = authorize
          )
        case StreamingPublicationResult.ErasurePending =>
          complete(preparation.identity, StreamingTerminalOutcome.ErasurePending, authorize)
        case StreamingPublicationResult.Published =>
          F.realTimeInstant.flatMap { completedAt =>
            (authorize *> journal
              .commitPublished(decision, completedAt))
              .as(StreamingCoordinatorResult(StreamingTerminalOutcome.Published, decision.candidateWatermark))
          }
      }
    } yield result

  private def complete(
      identity: StreamingBatchIdentity,
      outcome: StreamingTerminalOutcome,
      authorize: F[Unit]
  ): F[StreamingCoordinatorResult] =
    F.realTimeInstant.flatMap { completedAt =>
      (authorize *> journal.complete(identity, outcome, completedAt)).as(StreamingCoordinatorResult(outcome, None))
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
        AnalyticsError.InvalidInput.one("streaming batch identity was reused with different immutable input evidence")
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
    AnalyticsDigest.sha256Hex(value)
}
