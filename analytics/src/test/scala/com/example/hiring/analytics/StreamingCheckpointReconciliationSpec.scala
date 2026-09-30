package com.example.hiring.analytics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.adapter.spark.DeltaStreamingCheckpointAcknowledgement
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.streaming.*
import munit.FunSuite

import java.time.Instant

final class StreamingCheckpointReconciliationSpec extends FunSuite {
  private val lineage = StreamingLineage.from("stream-lineage").toOption.get
  private val batchId = StreamingBatchId.from(4L).toOption.get
  private val identity = StreamingBatchIdentity(lineage, batchId)
  private val summary = StreamingPartitionSummary.from("hiring.events", 0, 10L, 12L, 3L).toEither.toOption.get
  private val preparation = StreamingInputPreparation(
    identity,
    Instant.parse("2026-09-30T12:00:00Z"),
    None,
    RangeFingerprint.from("a" * 64).toOption.get,
    Vector(StreamingPartitionEndOffset.from("hiring.events", 0, 15L).toEither.toOption.get),
    Vector(summary)
  )
  private val state = StreamingJournalState(preparation, true, None, None)
  private val batch = StreamingCheckpointBatch(batchId, Map(("hiring.events", 0) -> 15L), committed = false)

  test("an offset record without a Spark commit can recover from its matching durable preparation") {
    val ack = new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(state)))
    ack.reconcile(lineage, Vector(batch), checkpointEstablished = true).unsafeRunSync()
  }

  test("checkpoint offsets may advance past the last delivered record when Kafka offsets contain gaps") {
    val gappedBatch = batch.copy(endOffsets = Map(("hiring.events", 0) -> 15L))
    val ack = new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(state)))
    ack.reconcile(lineage, Vector(gappedBatch), checkpointEstablished = true).unsafeRunSync()

    val tampered = gappedBatch.copy(endOffsets = Map(("hiring.events", 0) -> 150L))
    intercept[Throwable](
      new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(state)))
        .reconcile(lineage, Vector(tampered), checkpointEstablished = true)
        .unsafeRunSync()
    )

    val invalidPrior = StreamingCheckpointBatch(
      StreamingBatchId.from(3L).toOption.get,
      Map(("hiring.events", 0) -> 11L),
      committed = true
    )
    val priorIdentity = StreamingBatchIdentity(lineage, invalidPrior.batchId)
    val priorSummary = StreamingPartitionSummary.from("hiring.events", 0, 8L, 9L, 2L).toEither.toOption.get
    val priorState = state.copy(
      preparation = preparation.copy(
        identity = priorIdentity,
        sourceEndOffsets = Vector(StreamingPartitionEndOffset.from("hiring.events", 0, 11L).toEither.toOption.get),
        deliveredOffsets = Vector(priorSummary)
      ),
      terminalOutcome = Some(StreamingTerminalOutcome.Published)
    )
    val withPriorAck = new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(priorState, state)))
    assert(
      withPriorAck
        .reconcile(lineage, Vector(invalidPrior, batch), checkpointEstablished = true)
        .attempt
        .unsafeRunSync()
        .isLeft
    )
  }

  test("empty batches reconcile only against their exact prepared source end offsets") {
    val emptyPreparation = preparation.copy(
      sourceEndOffsets = Vector(StreamingPartitionEndOffset.from("hiring.events", 0, 20L).toEither.toOption.get),
      deliveredOffsets = Vector.empty
    )
    val emptyState = state.copy(preparation = emptyPreparation)
    val matching = StreamingCheckpointBatch(batchId, Map(("hiring.events", 0) -> 20L), committed = false)
    new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(emptyState)))
      .reconcile(lineage, Vector(matching), checkpointEstablished = true)
      .unsafeRunSync()

    val advanced = matching.copy(endOffsets = Map(("hiring.events", 0) -> 21L))
    intercept[Throwable](
      new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(emptyState)))
        .reconcile(lineage, Vector(advanced), checkpointEstablished = true)
        .unsafeRunSync()
    )
  }

  test("checkpoint reconciliation rejects offset mismatch, orphaned journal state, and nonterminal Spark commits") {
    val mismatched = batch.copy(endOffsets = Map(("hiring.events", 0) -> 14L))
    val mismatchedAck = new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(state)))
    assert(
      mismatchedAck.reconcile(lineage, Vector(mismatched), checkpointEstablished = true).attempt.unsafeRunSync().isLeft
    )

    val missingBatchAck = new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(state)))
    assert(
      missingBatchAck.reconcile(lineage, Vector.empty, checkpointEstablished = true).attempt.unsafeRunSync().isLeft
    )

    val committedAck = new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(state)))
    assert(
      committedAck
        .reconcile(lineage, Vector(batch.copy(committed = true)), checkpointEstablished = true)
        .attempt
        .unsafeRunSync()
        .isLeft
    )
  }

  test("bootstrap rejects an empty checkpoint when durable lineage progress already exists") {
    val terminalState = state.copy(terminalOutcome = Some(StreamingTerminalOutcome.Published))
    val ack = new DeltaStreamingCheckpointAcknowledgement[IO](new Journal(Vector(terminalState)))
    assert(
      ack
        .reconcile(lineage, Vector.empty, checkpointEstablished = false)
        .attempt
        .unsafeRunSync()
        .isLeft
    )
  }

  private final class Journal(states: Vector[StreamingJournalState]) extends StreamingBatchJournal[IO] {
    override def load(identity: StreamingBatchIdentity): IO[Option[StreamingJournalState]] =
      IO.pure(states.find(_.preparation.identity == identity))
    override def latestWatermark(lineage: StreamingLineage): IO[Option[Instant]] = IO.pure(None)
    override def hasLineageState(lineage: StreamingLineage): IO[Boolean] = IO.pure(states.nonEmpty)
    override def reconciliationStates(
        lineage: StreamingLineage,
        retainedBatchIds: Set[StreamingBatchId]
    ): IO[Vector[StreamingJournalState]] =
      IO.pure(states.filter { state =>
        val identity = state.preparation.identity
        identity.lineage == lineage && (retainedBatchIds.contains(identity.batchId) || state.terminalOutcome.isEmpty)
      })
    override def prepare(preparation: StreamingInputPreparation): IO[Unit] = IO.unit
    override def markIngestionCommitted(identity: StreamingBatchIdentity): IO[Unit] = IO.unit
    override def appendDecision(decision: StreamingDecisionRevision): IO[Unit] = IO.unit
    override def complete(
        identity: StreamingBatchIdentity,
        outcome: StreamingTerminalOutcome,
        completedAt: Instant
    ): IO[Unit] =
      IO.unit
    override def commitPublished(decision: StreamingDecisionRevision, completedAt: Instant): IO[Unit] = IO.unit
  }
}
