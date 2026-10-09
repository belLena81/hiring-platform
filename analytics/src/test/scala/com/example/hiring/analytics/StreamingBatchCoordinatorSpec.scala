package com.example.hiring.analytics

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.{
  ActiveDeletionMarkerSource,
  AnalyticsReportPublicationReceipt,
  AnalyticsReportReservation
}
import com.example.hiring.analytics.service.streaming.*
import munit.CatsEffectSuite

import java.time.Instant

final class StreamingBatchCoordinatorSpec extends CatsEffectSuite {
  private val lineage = right(StreamingLineage.from("hiring-stream"))
  private val identity = StreamingBatchIdentity(lineage, right(StreamingBatchId.from(4L)))
  private val prep = StreamingInputPreparation(
    identity,
    Instant.parse("2026-09-30T12:00:00Z"),
    None,
    right(RangeFingerprint.from("a" * 64)),
    Vector(right(StreamingPartitionEndOffset.from("hiring.events", 0, 11L).toEither.left.map(_.toList.mkString("; ")))),
    Vector(
      right(
        StreamingPartitionSummary.from("hiring.events", 0, 9L, 10L, 2L).toEither.left.map(_.toList.mkString("; "))
      )
    )
  )
  private val reservation = AnalyticsReportReservation(right(RunId.from("stream-test")), prep.inputFingerprint, 0L, 1L)
  private val eventTime = Instant.parse("2026-09-29T12:00:00Z")
  private val expectedWatermark = eventTime.minusSeconds(24L * 60L * 60L)

  private def right[A](value: Either[String, A]): A = value match {
    case Right(result) => result
    case Left(problem) => fail(s"test fixture is invalid: $problem")
  }

  test("publication is generation guarded before atomic watermark commit and callback acknowledgement") {
    for {
      state <- Ref.of[IO, FakeState](FakeState())
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.outcome, StreamingTerminalOutcome.Published)
      assertEquals(result.candidateWatermark, Some(expectedWatermark))
      assertEquals(
        observed.events.takeRight(3),
        Vector("publish", "commit-published", "callback-ack")
      )
      assertEquals(observed.latestWatermark, Some(expectedWatermark))
    }
  }

  test("interrupted publication reuses immutable preparation and the same persisted decision revision") {
    for {
      state <- Ref.of[IO, FakeState](FakeState(failPublishCount = 1))
      harness = new Harness(state)
      first <- harness.coordinator.process(prep).attempt
      afterFirst <- state.get
      retryPreparation = prep.copy(observedAt = prep.observedAt.plusSeconds(30))
      second <- harness.coordinator.process(retryPreparation)
      afterSecond <- state.get
    } yield {
      assert(first.isLeft)
      assertEquals(afterFirst.journal(identity).preparation, prep)
      assert(afterFirst.journal(identity).ingestionCommitted)
      assertEquals(afterFirst.journal(identity).decisions.map(_.revision), Vector(0L))
      assertEquals(second.outcome, StreamingTerminalOutcome.Published)
      assertEquals(afterSecond.journal(identity).preparation, prep)
      assertEquals(afterSecond.journal(identity).decisions.map(_.revision), Vector(0L))
      assertEquals(afterSecond.events.count(_.startsWith("ingest-")), 2)
      assertEquals(afterSecond.events.count(_ == "decision-0"), 1)
      assertEquals(afterSecond.latestWatermark, Some(expectedWatermark))
    }
  }

  test("recovery after a durable Silver write preserves the original eligible candidate under the same fence") {
    val decision = StreamingDecisionRevision(
      identity,
      0L,
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      Some(expectedWatermark),
      reservation
    )
    for {
      state <- Ref.of[IO, FakeState](
        FakeState(
          journal = Map(identity -> JournalEntry(prep, decisions = Vector(decision))),
          ingestionResult = StreamingIngestionResult.Ready(Vector.empty)
        )
      )
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.candidateWatermark, Some(expectedWatermark))
      assertEquals(observed.latestWatermark, Some(expectedWatermark))
      assertEquals(observed.journal(identity).decisions, Vector(decision))
    }
  }

  test("a superseded fence drops the original candidate when fresh assessment has no eligible contribution") {
    val decision = StreamingDecisionRevision(
      identity,
      0L,
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      Some(expectedWatermark),
      reservation
    )
    for {
      state <- Ref.of[IO, FakeState](
        FakeState(
          journal = Map(identity -> JournalEntry(prep, ingestionCommitted = true, decisions = Vector(decision))),
          publicationReceipt = AnalyticsReportPublicationReceipt.Superseded,
          ingestionResult = StreamingIngestionResult.Ready(Vector.empty)
        )
      )
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.candidateWatermark, None)
      assertEquals(observed.latestWatermark, None)
      assertEquals(observed.journal(identity).decisions.last.candidateWatermark, None)
    }
  }

  test("a current Mongo publication receipt completes Delta progress without rebuilding the report") {
    val decision = StreamingDecisionRevision(
      identity,
      0L,
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      Some(expectedWatermark),
      reservation
    )
    val entry = JournalEntry(prep, ingestionCommitted = true, decisions = Vector(decision))
    for {
      state <- Ref.of[IO, FakeState](
        FakeState(
          journal = Map(identity -> entry),
          publicationReceipt = AnalyticsReportPublicationReceipt.CurrentGeneration
        )
      )
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.outcome, StreamingTerminalOutcome.Published)
      assertEquals(result.candidateWatermark, Some(expectedWatermark))
      assert(!observed.events.contains("assess-recovery"))
      assert(!observed.events.contains("publish"))
      assertEquals(observed.events.takeRight(2), Vector("commit-published", "callback-ack"))
      assertEquals(observed.latestWatermark, Some(expectedWatermark))
    }
  }

  test("a superseded publication receipt forces a new guarded report decision") {
    val decision = StreamingDecisionRevision(
      identity,
      0L,
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      Some(expectedWatermark),
      reservation
    )
    val entry = JournalEntry(prep, ingestionCommitted = true, decisions = Vector(decision))
    for {
      state <- Ref.of[IO, FakeState](
        FakeState(
          journal = Map(identity -> entry),
          publicationReceipt = AnalyticsReportPublicationReceipt.Superseded
        )
      )
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.outcome, StreamingTerminalOutcome.Published)
      assertEquals(observed.journal(identity).decisions.map(_.revision), Vector(0L, 1L))
      assert(observed.events.contains("publish"))
      assertEquals(observed.latestWatermark, Some(expectedWatermark))
    }
  }

  test("quality blocked ingestion is terminal and cannot publish or advance the watermark") {
    for {
      state <- Ref.of[IO, FakeState](FakeState(ingestionResult = StreamingIngestionResult.QualityBlocked))
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.outcome, StreamingTerminalOutcome.QualityBlocked)
      assertEquals(result.candidateWatermark, None)
      assertEquals(observed.latestWatermark, None)
      assert(!observed.events.contains("publish"))
      assertEquals(observed.events.takeRight(2), Vector("complete-quality", "callback-ack"))
    }
  }

  test("active deletion markers keep publication hidden and watermark unchanged") {
    val token = right(SubjectToken.fromHmac("test_" + "a" * 43))
    for {
      state <- Ref.of[IO, FakeState](FakeState(markerSnapshots = Vector(Vector(token), Vector(token))))
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.outcome, StreamingTerminalOutcome.ErasurePending)
      assertEquals(result.candidateWatermark, None)
      assertEquals(observed.latestWatermark, None)
      assert(!observed.events.contains("publish"))
    }
  }

  test("reused batch identity with changed immutable input is rejected before effects") {
    val changed = prep.copy(inputFingerprint = right(RangeFingerprint.from("b" * 64)))
    for {
      state <- Ref.of[IO, FakeState](FakeState(journal = Map(identity -> JournalEntry(prep))))
      harness = new Harness(state)
      result <- harness.coordinator.process(changed).attempt
      observed <- state.get
    } yield {
      assert(result.left.exists(_.isInstanceOf[com.example.hiring.analytics.errors.AnalyticsError.InvalidInput]))
      assertEquals(observed.events, Vector.empty)
    }
  }

  test("a marker appearing after ingestion prevents publication and watermark advancement") {
    val token = right(SubjectToken.fromHmac("race_" + "b" * 43))
    for {
      state <- Ref.of[IO, FakeState](
        FakeState(markerSnapshots = Vector(Vector.empty, Vector(token)))
      )
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.outcome, StreamingTerminalOutcome.ErasurePending)
      assertEquals(result.candidateWatermark, None)
      assertEquals(observed.latestWatermark, None)
      assert(!observed.events.contains("publish"))
    }
  }

  test("a changed marker snapshot appends a decision revision before retry ingestion") {
    val token = right(SubjectToken.fromHmac("retry_" + "c" * 43))
    for {
      state <- Ref.of[IO, FakeState](
        FakeState(
          markerSnapshots = Vector(Vector.empty, Vector(token), Vector(token)),
          failIngestAfterWriteCount = 1
        )
      )
      harness = new Harness(state)
      first <- harness.coordinator.process(prep).attempt
      second <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assert(first.isLeft)
      assertEquals(second.outcome, StreamingTerminalOutcome.ErasurePending)
      assertEquals(observed.journal(identity).decisions.map(_.revision), Vector(0L, 1L))
      assertEquals(observed.events.indexOf("decision-1") < observed.events.lastIndexOf("ingest-recovery"), true)
      assert(observed.events.contains("ingest-recovery"))
      assert(!observed.events.contains("publish"))
      assertEquals(observed.latestWatermark, None)
    }
  }

  test("generation drift with unchanged markers re-admits under a new pinned reservation") {
    for {
      state <- Ref.of[IO, FakeState](FakeState(supersedePublishCount = 1))
      harness = new Harness(state)
      result <- harness.coordinator.process(prep)
      observed <- state.get
    } yield {
      assertEquals(result.outcome, StreamingTerminalOutcome.Published)
      assertEquals(result.candidateWatermark, None)
      assertEquals(observed.latestWatermark, None)
      assertEquals(observed.journal(identity).decisions.map(_.revision), Vector(0L, 1L))
      assertEquals(observed.journal(identity).decisions.map(_.publicationReservation.generation), Vector(0L, 1L))
      assert(observed.events.indexOf("reserve-0") < observed.events.indexOf("assess-new"))
    }
  }

  test("grant expiry after publication prevents watermark commit and checkpoint acknowledgement") {
    for {
      state <- Ref.of[IO, FakeState](FakeState(expireWhenPublished = true))
      harness = new Harness(state)
      authorization = state.get.flatMap(value =>
        IO.raiseUnless(value.authorizationValid)(
          com.example.hiring.analytics.errors.AnalyticsError.InvalidConfiguration("activation expired")
        )
      )
      result <- harness.coordinator.process(prep, authorization).attempt
      observed <- state.get
    } yield {
      assert(result.isLeft)
      assertEquals(observed.latestWatermark, None)
      assertEquals(observed.journal(identity).terminal, None)
      assert(!observed.events.contains("callback-ack"))
    }
  }

  test("grant expiry while reconciling a receipt prevents recovered watermark commit") {
    val decision = StreamingDecisionRevision(
      identity,
      0L,
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      Some(expectedWatermark),
      reservation
    )
    for {
      state <- Ref.of[IO, FakeState](
        FakeState(
          journal = Map(identity -> JournalEntry(prep, ingestionCommitted = true, decisions = Vector(decision))),
          publicationReceipt = AnalyticsReportPublicationReceipt.CurrentGeneration,
          expireWhenMarkersRead = true
        )
      )
      harness = new Harness(state)
      authorization = state.get.flatMap(value =>
        IO.raiseUnless(value.authorizationValid)(
          com.example.hiring.analytics.errors.AnalyticsError.InvalidConfiguration("activation expired")
        )
      )
      result <- harness.coordinator.process(prep, authorization).attempt
      observed <- state.get
    } yield {
      assert(result.isLeft)
      assertEquals(observed.latestWatermark, None)
      assertEquals(observed.journal(identity).terminal, None)
      assert(!observed.events.contains("callback-ack"))
    }
  }

  private final case class JournalEntry(
      preparation: StreamingInputPreparation,
      ingestionCommitted: Boolean = false,
      decisions: Vector[StreamingDecisionRevision] = Vector.empty,
      terminal: Option[StreamingTerminalOutcome] = None
  ) {
    def asState: StreamingJournalState =
      StreamingJournalState(preparation, ingestionCommitted, decisions.lastOption, terminal)
  }

  private final case class FakeState(
      journal: Map[StreamingBatchIdentity, JournalEntry] = Map.empty,
      events: Vector[String] = Vector.empty,
      latestWatermark: Option[Instant] = None,
      failPublishCount: Int = 0,
      markerSnapshots: Vector[Vector[SubjectToken]] = Vector(Vector.empty),
      markerReadCount: Int = 0,
      failIngestAfterWriteCount: Int = 0,
      ingestionResult: StreamingIngestionResult = StreamingIngestionResult.Ready(Vector(eventTime)),
      publicationReceipt: AnalyticsReportPublicationReceipt = AnalyticsReportPublicationReceipt.Absent,
      supersedePublishCount: Int = 0,
      generation: Long = 0L,
      authorizationValid: Boolean = true,
      expireWhenMarkersRead: Boolean = false,
      expireWhenPublished: Boolean = false
  )

  private final class Harness(state: Ref[IO, FakeState]) {
    private val journal = new StreamingBatchJournal[IO] {
      override def load(id: StreamingBatchIdentity): IO[Option[StreamingJournalState]] =
        state.get.map(_.journal.get(id).map(_.asState))
      override def latestWatermark(id: StreamingLineage): IO[Option[Instant]] =
        state.get.map(_.latestWatermark)
      override def hasLineageState(id: StreamingLineage): IO[Boolean] =
        state.get.map(_.journal.keysIterator.exists(_.lineage == id))
      override def reconciliationStates(
          id: StreamingLineage,
          retainedBatchIds: Set[StreamingBatchId]
      ): IO[Vector[StreamingJournalState]] =
        state.get.map(
          _.journal.iterator
            .collect {
              case (identity, entry)
                  if identity.lineage == id && (retainedBatchIds
                    .contains(identity.batchId) || entry.terminal.isEmpty) =>
                entry.asState
            }
            .toVector
        )
      override def prepare(preparation: StreamingInputPreparation): IO[Unit] =
        state.update(s => s.copy(journal = s.journal.updated(preparation.identity, JournalEntry(preparation))))
      override def markIngestionCommitted(id: StreamingBatchIdentity): IO[Unit] =
        state.update { s =>
          val entry = s.journal(id)
          s.copy(
            journal = s.journal.updated(id, entry.copy(ingestionCommitted = true)),
            events = s.events :+ "ingestion-committed"
          )
        }
      override def appendDecision(decision: StreamingDecisionRevision): IO[Unit] =
        state.update { s =>
          val entry = s.journal(decision.identity)
          val revised =
            if (entry.decisions.lastOption.contains(decision)) entry.decisions else entry.decisions :+ decision
          s.copy(
            journal = s.journal.updated(decision.identity, entry.copy(decisions = revised)),
            events = s.events :+ s"decision-${decision.revision}"
          )
        }
      override def complete(
          id: StreamingBatchIdentity,
          outcome: StreamingTerminalOutcome,
          completedAt: Instant
      ): IO[Unit] =
        state.update { s =>
          val entry = s.journal(id)
          s.copy(
            journal = s.journal.updated(id, entry.copy(terminal = Some(outcome))),
            events = s.events :+ (outcome match {
              case StreamingTerminalOutcome.QualityBlocked => "complete-quality"
              case _                                       => "complete-erasure"
            })
          )
        }
      override def commitPublished(decision: StreamingDecisionRevision, completedAt: Instant): IO[Unit] =
        state.update { s =>
          val entry = s.journal(decision.identity)
          s.copy(
            journal = s.journal.updated(
              decision.identity,
              entry.copy(terminal = Some(StreamingTerminalOutcome.Published))
            ),
            latestWatermark = decision.candidateWatermark.orElse(s.latestWatermark),
            events = s.events :+ "commit-published"
          )
        }
    }

    private val markers = new ActiveDeletionMarkerSource[IO] {
      override def activeSubjectTokens: IO[Vector[SubjectToken]] = state.modify { current =>
        val index = current.markerReadCount.min(current.markerSnapshots.size - 1)
        current.copy(
          markerReadCount = current.markerReadCount + 1,
          authorizationValid = current.authorizationValid && !current.expireWhenMarkersRead
        ) -> current.markerSnapshots(index)
      }
    }

    private val stages = new StreamingBatchStages[IO] {
      override def reservePublication(
          preparation: StreamingInputPreparation,
          revision: Long
      ): IO[AnalyticsReportReservation] =
        state.modify(s =>
          s.copy(events = s.events :+ s"reserve-$revision") ->
            reservation.copy(generation = s.generation, revision = revision + 1)
        )

      override def publicationReceipt(
          preparation: StreamingInputPreparation,
          decision: StreamingDecisionRevision
      ): IO[AnalyticsReportPublicationReceipt] = state.get.map(_.publicationReceipt)

      override def assess(
          preparation: StreamingInputPreparation,
          activeTokens: Vector[SubjectToken],
          isRecoveryAttempt: Boolean
      ): IO[StreamingIngestionResult] =
        state
          .update(s => s.copy(events = s.events :+ (if (isRecoveryAttempt) "assess-recovery" else "assess-new")))
          .flatMap(_ => state.get.map(_.ingestionResult))

      override def ingest(
          preparation: StreamingInputPreparation,
          activeTokens: Vector[SubjectToken],
          assessment: StreamingIngestionResult,
          decision: StreamingDecisionRevision,
          isRecoveryAttempt: Boolean
      ): IO[Unit] =
        state
          .modify { s =>
            val failed = s.failIngestAfterWriteCount > 0
            s.copy(
              failIngestAfterWriteCount = if (failed) s.failIngestAfterWriteCount - 1 else 0,
              events = s.events :+ (if (isRecoveryAttempt) "ingest-recovery" else "ingest-new")
            ) -> failed
          }
          .flatMap(failed =>
            if (failed) IO.raiseError(new RuntimeException("injected interruption after sink write")) else IO.unit
          )

      override def publish(
          preparation: StreamingInputPreparation,
          decision: StreamingDecisionRevision,
          activeTokens: Vector[SubjectToken]
      ): IO[StreamingPublicationResult] =
        state
          .modify { s =>
            if (s.failPublishCount > 0)
              s.copy(failPublishCount = s.failPublishCount - 1) -> Left(
                new RuntimeException("injected publish interruption")
              )
            else if (s.supersedePublishCount > 0)
              s.copy(
                supersedePublishCount = s.supersedePublishCount - 1,
                generation = s.generation + 1,
                ingestionResult = StreamingIngestionResult.Ready(Vector.empty)
              ) -> Right(StreamingPublicationResult.Superseded)
            else
              s.copy(
                events = s.events :+ "publish",
                authorizationValid = s.authorizationValid && !s.expireWhenPublished
              ) ->
                Right(StreamingPublicationResult.Published)
          }
          .flatMap(_.liftTo[IO])
    }

    private val checkpoint = new StreamingCheckpointAcknowledgement[IO] {
      override def callbackMayAcknowledge(id: StreamingBatchIdentity): IO[Unit] =
        state.update(s => s.copy(events = s.events :+ "callback-ack"))
      override def reconcile(
          lineage: StreamingLineage,
          checkpointBatches: Vector[StreamingCheckpointBatch],
          checkpointEstablished: Boolean
      ): IO[Unit] = IO.unit
    }

    val coordinator = new StreamingBatchCoordinator[IO](journal, markers, stages, checkpoint)
  }
}
