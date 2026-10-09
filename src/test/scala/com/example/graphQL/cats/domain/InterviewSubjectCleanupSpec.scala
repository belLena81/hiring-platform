package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import java.time.Instant
import java.util.UUID
import munit.FunSuite

final class InterviewSubjectCleanupSpec extends FunSuite {
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val subject = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  private val initial = InterviewSubjectCleanup(subject, 0L, now, Vector.empty, InterviewCleanupState.Pending)
  private val barriers = Vector(
    InterviewRetentionBarrier("hiring.interview-commands", 0, 4L),
    InterviewRetentionBarrier("hiring.interview-results", 0, 5L)
  )

  test("cleanup requires fencing, purge, both barriers and retention observation in order") {
    val result = for {
      fenced <- InterviewSubjectCleanup.decide(initial, InterviewCleanupObservation.ProducersFenced, now)
      purged <- InterviewSubjectCleanup.decide(fenced, InterviewCleanupObservation.MongoPurged, now)
      waiting <- InterviewSubjectCleanup.decide(purged, InterviewCleanupObservation.BarriersCaptured(barriers), now)
      complete <- InterviewSubjectCleanup.decide(
        waiting,
        InterviewCleanupObservation.RetentionPassedAndMongoAbsent,
        now
      )
    } yield complete
    assertEquals(result.map(_.revision), Right(4L))
    assertEquals(result.map(_.state), Right(InterviewCleanupState.Complete(now)))
    assertEquals(initial.state, InterviewCleanupState.Pending)
    assertEquals(
      InterviewSubjectCleanup.decide(initial, InterviewCleanupObservation.MongoPurged, now),
      Left(InterviewCleanupError.InvalidTransition)
    )
  }

  test("incomplete, duplicated and negative physical barriers fail closed") {
    val purged = initial.copy(state = InterviewCleanupState.MongoPurged)
    List(
      Vector.empty,
      barriers.take(1),
      barriers :+ barriers.head,
      barriers.updated(0, barriers.head.copy(endOffset = -1L)),
      barriers.updated(0, barriers.head.copy(partition = -1))
    ).foreach { invalid =>
      assertEquals(
        InterviewSubjectCleanup.decide(purged, InterviewCleanupObservation.BarriersCaptured(invalid), now),
        Left(InterviewCleanupError.InvalidBarriers)
      )
    }
  }

  test("producer identity validation isolates interview prefixes and canonical UUIDs") {
    val id = "hiring-interview-worker-00000000-0000-0000-0000-000000000002"
    assertEquals(InterviewSubjectCleanup.validateProducerIds(Vector(id, id)), Right(Vector(id)))
    assertEquals(
      InterviewSubjectCleanup.validateProducerIds(Vector("hiring-publisher-00000000-0000-0000-0000-000000000002")),
      Left(InterviewCleanupError.InvalidProducerIds)
    )
    assertEquals(
      InterviewSubjectCleanup.validateProducerIds(Vector("hiring-interview-worker-1-1-1-1-1")),
      Left(InterviewCleanupError.InvalidProducerIds)
    )
  }

  test("revision exhaustion and completed-state replay cannot produce another cleanup effect") {
    assertEquals(
      InterviewSubjectCleanup
        .decide(initial.copy(revision = Long.MaxValue), InterviewCleanupObservation.ProducersFenced, now),
      Left(InterviewCleanupError.InvalidRevision)
    )
    assertEquals(
      InterviewSubjectCleanup
        .decide(initial.copy(revision = Long.MaxValue - 1L), InterviewCleanupObservation.ProducersFenced, now),
      Left(InterviewCleanupError.InvalidRevision)
    )
    val completed = initial.copy(state = InterviewCleanupState.Complete(now))
    assertEquals(InterviewSubjectCleanup.command(completed), InterviewCleanupCommand.Finished)
    assertEquals(
      InterviewSubjectCleanup.decide(completed, InterviewCleanupObservation.ProducersFenced, now),
      Left(InterviewCleanupError.InvalidTransition)
    )
  }

  test("retention proof records exactly the immutable physical topic pair") {
    val topics = InterviewTopicPair("hiring-test-one.commands", "hiring-test-one.results")
    val actual = Vector(
      InterviewRetentionBarrier(topics.commands, 0, 4L),
      InterviewRetentionBarrier(topics.results, 0, 5L)
    )
    val purged = initial.copy(state = InterviewCleanupState.MongoPurged)
    val waiting =
      InterviewSubjectCleanup.decide(purged, InterviewCleanupObservation.BarriersCaptured(actual), now, topics)
    assertEquals(waiting.map(_.state), Right(InterviewCleanupState.AwaitingRetention(actual)))
    assertEquals(
      waiting.flatMap(InterviewSubjectCleanup.validate(_, InterviewTopicPair.Default)),
      Left(InterviewCleanupError.InvalidBarriers)
    )
    assertEquals(
      InterviewRetentionBarrier.validate(actual, topics.copy(results = topics.commands)),
      Left(InterviewCleanupError.InvalidBarriers)
    )
  }

  test("a live hold is cancelled under the cancel key of its own generation and nothing else is addressable") {
    val id = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-0000000000a1"))
    val other = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-0000000000a2"))
    List(0, 1, 2, 7).foreach { generation =>
      assertEquals(
        InterviewCalendarKeys.cancellationKeyFor(id, InterviewWorkflow.reservationKey(id, generation)),
        Some(InterviewWorkflow.cancellationKey(id, generation))
      )
    }
    // A cancel key, another workflow's key, a non-canonical generation and free text address no reservation.
    assertEquals(InterviewCalendarKeys.cancellationKeyFor(id, InterviewWorkflow.cancellationKey(id, 1)), None)
    assertEquals(InterviewCalendarKeys.cancellationKeyFor(id, InterviewWorkflow.reservationKey(other, 1)), None)
    assertEquals(InterviewCalendarKeys.cancellationKeyFor(id, s"${id.value}:reserve:g01"), None)
    assertEquals(InterviewCalendarKeys.cancellationKeyFor(id, s"${id.value}:reserve:g0"), None)
    assertEquals(InterviewCalendarKeys.cancellationKeyFor(id, ""), None)
  }

  test("confirming holds is part of the purge step: the cleanup states and their order are unchanged") {
    val fenced = initial.copy(state = InterviewCleanupState.ProducersFenced)
    assertEquals(InterviewSubjectCleanup.command(fenced), InterviewCleanupCommand.PurgeMongo)
    assertEquals(
      InterviewSubjectCleanup.decide(fenced, InterviewCleanupObservation.MongoPurged, now).map(_.state),
      Right(InterviewCleanupState.MongoPurged)
    )
    assertEquals(
      InterviewSubjectCleanup.decide(initial, InterviewCleanupObservation.MongoPurged, now),
      Left(InterviewCleanupError.InvalidTransition),
      "a purge can never be observed before the producers are fenced"
    )
  }
}
