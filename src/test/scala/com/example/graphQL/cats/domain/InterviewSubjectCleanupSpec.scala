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
    val completed = initial.copy(state = InterviewCleanupState.Complete(now))
    assertEquals(InterviewSubjectCleanup.command(completed), InterviewCleanupCommand.Finished)
    assertEquals(
      InterviewSubjectCleanup.decide(completed, InterviewCleanupObservation.ProducersFenced, now),
      Left(InterviewCleanupError.InvalidTransition)
    )
  }
}
