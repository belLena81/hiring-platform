package com.example.graphQL.cats.domain.workflow.characterization

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import java.time.Instant
import java.util.UUID

/** SAG-01.3 (interview half): `InterviewSubjectCleanup.decide` and `command` for every state and observation. */
class InterviewSubjectCleanupDecisionCharacterizationSpec extends GoldenTableSuite {
  import InterviewCleanupObservation as Observation
  import InterviewCleanupState as State

  private val subject = UserId(UUID.fromString("00000000-0000-0000-0000-0000000000a1"))
  private val requestedAt = Instant.parse("2026-10-01T12:00:00Z")
  private val decidedAt = Instant.parse("2026-10-02T12:00:00Z")
  private val orchestrator = "hiring-interview-orchestrator-00000000-0000-0000-0000-0000000000b1"
  private val worker = "hiring-interview-worker-00000000-0000-0000-0000-0000000000b2"
  private val commandTopic = InterviewTopicPair.Default.commands
  private val resultTopic = InterviewTopicPair.Default.results

  private def barrier(topic: String, partition: Int = 0, endOffset: Long = 7L) =
    InterviewRetentionBarrier(topic, partition, endOffset)

  private val validBarriers = Vector(barrier(commandTopic), barrier(resultTopic, endOffset = 9L))
  private val multiPartitionBarriers =
    Vector(barrier(commandTopic), barrier(commandTopic, 1, 0L), barrier(resultTopic, endOffset = 0L))
  private val invalidBarrierSets: Vector[(String, Vector[InterviewRetentionBarrier])] = Vector(
    "empty" -> Vector.empty,
    "missingResultTopic" -> Vector(barrier(commandTopic)),
    "foreignTopic" -> Vector(barrier(commandTopic), barrier("other.topic")),
    "negativeOffset" -> Vector(barrier(commandTopic), barrier(resultTopic, endOffset = -1L)),
    "negativePartition" -> Vector(barrier(commandTopic), barrier(resultTopic, partition = -1)),
    "duplicatePartition" -> Vector(barrier(commandTopic), barrier(commandTopic), barrier(resultTopic))
  )

  private val states: Vector[(String, InterviewCleanupState)] =
    Vector[(String, InterviewCleanupState)](
      "Pending" -> State.Pending,
      "ProducersFenced" -> State.ProducersFenced,
      "MongoPurged" -> State.MongoPurged,
      "AwaitingRetention(valid)" -> State.AwaitingRetention(validBarriers),
      "AwaitingRetention(multiPartition)" -> State.AwaitingRetention(multiPartitionBarriers),
      "Complete" -> State.Complete(requestedAt)
    ) ++ invalidBarrierSets.map { case (name, set) => s"AwaitingRetention($name)" -> State.AwaitingRetention(set) }

  private val observations: Vector[(String, InterviewCleanupObservation)] =
    Vector[(String, InterviewCleanupObservation)](
      "ProducersFenced" -> Observation.ProducersFenced,
      "MongoPurged" -> Observation.MongoPurged,
      "BarriersCaptured(valid)" -> Observation.BarriersCaptured(validBarriers),
      "BarriersCaptured(multiPartition)" -> Observation.BarriersCaptured(multiPartitionBarriers),
      "RetentionPassedAndMongoAbsent" -> Observation.RetentionPassedAndMongoAbsent
    ) ++ invalidBarrierSets.map { case (name, set) => s"BarriersCaptured($name)" -> Observation.BarriersCaptured(set) }

  /** Subject variants: revision bounds, producer identifier normalization and rejection. */
  private val subjects: Vector[(String, Long, Vector[String])] = Vector(
    ("rev5", 5L, Vector(orchestrator, worker)),
    ("rev0", 0L, Vector(orchestrator, worker)),
    ("revMaxMinus1", Long.MaxValue - 1L, Vector(orchestrator, worker)),
    ("revMax", Long.MaxValue, Vector(orchestrator, worker)),
    ("revNegative", -1L, Vector(orchestrator, worker)),
    ("idsUnsortedDuplicated", 5L, Vector(worker, orchestrator, worker)),
    ("idsEmpty", 5L, Vector.empty),
    ("idsInvalid", 5L, Vector(orchestrator, "hiring-interview-worker-not-a-uuid")),
    ("idsForeignPrefix", 5L, Vector("other-" + worker))
  )

  private val topicPairs: Vector[(String, InterviewTopicPair)] = Vector(
    "defaultTopics" -> InterviewTopicPair.Default,
    "sameTopics" -> InterviewTopicPair("a", "a"),
    "emptyTopic" -> InterviewTopicPair("", "b")
  )

  private def cleanup(revision: Long, ids: Vector[String], state: InterviewCleanupState) =
    InterviewSubjectCleanup(subject, revision, requestedAt, ids, state)

  private def render(result: Either[InterviewCleanupError, InterviewSubjectCleanup]): String = result match {
    case Left(error)  => s"Left($error)"
    case Right(value) => s"Right(rev=${value.revision} ids=${value.transactionalIds} state=${value.state})"
  }

  private val decisions: GoldenSection = {
    val inputs = for {
      (subjectName, revision, ids) <- subjects
      (stateName, state) <- states
      (observationName, observation) <- observations
    } yield (subjectName, revision, ids, stateName, state, observationName, observation)
    GoldenSection.of("InterviewSubjectCleanup.decide subject x state x observation (default topics)", inputs) {
      case (subjectName, _, _, stateName, _, observationName, _) =>
        s"subject=$subjectName state=$stateName observation=$observationName"
    } { case (_, revision, ids, _, state, _, observation) =>
      render(InterviewSubjectCleanup.decide(cleanup(revision, ids, state), observation, decidedAt))
    }
  }

  private val topicVariants: GoldenSection = {
    val inputs = for {
      (topicName, topics) <- topicPairs.tail
      (stateName, state) <- states
      (observationName, observation) <- observations
    } yield (topicName, topics, stateName, state, observationName, observation)
    GoldenSection.of("InterviewSubjectCleanup.decide invalid topic pairs", inputs) {
      case (topicName, _, stateName, _, observationName, _) =>
        s"topics=$topicName state=$stateName observation=$observationName"
    } { case (_, topics, _, state, _, observation) =>
      render(
        InterviewSubjectCleanup.decide(cleanup(5L, Vector(orchestrator, worker), state), observation, decidedAt, topics)
      )
    }
  }

  private val commands: GoldenSection =
    GoldenSection.of("InterviewSubjectCleanup.command by state", states.map(_._2)) { state =>
      s"state=${states.collectFirst { case (name, s) if s == state => name }.get}"
    } { state => InterviewSubjectCleanup.command(cleanup(5L, Vector(orchestrator, worker), state)).toString }

  test("SAG-01.3 generator covers every cleanup state, observation and command") {
    assertEveryCaseGenerated("InterviewCleanupState", GoldenCases.labels[InterviewCleanupState], states.map(_._2))
    assertEveryCaseGenerated(
      "InterviewCleanupObservation",
      GoldenCases.labels[InterviewCleanupObservation],
      observations.map(_._2)
    )
    assertEveryCaseGenerated(
      "InterviewCleanupCommand",
      GoldenCases.labels[InterviewCleanupCommand],
      states.map(_._2).map(state => InterviewSubjectCleanup.command(cleanup(5L, Vector(orchestrator), state)))
    )
    assertEquals(decisions.rows.size, subjects.size * states.size * observations.size)
  }

  test("SAG-01.3 InterviewSubjectCleanup decisions and commands match the committed golden table") {
    assertGolden("interview-subject-cleanup-decisions.txt", Vector(decisions, topicVariants, commands))
  }
}
