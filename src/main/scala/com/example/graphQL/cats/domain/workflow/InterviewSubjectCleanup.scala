package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import java.time.Instant
import java.util.UUID

final case class InterviewRetentionBarrier(topic: String, partition: Int, endOffset: Long)

/** Physical log identities are immutable inputs to retention proof, including isolated test logs. */
final case class InterviewTopicPair(commands: String, results: String) {
  def names: Set[String] = Set(commands, results)
  def valid: Boolean = commands.nonEmpty && results.nonEmpty && commands != results
}

object InterviewTopicPair {
  val Default: InterviewTopicPair = InterviewTopicPair("hiring.interview-commands", "hiring.interview-results")
}

object InterviewRetentionBarrier {
  val Topics: Set[String] = InterviewTopicPair.Default.names

  def validate(
      values: Vector[InterviewRetentionBarrier],
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): Either[InterviewCleanupError, Vector[InterviewRetentionBarrier]] =
    Either.cond(
      topics.valid && values.nonEmpty && values.map(_.topic).toSet == topics.names &&
        values.forall(value => value.partition >= 0 && value.endOffset >= 0L) &&
        values.map(value => (value.topic, value.partition)).distinct.size == values.size,
      values,
      InterviewCleanupError.InvalidBarriers
    )
}

enum InterviewCleanupError {
  case InvalidRevision, InvalidProducerIds, InvalidBarriers, InvalidTransition
}

enum InterviewCleanupState {
  case Pending, ProducersFenced, MongoPurged
  case AwaitingRetention(barriers: Vector[InterviewRetentionBarrier])
  case Complete(completedAt: Instant)
}

final case class InterviewSubjectCleanup(
    subjectId: UserId,
    revision: Long,
    requestedAt: Instant,
    transactionalIds: Vector[String],
    state: InterviewCleanupState
)

enum InterviewCleanupCommand {
  case FenceProducers(transactionalIds: Vector[String])
  case PurgeMongo
  case CaptureBarriers
  case AwaitRetention(barriers: Vector[InterviewRetentionBarrier])
  case Finished
}

enum InterviewCleanupObservation {
  case ProducersFenced, MongoPurged
  case BarriersCaptured(barriers: Vector[InterviewRetentionBarrier])
  case RetentionPassedAndMongoAbsent
}

object InterviewSubjectCleanup {
  private val ProducerPrefixes = Vector("hiring-interview-orchestrator-", "hiring-interview-worker-")

  def validateProducerIds(ids: Vector[String]): Either[InterviewCleanupError, Vector[String]] = {
    def valid(id: String): Boolean = ProducerPrefixes.exists { prefix =>
      id.startsWith(prefix) && {
        val suffix = id.substring(prefix.length)
        scala.util.Try(UUID.fromString(suffix)).toOption.exists(_.toString == suffix)
      }
    }
    Either.cond(ids.forall(valid), ids.distinct.sorted, InterviewCleanupError.InvalidProducerIds)
  }

  def validate(
      value: InterviewSubjectCleanup,
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): Either[InterviewCleanupError, InterviewSubjectCleanup] =
    for {
      _ <- Either.cond(topics.valid, (), InterviewCleanupError.InvalidBarriers)
      _ <- Either.cond(
        value.revision >= 0L && value.revision < Long.MaxValue,
        (),
        InterviewCleanupError.InvalidRevision
      )
      ids <- validateProducerIds(value.transactionalIds)
      _ <- value.state match {
        case InterviewCleanupState.AwaitingRetention(barriers) =>
          InterviewRetentionBarrier.validate(barriers, topics).map(_ => ())
        case _ => Right(())
      }
    } yield value.copy(transactionalIds = ids)

  def command(value: InterviewSubjectCleanup): InterviewCleanupCommand = value.state match {
    case InterviewCleanupState.Pending         => InterviewCleanupCommand.FenceProducers(value.transactionalIds)
    case InterviewCleanupState.ProducersFenced => InterviewCleanupCommand.PurgeMongo
    case InterviewCleanupState.MongoPurged     => InterviewCleanupCommand.CaptureBarriers
    case InterviewCleanupState.AwaitingRetention(barriers) => InterviewCleanupCommand.AwaitRetention(barriers)
    case InterviewCleanupState.Complete(_)                 => InterviewCleanupCommand.Finished
  }

  def decide(
      value: InterviewSubjectCleanup,
      observation: InterviewCleanupObservation,
      now: Instant,
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): Either[InterviewCleanupError, InterviewSubjectCleanup] =
    validate(value, topics).flatMap { current =>
      val next = (current.state, observation) match {
        case (InterviewCleanupState.Pending, InterviewCleanupObservation.ProducersFenced) =>
          Right(InterviewCleanupState.ProducersFenced)
        case (InterviewCleanupState.ProducersFenced, InterviewCleanupObservation.MongoPurged) =>
          Right(InterviewCleanupState.MongoPurged)
        case (InterviewCleanupState.MongoPurged, InterviewCleanupObservation.BarriersCaptured(barriers)) =>
          InterviewRetentionBarrier.validate(barriers, topics).map(InterviewCleanupState.AwaitingRetention.apply)
        case (InterviewCleanupState.AwaitingRetention(_), InterviewCleanupObservation.RetentionPassedAndMongoAbsent) =>
          Right(InterviewCleanupState.Complete(now))
        case _ => Left(InterviewCleanupError.InvalidTransition)
      }
      next.flatMap(state => validate(current.copy(revision = current.revision + 1L, state = state), topics))
    }
}
