package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import java.time.Instant
import java.util.UUID

final case class InterviewRetentionBarrier(topic: String, partition: Int, endOffset: Long)

object InterviewRetentionBarrier {
  val Topics: Set[String] = Set("hiring.interview-commands", "hiring.interview-results")

  def validate(
      values: Vector[InterviewRetentionBarrier]
  ): Either[InterviewCleanupError, Vector[InterviewRetentionBarrier]] =
    Either.cond(
      values.nonEmpty && values.map(_.topic).toSet == Topics &&
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

  def validate(value: InterviewSubjectCleanup): Either[InterviewCleanupError, InterviewSubjectCleanup] =
    for {
      _ <- Either.cond(
        value.revision >= 0L && value.revision < Long.MaxValue,
        (),
        InterviewCleanupError.InvalidRevision
      )
      ids <- validateProducerIds(value.transactionalIds)
      _ <- value.state match {
        case InterviewCleanupState.AwaitingRetention(barriers) =>
          InterviewRetentionBarrier.validate(barriers).map(_ => ())
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
      now: Instant
  ): Either[InterviewCleanupError, InterviewSubjectCleanup] =
    validate(value).flatMap { current =>
      val next = (current.state, observation) match {
        case (InterviewCleanupState.Pending, InterviewCleanupObservation.ProducersFenced) =>
          Right(InterviewCleanupState.ProducersFenced)
        case (InterviewCleanupState.ProducersFenced, InterviewCleanupObservation.MongoPurged) =>
          Right(InterviewCleanupState.MongoPurged)
        case (InterviewCleanupState.MongoPurged, InterviewCleanupObservation.BarriersCaptured(barriers)) =>
          InterviewRetentionBarrier.validate(barriers).map(InterviewCleanupState.AwaitingRetention.apply)
        case (InterviewCleanupState.AwaitingRetention(_), InterviewCleanupObservation.RetentionPassedAndMongoAbsent) =>
          Right(InterviewCleanupState.Complete(now))
        case _ => Left(InterviewCleanupError.InvalidTransition)
      }
      next.map(state => current.copy(revision = current.revision + 1L, state = state))
    }
}
