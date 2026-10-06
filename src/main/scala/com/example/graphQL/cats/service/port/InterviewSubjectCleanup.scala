package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup

type InterviewRetentionBarrier = com.example.graphQL.cats.domain.workflow.InterviewRetentionBarrier
val InterviewRetentionBarrier = com.example.graphQL.cats.domain.workflow.InterviewRetentionBarrier

trait InterviewPublisherFencer {

  /** Success confirms broker fencing for every captured generation, including an otherwise idle producer. */
  def fence(transactionalIds: Vector[String]): RepositoryIO[Unit]
}

enum InterviewCleanupUpdate {
  case Applied, StaleRevision
}

trait InterviewSubjectCleanupRepository {
  def pending: RepositoryIO[Vector[InterviewSubjectCleanup]]
  def find(subject: UserId): RepositoryIO[Option[InterviewSubjectCleanup]]
  def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup): RepositoryIO[InterviewCleanupUpdate]
  def purge(subject: UserId): RepositoryIO[Unit]
  def absent(subject: UserId): RepositoryIO[Boolean]
}
