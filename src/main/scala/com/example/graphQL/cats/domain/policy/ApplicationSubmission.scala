package com.example.graphQL.cats.domain.policy

import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.error.DomainError.{CandidateRequired, JobMustBeOpen}
import com.example.graphQL.cats.domain.model.Application
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.model.{JobSubmissionSnapshot, JobStatus, UserRole}
import java.time.Instant

object ApplicationSubmission {
  def create(
      candidateId: UserId,
      candidateRole: UserRole,
      job: JobSubmissionSnapshot,
      applicationId: ApplicationId,
      now: Instant
  ): Either[DomainError, Application] =
    for {
      _ <- Either.cond(candidateRole == UserRole.Candidate, (), CandidateRequired)
      _ <- Either.cond(job.status == JobStatus.Open, (), JobMustBeOpen)
    } yield Application.create(applicationId, candidateId, job.id, now)
}
