package com.example.graphQL.cats.domain.policy

import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.error.DomainError.{CandidateRequired, JobMustBeOpen}
import com.example.graphQL.cats.domain.model.Application
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.domain.model.{Job, JobStatus, User, UserRole}
import java.time.Instant

object ApplicationSubmission {
  def create(
      candidate: User,
      job: Job,
      applicationId: ApplicationId,
      now: Instant
  ): Either[DomainError, Application] =
    for {
      _ <- Either.cond(candidate.role == UserRole.Candidate, (), CandidateRequired)
      _ <- Either.cond(job.status == JobStatus.Open, (), JobMustBeOpen)
    } yield Application.create(applicationId, candidate.id, job.id, now)
}
