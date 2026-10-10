package com.example.graphQL.cats.domain.policy

import cats.data.StateT
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Job, JobStatus}
import java.time.Instant

object JobLifecycle {
  def create(job: Job): Either[DomainError, Job] =
    if (job.status == JobStatus.Closed) Left(DomainError.InvalidInitialJobStatus(JobStatus.Closed))
    else Right(job)

  /** Applies the editable content of an already validated job; owner, status and embedding stay as they are. */
  def update(input: Job): LifecycleProgram[Job, Unit] =
    StateT { job =>
      Right(
        (
          job.copy(
            title = input.title,
            description = input.description,
            requirements = input.requirements,
            skills = input.skills,
            location = input.location,
            updatedAt = input.updatedAt
          ),
          ()
        )
      )
    }

  def publish(updatedAt: Instant): LifecycleProgram[Job, Unit] =
    StateT { job =>
      if (job.status == JobStatus.Draft) Right((job.copy(status = JobStatus.Open, updatedAt = updatedAt), ()))
      else Left(DomainError.InvalidJobTransition(job.status, JobStatus.Open))
    }

  def close(updatedAt: Instant): LifecycleProgram[Job, Unit] =
    StateT { job =>
      job.status match {
        case JobStatus.Draft | JobStatus.Open =>
          Right((job.copy(status = JobStatus.Closed, updatedAt = updatedAt, closedAt = Some(updatedAt)), ()))
        case JobStatus.Closed => Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed))
      }
    }
}
