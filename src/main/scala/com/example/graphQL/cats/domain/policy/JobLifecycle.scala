package com.example.graphQL.cats.domain.policy

import cats.data.StateT
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Job, JobState, JobStatus}
import java.time.Instant

object JobLifecycle {
  def create(job: Job): Either[DomainError, Job] =
    job.state match {
      case JobState.Closed(_)             => Left(DomainError.InvalidInitialJobStatus(JobStatus.Closed))
      case JobState.Draft | JobState.Open => Right(job)
    }

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
      job.state match {
        case JobState.Draft => Right((job.copy(state = JobState.Open, updatedAt = updatedAt), ()))
        case other          => Left(DomainError.InvalidJobTransition(other.status, JobStatus.Open))
      }
    }

  def close(updatedAt: Instant): LifecycleProgram[Job, Unit] =
    StateT { job =>
      job.state match {
        case JobState.Draft | JobState.Open =>
          Right((job.copy(state = JobState.Closed(updatedAt), updatedAt = updatedAt), ()))
        case JobState.Closed(_) => Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed))
      }
    }
}
