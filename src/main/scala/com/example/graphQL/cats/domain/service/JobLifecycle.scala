package com.example.graphQL.cats.domain.service

import cats.data.State
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import java.time.Instant

object JobLifecycle {
  final case class Update(
      title: String,
      description: String,
      requirements: List[String],
      skills: Set[String],
      location: Location,
      updatedAt: Instant
  )

  type Transition = State[Job, Either[DomainError, Job]]

  def create(job: Job): Transition =
    State(_ => (job, Right(job)))

  def update(input: Update): Transition =
    State { job =>
      val updated = job.copy(
        title = input.title,
        description = input.description,
        requirements = input.requirements,
        skills = input.skills,
        location = input.location,
        updatedAt = input.updatedAt
      )
      (updated, Right(updated))
    }

  def publish(updatedAt: Instant): Transition =
    State { job =>
      if (job.status == JobStatus.Draft) {
        val updated = job.copy(status = JobStatus.Open, updatedAt = updatedAt)
        (updated, Right(updated))
      } else {
        (job, Left(DomainError.InvalidJobTransition(job.status, JobStatus.Open)))
      }
    }

  def close(updatedAt: Instant): Transition =
    State { job =>
      job.status match {
        case JobStatus.Draft | JobStatus.Open =>
          val updated = job.copy(status = JobStatus.Closed, updatedAt = updatedAt, closedAt = Some(updatedAt))
          (updated, Right(updated))
        case JobStatus.Closed =>
          (job, Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed)))
      }
    }
}
