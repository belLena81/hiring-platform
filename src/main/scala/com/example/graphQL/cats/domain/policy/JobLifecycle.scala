package com.example.graphQL.cats.domain.policy

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

  def create(job: Job): Either[DomainError, Job] =
    if (job.status == JobStatus.Closed) Left(DomainError.InvalidInitialJobStatus(JobStatus.Closed))
    else Right(job)

  def update(job: Job, input: Update): Job =
    job.copy(
      title = input.title,
      description = input.description,
      requirements = input.requirements,
      skills = input.skills,
      location = input.location,
      updatedAt = input.updatedAt
    )

  def publish(job: Job, updatedAt: Instant): Either[DomainError, Job] =
    if (job.status == JobStatus.Draft) Right(job.copy(status = JobStatus.Open, updatedAt = updatedAt))
    else Left(DomainError.InvalidJobTransition(job.status, JobStatus.Open))

  def close(job: Job, updatedAt: Instant): Either[DomainError, Job] =
    job.status match {
      case JobStatus.Draft | JobStatus.Open =>
        Right(job.copy(status = JobStatus.Closed, updatedAt = updatedAt, closedAt = Some(updatedAt)))
      case JobStatus.Closed => Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed))
    }
}
