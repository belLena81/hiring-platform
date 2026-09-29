package com.example.graphQL.cats.service

import com.example.graphQL.cats.service.port.{ApplicationRepository, JobRepository, UserRepository}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.TestIdempotency
import com.example.graphQL.cats.service.search.TestEmbeddingWorkPublisher

private[cats] object TestHiringServices {
  def job(users: UserRepository, jobs: JobRepository): JobService =
    new JobService(users, jobs, TestEmbeddingWorkPublisher.noop, TestIdempotency.noop, Diagnostics.noop)

  def applications(
      users: UserRepository,
      jobs: JobRepository,
      applications: ApplicationRepository
  ): ApplicationService =
    new ApplicationService(users, jobs, applications, TestIdempotency.noop)
}
