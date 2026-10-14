package com.example.graphQL.cats.service

import com.example.graphQL.cats.service.HiringReadService
import com.example.graphQL.cats.service.port.{ApplicationRepository, JobRepository, LexicalJobMatcher, UserRepository}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.TestIdempotency
import com.example.graphQL.cats.service.search.{LexicalMatchLimit, TestEmbeddingWorkPublisher}

private[cats] object TestHiringServices {
  def job(
      users: UserRepository,
      jobs: JobRepository,
      lexicalMatcher: Option[LexicalJobMatcher] = None,
      lexicalFacetMatchLimit: LexicalMatchLimit = LexicalMatchLimit.Default,
      lexicalFacetDeadline: Option[scala.concurrent.duration.FiniteDuration] = None,
      clock: cats.effect.Clock[cats.effect.IO] = cats.effect.Clock[cats.effect.IO]
  ): JobService =
    new JobService(
      users,
      jobs,
      TestEmbeddingWorkPublisher.noop,
      TestIdempotency.noop,
      Diagnostics.noop,
      lexicalMatcher = lexicalMatcher,
      clock = clock,
      lexicalFacetMatchLimit = lexicalFacetMatchLimit,
      lexicalFacetDeadline = lexicalFacetDeadline
    )

  def applications(
      users: UserRepository,
      jobs: JobRepository,
      applications: ApplicationRepository
  ): ApplicationService =
    new ApplicationService(
      users,
      jobs,
      applications,
      HiringReadService(users, jobs, applications),
      TestIdempotency.noop
    )
}
