package com.example.graphQL.cats.service

import cats.effect.IO

import com.example.graphQL.cats.service.HiringReadService
import com.example.graphQL.cats.service.port.{ApplicationRepository, JobRepository, LexicalJobMatcher, UserRepository}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.TestIdempotency
import com.example.graphQL.cats.service.search.{LexicalMatchLimit, TestEmbeddingWorkPublisher}

private[cats] object TestHiringServices {
  def job(
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      lexicalMatcher: Option[LexicalJobMatcher[IO]] = None,
      lexicalFacetMatchLimit: LexicalMatchLimit = LexicalMatchLimit.Default,
      lexicalFacetDeadline: Option[scala.concurrent.duration.FiniteDuration] = None,
      clock: cats.effect.Clock[cats.effect.IO] = cats.effect.Clock[cats.effect.IO]
  ): JobService[IO] =
    new JobService(
      users,
      jobs,
      TestEmbeddingWorkPublisher.noop,
      TestIdempotency.noop,
      Diagnostics.noop,
      clock,
      cats.effect.std.UUIDGen[IO],
      lexicalMatcher = lexicalMatcher,
      lexicalFacetMatchLimit = lexicalFacetMatchLimit,
      lexicalFacetDeadline = lexicalFacetDeadline
    )

  def applications(
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      applications: ApplicationRepository[IO]
  ): ApplicationService[IO] =
    new ApplicationService(
      users,
      jobs,
      applications,
      HiringReadService(users, jobs, applications),
      TestIdempotency.noop,
      cats.effect.Clock[IO],
      cats.effect.std.UUIDGen[IO]
    )
}
