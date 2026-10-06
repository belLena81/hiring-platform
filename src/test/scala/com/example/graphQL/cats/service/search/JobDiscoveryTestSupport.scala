package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.{ServiceFixtures, TestHiringServices}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.port.{JobRepository, RepositoryIO, UserRepository}

private[cats] object JobDiscoveryTestSupport {
  final case class Calls(nearby: List[NearbyJobsQuery] = Nil, facets: List[JobFacetQuery] = Nil)
  final case class Fixture(service: JobService, users: UserRepository, jobs: JobRepository, calls: Ref[IO, Calls])

  def fixture(users: List[User], values: List[Job] = Nil): IO[Fixture] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](values.map(job => job.id -> job).toMap)
      calls <- Ref.of[IO, Calls](Calls())
      userRepository = new ServiceFixtures.InMemoryUsers(usersRef)
      jobRepository = new RecordingJobs(new ServiceFixtures.InMemoryJobs(jobsRef), calls)
    } yield Fixture(TestHiringServices.job(userRepository, jobRepository), userRepository, jobRepository, calls)

  private final class RecordingJobs(delegate: JobRepository, calls: Ref[IO, Calls]) extends JobRepository {
    export delegate.{
      relatedJobs,
      find,
      findVersioned,
      findMany,
      findOpen,
      findAll,
      findByRecruiter,
      createWithEvents,
      updateWithEvents,
      updateEmbedding
    }

    override def nearbyJobs(query: NearbyJobsQuery, limit: Int): RepositoryIO[List[NearbyJob]] =
      RepositoryIO.lift(calls.update(state => state.copy(nearby = state.nearby :+ query))) *>
        delegate.nearbyJobs(query, limit)

    override def jobDiscoveryFacets(query: JobFacetQuery): RepositoryIO[JobDiscoveryFacets] =
      RepositoryIO.lift(calls.update(state => state.copy(facets = state.facets :+ query))) *>
        delegate.jobDiscoveryFacets(query)
  }
}
