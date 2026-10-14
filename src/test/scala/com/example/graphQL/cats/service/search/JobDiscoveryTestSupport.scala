package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.{RepositoryError, ServiceFixtures, TestHiringServices}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.port.{JobRepository, LexicalJobMatcher, RepositoryIO, UserRepository}

private[cats] object JobDiscoveryTestSupport {
  final case class Calls(
      nearby: List[NearbyJobsQuery] = Nil,
      facets: List[JobFacetQuery] = Nil,
      facetIds: List[Option[Set[JobId]]] = Nil,
      facetBudgets: List[Option[scala.concurrent.duration.FiniteDuration]] = Nil
  )
  final case class Fixture(service: JobService, users: UserRepository, jobs: JobRepository, calls: Ref[IO, Calls])

  /** Deterministic lexical index double: returns the highest-scoring window of `limit + 1` hits like mongot would. */
  final class FakeLexicalMatcher(
      scores: Map[JobId, Double],
      outcome: Either[RepositoryError, Unit],
      val calls: Ref[IO, List[(String, JobSearchFilter, Int)]]
  ) extends LexicalJobMatcher {
    override def matches(text: String, filter: JobSearchFilter, limit: Int): RepositoryIO[LexicalJobMatches] =
      RepositoryIO.lift(calls.update(_ :+ ((text, filter, limit)))) *>
        RepositoryIO.fromEither(outcome.map { _ =>
          val window = scores.toList.map(ScoredJobId.apply.tupled).sortBy(hit => (-hit.score, hit.id.value.toString))
          LexicalJobMatches.fromWindow(window.take(limit + 1), limit)
        })
  }

  def matcher(
      scores: Map[JobId, Double],
      outcome: Either[RepositoryError, Unit] = Right(())
  ): IO[FakeLexicalMatcher] =
    Ref.of[IO, List[(String, JobSearchFilter, Int)]](Nil).map(new FakeLexicalMatcher(scores, outcome, _))

  def fixture(
      users: List[User],
      values: List[Job] = Nil,
      lexicalMatcher: Option[LexicalJobMatcher] = None,
      lexicalLimit: Int = LexicalMatchLimit.Default.value,
      deadline: Option[scala.concurrent.duration.FiniteDuration] = None,
      clock: cats.effect.Clock[IO] = cats.effect.Clock[IO]
  ): IO[Fixture] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](values.map(job => job.id -> job).toMap)
      calls <- Ref.of[IO, Calls](Calls())
      userRepository = new ServiceFixtures.InMemoryUsers(usersRef)
      jobRepository = new RecordingJobs(
        new ServiceFixtures.InMemoryJobs(jobsRef, discoveryActors = Some(usersRef)),
        calls
      )
    } yield Fixture(
      TestHiringServices.job(
        userRepository,
        jobRepository,
        lexicalMatcher,
        LexicalMatchLimit.from(lexicalLimit).get,
        deadline,
        clock
      ),
      userRepository,
      jobRepository,
      calls
    )

  private final class RecordingJobs(delegate: JobRepository, calls: Ref[IO, Calls]) extends JobRepository {
    export delegate.{
      relatedJobs,
      find,
      findVersioned,
      findSubmissionSnapshot,
      findOpen,
      findAll,
      findByRecruiter,
      createWithEvents,
      updateWithEvents,
      updateEmbedding
    }

    override def nearbyJobs(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        query: NearbyJobsQuery,
        limit: Int
    ): RepositoryIO[List[NearbyJob]] =
      RepositoryIO.lift(calls.update(state => state.copy(nearby = state.nearby :+ query))) *>
        delegate.nearbyJobs(scope, query, limit)

    override def jobDiscoveryFacets(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        query: JobFacetQuery,
        matchedIds: Option[Set[JobId]],
        maxTime: Option[scala.concurrent.duration.FiniteDuration]
    ): RepositoryIO[JobDiscoveryFacets] =
      RepositoryIO.lift(
        calls.update(state =>
          state.copy(
            facets = state.facets :+ query,
            facetIds = state.facetIds :+ matchedIds,
            facetBudgets = state.facetBudgets :+ maxTime
          )
        )
      ) *>
        delegate.jobDiscoveryFacets(scope, query, matchedIds, maxTime)
  }
}
