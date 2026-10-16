package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.{Deferred, IO, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.{
  ActorContext,
  Diagnostics,
  HiringReadService,
  LogEvent,
  LogField,
  RepositoryError,
  ServiceFixtures,
  UseCaseError
}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.events.{OperationalEventPayload, OperationalTelemetryService}
import com.example.graphQL.cats.service.mutation.TestIdempotency
import com.example.graphQL.cats.service.port.{
  SearchSessionLookup,
  SearchSessionRepository,
  TestSearchSessionWorkRepository
}
import com.example.graphQL.cats.service.port.{RepositoryF, SearchSessionWorkRepository}
import com.example.graphQL.cats.service.protocol.{SearchUseCases, UseCaseF}
import com.example.graphQL.cats.service.search.{
  CandidateMatchFilters,
  JobSearchFilter,
  RankedJob,
  SearchResults,
  SearchSessionEntry,
  SearchedCandidate,
  SearchedJob,
  SearchSessionRecording
}
import com.example.graphQL.cats.service.events.SearchFilterSnapshot
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

/** Recording a search session is best-effort: the caller keeps its results and `searchId` when it fails. */
final class HiringGraphQLSearchRecordingSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-17T08:00:00Z")
  private val candidate = ServiceFixtures.candidate
  private val job = ServiceFixtures.openJob
  private val searchId = UUID.fromString("30000000-0000-0000-0000-000000000001")
  private val meta = EmbeddingMeta("voyage-4-lite", "hash", now)
  private val queryText = "very-secret-query-text"

  private val ranked = new SearchUseCases[IO] {
    def semanticJobSearch(
        a: ActorContext,
        text: String,
        f: JobSearchFilter,
        first: PageSize,
        id: UUID
    ): UseCaseF[IO, SearchResults[SearchedJob]] =
      EitherT.rightT[IO, UseCaseError](
        SearchResults(List(SearchedJob.from(RankedJob(job, 0.9, SearchMode.HYBRID, meta, id))), None)
      )
    def recommendedJobs(a: ActorContext, first: PageSize, id: UUID): UseCaseF[IO, List[RankedJob]] =
      EitherT.rightT(Nil)
    def candidateMatches(
        a: ActorContext,
        j: JobId,
        q: Option[String],
        f: CandidateMatchFilters,
        first: PageSize,
        id: UUID
    ): UseCaseF[IO, SearchResults[SearchedCandidate]] = EitherT.rightT(SearchResults(Nil, None))
  }

  private type FailureRecord = (UserId, OperationalEventPayload.SearchKind, UUID, String)

  private def recording(
      result: IO[Either[UseCaseError, Unit]],
      onFailure: FailureRecord => IO[Unit] = _ => IO.unit
  ): SearchSessionRecording[IO] =
    new SearchSessionRecording[IO] {
      def searchId(clientSupplied: Option[UUID]): IO[UUID] = IO.pure(clientSupplied.getOrElse(RecordingSpecIds.search))
      def record(
          actorId: UserId,
          kind: OperationalEventPayload.SearchKind,
          id: UUID,
          filter: SearchFilterSnapshot,
          model: Option[String],
          results: List[SearchSessionEntry]
      ): UseCaseF[IO, Unit] = EitherT(result)
      def recordFailure(
          actorId: UserId,
          kind: OperationalEventPayload.SearchKind,
          id: UUID,
          failure: String
      ): IO[Unit] = onFailure((actorId, kind, id, failure))
    }

  private object RecordingSpecIds { val search: UUID = searchId }

  private final class Captured(ref: Ref[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]])
      extends Diagnostics[IO] {
    def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
      ref.update(_ :+ ((event, requestId, fields)))
  }

  private def execute(
      query: String,
      sessions: SearchSessionRecording[IO],
      events: Ref[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]],
      interaction: com.example.graphQL.cats.service.protocol.InteractionUseCases[IO] = TestGraphQLSupport.interactions
  ): IO[Json] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](Map(candidate.id -> candidate))
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(job.id -> job))
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      createError <- Ref.of[IO, Option[RepositoryError]](None)
      users = new InMemoryUsers(usersRef)
      jobs = new InMemoryJobs(jobsRef)
      services = TestGraphQLSupport.emptyServices.copy(
        readModel =
          new HiringReadService(users, jobs, new InMemoryApplications(applicationsRef, eventsRef, createError)),
        searchSessions = sessions,
        interactionService = interaction,
        semanticSearchService = Some(ranked)
      )
      result <- TestGraphQLSupport
        .context(
          Some(ActorContext(candidate.id, candidate.role)),
          services,
          diagnostics = new Captured(events),
          requestId = Some("req-1")
        )
        .use(context =>
          TestGraphQLSupport
            .parseAndExecute(GraphQLRequest(query, Json.obj(), None), context)
            .map(_.fold(failure => fail(failure.toString), identity))
        )
    } yield result

  private val search =
    s"""query { semanticJobSearch(query: "$queryText", first: 5) {
       |  ... on RankedJobResults { results { searchId job { id } } } ... on UserError { code } } }""".stripMargin

  private def logged(events: Ref[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]]) =
    events.get.map(_.filter(_._1 == LogEvent.SearchRecordingFailed))

  test("AIR-04.1 a failed recording still returns the results and searchId, with one safe log event") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      json <- execute(search, recording(IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable)))), events)
      entries <- logged(events)
    } yield {
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      val first = json.hcursor.downField("data").downField("semanticJobSearch").downField("results").downArray
      assertEquals(first.downField("job").get[String]("id"), Right(job.id.value.toString))
      assertEquals(first.get[String]("searchId"), Right(searchId.toString))
      assertEquals(entries.size, 1)
      val (_, requestId, fields) = entries.head
      assertEquals(requestId, Some("req-1"))
      assertEquals(fields, Map(LogField.SearchKind -> "semanticJobSearch", LogField.ErrorCode -> "UNAVAILABLE"))
      assert(!fields.values.exists(value => value.contains(job.id.value.toString) || value.contains(queryText)))
    }
  }

  test("AIR-04.1 an unexpected exception from recording is logged by class and does not fail the search") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      json <- execute(search, recording(IO.raiseError(new RuntimeException("boom " + job.id.value))), events)
      entries <- logged(events)
    } yield {
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      assertEquals(entries.size, 1)
      val fields = entries.head._3
      assertEquals(fields.get(LogField.ErrorType), Some("java.lang.RuntimeException"))
      assertEquals(fields.get(LogField.SearchKind), Some("semanticJobSearch"))
      assert(!fields.values.exists(_.contains("boom")))
    }
  }

  test("AIR-04.3 a failed recording publishes one minimal failure event with the kind and error code only") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      published <- Ref.of[IO, Vector[FailureRecord]](Vector.empty)
      json <- execute(
        search,
        recording(
          IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable))),
          record => published.update(_ :+ record)
        ),
        events
      )
      recorded <- published.get
    } yield {
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      assertEquals(
        recorded,
        Vector((candidate.id, OperationalEventPayload.SearchKind.SemanticJobSearch, searchId, "UNAVAILABLE"))
      )
      assert(!recorded.exists(_._4.contains(queryText)))
    }
  }

  test("AIR-04.3 an unexpected recording exception publishes the exception class, not its message") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      published <- Ref.of[IO, Vector[FailureRecord]](Vector.empty)
      _ <- execute(
        search,
        recording(IO.raiseError(new RuntimeException("boom " + job.id.value)), record => published.update(_ :+ record)),
        events
      )
      recorded <- published.get
    } yield assertEquals(recorded.map(_._4), Vector("java.lang.RuntimeException"))
  }

  test("AIR-04.3 a failing failure-event publication never fails the search") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      json <- execute(
        search,
        recording(
          IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable))),
          _ => IO.raiseError(new IllegalStateException("outbox down"))
        ),
        events
      )
      entries <- logged(events)
    } yield {
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      assertEquals(entries.size, 1)
    }
  }

  test("AIR-04.3 a successful recording publishes no failure event") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      published <- Ref.of[IO, Vector[FailureRecord]](Vector.empty)
      _ <- execute(search, recording(IO.pure(Right(())), record => published.update(_ :+ record)), events)
      recorded <- published.get
    } yield assertEquals(recorded, Vector.empty)
  }

  test("AIR-04.1 a successful recording logs nothing") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      _ <- execute(search, recording(IO.pure(Right(()))), events)
      entries <- logged(events)
    } yield assertEquals(entries, Vector.empty)
  }

  test("AIR-04.1 cancelling a search while it is being recorded propagates and logs no failure") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      started <- Deferred[IO, Unit]
      services = TestGraphQLSupport.emptyServices.copy(searchSessions = recording(started.complete(()) *> IO.never))
      outcome <- TestGraphQLSupport
        .context(Some(ActorContext(candidate.id, candidate.role)), services, diagnostics = new Captured(events))
        .use { context =>
          for {
            fiber <- HiringGraphQLResolverSupport
              .recordSearch(
                context,
                services,
                candidate.id,
                OperationalEventPayload.SearchKind.SemanticJobSearch,
                searchId,
                SearchFilterSnapshot.Unfiltered
              )(
                List(job)
              )(_.id.value.toString, _ => 0d)
              .value
              .start
            _ <- started.get
            _ <- fiber.cancel
            result <- fiber.join
          } yield result
        }
      entries <- logged(events)
    } yield {
      assert(outcome.isCanceled, clue(outcome))
      assertEquals(entries, Vector.empty)
    }
  }

  test("AIR-04.2 a later click on a search whose recording failed returns DomainError SEARCH_SESSION_UNAVAILABLE") {
    val failedLookup = new SearchSessionWorkRepository[IO] {
      export TestSearchSessionWorkRepository.{claim, complete, enqueue, fail, retry}
      override def findForActor(actorId: UserId, id: UUID): RepositoryF[IO, Option[SearchSessionLookup]] =
        RepositoryF.at[IO].fromEither(Right(Some(SearchSessionLookup.Failed)))
    }
    val noSessions = new SearchSessionRepository[IO] {
      def save(
          session: com.example.graphQL.cats.service.events.SearchSession,
          event: com.example.graphQL.cats.service.events.OperationalEventEnvelope
      ) = RepositoryF.at[IO].fromEither(Right(()))
      def find(id: UUID) = RepositoryF.at[IO].fromEither(Right(None))
      def recordInteraction(
          event: com.example.graphQL.cats.service.events.OperationalEventEnvelope,
          context: com.example.graphQL.cats.service.port.MutationWriteContext
      ) = RepositoryF.at[IO].fromEither(Right(true))
    }
    val click = s"""mutation { recordSearchResultClick(input: {
       |  idempotencyKey: "30000000-0000-0000-0000-0000000000a1", eventId: "30000000-0000-0000-0000-0000000000a2",
       |  searchId: "$searchId", resultId: "${job.id.value}" }) { __typename ... on DomainError { code } } }""".stripMargin
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String], Map[LogField, String])]](Vector.empty)
      usersRef <- Ref.of[IO, Map[UserId, User]](Map(candidate.id -> candidate))
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(job.id -> job))
      telemetry = new OperationalTelemetryService[IO](
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        noSessions,
        failedLookup,
        TestIdempotency.noop,
        com.example.graphQL.cats.FixedTestClock.at(now)
      )
      failing = recording(IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable))))
      searched <- execute(search, failing, events, telemetry)
      clicked <- execute(click, failing, events, telemetry)
    } yield {
      assert(!searched.hcursor.downField("errors").succeeded, clue(searched))
      val payload = clicked.hcursor.downField("data").downField("recordSearchResultClick")
      assertEquals(payload.get[String]("__typename"), Right("DomainError"))
      assertEquals(payload.get[String]("code"), Right("SEARCH_SESSION_UNAVAILABLE"))
    }
  }
}
