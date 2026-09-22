package com.example.graphQL.cats.service.events

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.repository.protocol.SearchSessionRepository
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, OperationalEventType, SearchSession, SearchSessionResult}
import io.circe.Json
import munit.CatsEffectSuite

import java.util.UUID
import scala.concurrent.duration.*

class OperationalTelemetryServiceSpec extends CatsEffectSuite {
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000101")
  private val eventId = UUID.fromString("00000000-0000-0000-0000-000000000102")
  private val clickEventId = UUID.fromString("00000000-0000-0000-0000-000000000103")
  private val otherActorId = UUID.fromString("00000000-0000-0000-0000-000000000104")

  test("recordJobView derives rank from the durable search session and treats identical retries as one event") {
    withService().flatMap { case (sessions, service) =>
      val actor = ActorContext(candidateId, UserRole.Candidate)
      for {
        first <- service.recordJobView(actor, eventId, jobId, Some(searchId), now)
        retry <- service.recordJobView(actor, eventId, jobId, Some(searchId), later)
        events <- sessions.events
      } yield {
        assertEquals(first, Right(()))
        assertEquals(retry, Right(()))
        assertEquals(events.size, 1)
        assertEquals(events.head.eventType, OperationalEventType.JOB_VIEWED)
        assertEquals(events.head.payload.hcursor.get[Int]("rank"), Right(2))
      }
    }
  }

  test("recordSearchResultClick rejects forged actor and result data") {
    withService().flatMap { case (_, service) =>
      val actor = ActorContext(candidateId, UserRole.Candidate)
      val otherActor = ActorContext(com.example.graphQL.cats.domain.model.Identifiers.UserId(otherActorId), UserRole.Candidate)
      for {
        forgedActor <- service.recordSearchResultClick(otherActor, clickEventId, searchId, jobId.value.toString, now)
        forgedResult <- service.recordSearchResultClick(actor, clickEventId, searchId, "not-a-result", now)
      } yield {
        assertEquals(forgedActor, Left(UseCaseError.Domain(DomainError.Forbidden)))
        assertEquals(forgedResult, Left(UseCaseError.Domain(DomainError.Forbidden)))
      }
    }
  }

  test("recordInteraction detects conflicting duplicate client event IDs") {
    val conflictingJobId = com.example.graphQL.cats.domain.model.Identifiers.JobId(
      UUID.fromString("00000000-0000-0000-0000-000000000105")
    )
    val conflictingJob = openJob.copy(id = conflictingJobId)
    withService(extraJobs = Map(conflictingJobId -> conflictingJob)).flatMap { case (_, service) =>
      val actor = ActorContext(candidateId, UserRole.Candidate)
      for {
        first <- service.recordJobView(actor, eventId, jobId, Some(searchId), now)
        conflict <- service.recordJobView(actor, eventId, conflictingJobId, None, later)
      } yield {
        assertEquals(first, Right(()))
        assertEquals(conflict, Left(UseCaseError.Repository(RepositoryError.Conflict)))
      }
    }
  }

  private def withService(extraJobs: Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job] = Map.empty) =
    for {
      usersRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, com.example.graphQL.cats.domain.model.User]](
        Map(candidateId -> candidate, recruiterId -> recruiter)
      )
      jobsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job]](
        Map(jobId -> openJob) ++ extraJobs
      )
      sessionsRef <- Ref.of[IO, Map[UUID, SearchSession]](Map(searchId -> session))
      eventsRef <- Ref.of[IO, Vector[OperationalEventEnvelope]](Vector.empty)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      sessions = new InMemorySearchSessions(sessionsRef, eventsRef)
    } yield sessions -> OperationalTelemetryService(users, jobs, sessions)

  private val session: SearchSession =
    SearchSession(
      searchId,
      candidateId,
      "jobs",
      None,
      Json.obj(),
      None,
      List(
        SearchSessionResult("other-result", 1, 0d),
        SearchSessionResult(jobId.value.toString, 2, 0d)
      ),
      now,
      now.plusSeconds(7.days.toSeconds)
    )

  private final class InMemorySearchSessions(
      sessions: Ref[IO, Map[UUID, SearchSession]],
      storedEvents: Ref[IO, Vector[OperationalEventEnvelope]]
  ) extends SearchSessionRepository {
    override def save(session: SearchSession, event: OperationalEventEnvelope): IO[Either[RepositoryError, Unit]] = {
      val _ = event
      sessions.update(_ + (session.id -> session)).as(Right(()))
    }

    override def find(id: UUID): IO[Either[RepositoryError, Option[SearchSession]]] =
      sessions.get.map(values => Right(values.get(id)))

    override def recordInteraction(event: OperationalEventEnvelope): IO[Either[RepositoryError, Boolean]] =
      storedEvents.modify { events =>
        events.find(_.eventId == event.eventId) match {
          case Some(existing) if sameLogicalEvent(existing, event) => events -> Right(false)
          case Some(_) => events -> Left(RepositoryError.Conflict)
          case None => (events :+ event) -> Right(true)
        }
      }

    def events: IO[Vector[OperationalEventEnvelope]] =
      storedEvents.get

    private def sameLogicalEvent(left: OperationalEventEnvelope, right: OperationalEventEnvelope): Boolean =
      left.eventId == right.eventId &&
        left.eventType == right.eventType &&
        left.aggregateType == right.aggregateType &&
        left.aggregateId == right.aggregateId &&
        left.actorId == right.actorId &&
        left.payload == right.payload
  }
}
