package com.example.graphQL.cats.infrastructure.events

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  Application,
  ApplicationEvent,
  ApplicationStatus,
  Job,
  JobStatus,
  Location
}
import com.example.graphQL.cats.service.events.OperationalEventPayload.{JobKind, SearchKind}
import com.example.graphQL.cats.service.events.{
  OperationalEventEnvelope,
  OperationalEvents,
  SearchFilterSnapshot,
  SearchSession,
  SearchSessionResult
}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import munit.FunSuite

/** Byte-level wire goldens: the literals below must stay identical across any refactoring of the codec's home. */
class OperationalEventWireContractSpec extends FunSuite {
  private def id(n: Long): UUID = new UUID(0L, n)
  private val eventId = id(0x201)
  private val actor = UserId(id(0x202))
  private val applicationId = id(0x203)
  private val jobId = id(0x204)
  private val searchId = id(0x205)
  private val now = Instant.parse("2026-09-19T10:15:30Z")

  private def job(status: JobStatus, skills: Set[String]): Job =
    Job(
      JobId(jobId),
      actor,
      "title",
      "description",
      Nil,
      skills,
      Location("CY", "Nicosia", remote = false),
      status,
      now,
      now
    )
  private val application =
    Application(ApplicationId(applicationId), actor, JobId(jobId), ApplicationStatus.Created, now, now)
  private def statusEvent(previous: Option[ApplicationStatus], next: ApplicationStatus) =
    ApplicationEvent(
      ApplicationEventId(id(0x206)),
      ApplicationId(applicationId),
      previous,
      next,
      actor,
      now,
      None,
      None
    )
  private def session(kind: SearchKind, results: List[SearchSessionResult]) =
    SearchSession(searchId, actor, kind, None, SearchFilterSnapshot.Unfiltered, None, results, now, now.plusSeconds(60))
  private def ok[A, E](value: Either[E, A]): A = value.fold(error => fail(error.toString), identity)

  private def utf8(event: OperationalEventEnvelope): String =
    new String(OperationalEventWireCodec.bytes(event), StandardCharsets.UTF_8)

  private val cases: List[(String, OperationalEventEnvelope)] = List(
    "JOB_CREATED" -> ok(
      OperationalEvents.jobEvent(JobKind.Created, eventId, job(JobStatus.Open, Set("Scala", "Cats")), actor, now)
    ),
    "JOB_UPDATED escaped and unicode skill" -> ok(
      OperationalEvents
        .jobEvent(JobKind.Updated, eventId, job(JobStatus.Open, Set("C\"++\\", "Zażółć ☃ 😀")), actor, now)
    ),
    "JOB_CLOSED" -> ok(
      OperationalEvents.jobEvent(JobKind.Closed, eventId, job(JobStatus.Closed, Set("Scala")), actor, now)
    ),
    "JOB_VIEWED without searchId" -> OperationalEvents.jobViewed(eventId, JobId(jobId), actor, None, None, now),
    "JOB_VIEWED with searchId" -> OperationalEvents
      .jobViewed(eventId, JobId(jobId), actor, Some(searchId), Some(3), now),
    "SEARCH_PERFORMED scores 0.5 1.0 1.0E-4" -> ok(
      OperationalEvents.searchPerformed(
        eventId,
        session(
          SearchKind.SemanticJobSearch,
          List(
            SearchSessionResult(id(0x301).toString, 1, 0.5),
            SearchSessionResult(id(0x302).toString, 2, 1.0),
            SearchSessionResult(id(0x303).toString, 3, 1.0e-4)
          )
        )
      )
    ),
    "SEARCH_PERFORMED candidateMatches" -> ok(
      OperationalEvents.searchPerformed(
        eventId,
        session(
          SearchKind.CandidateMatches,
          List(SearchSessionResult(id(0x301).toString, 1, 0.5), SearchSessionResult(id(0x302).toString, 2, 0.25))
        )
      )
    ),
    "SEARCH_RESULT_CLICKED" -> ok(
      OperationalEvents.searchResultClicked(eventId, searchId, jobId.toString, SearchKind.Jobs, actor, 2, now)
    ),
    "APPLICATION_CREATED" -> OperationalEvents.applicationCreated(eventId, application, actor, now),
    "APPLICATION_STATUS_CHANGED null previousStatus" ->
      OperationalEvents.statusChanged(eventId, application, statusEvent(None, ApplicationStatus.Created)),
    "APPLICATION_STATUS_CHANGED" ->
      OperationalEvents.statusChanged(
        eventId,
        application,
        statusEvent(Some(ApplicationStatus.Interview), ApplicationStatus.Hired)
      ),
    "CANDIDATE_HIRED" ->
      OperationalEvents.candidateHired(
        eventId,
        application,
        statusEvent(Some(ApplicationStatus.Interview), ApplicationStatus.Hired)
      )
  )

  private final case class Golden(name: String, envelopeBytes: String, payload: String)

  private val goldens: List[Golden] = List(
    Golden(
      "JOB_CREATED",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"JOB_CREATED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Job","aggregateId":"00000000-0000-0000-0000-000000000204","actorId":"00000000-0000-0000-0000-000000000202","payload":{"job":{"jobId":"00000000-0000-0000-0000-000000000204","skills":["Cats","Scala"],"status":"Open"}}}""",
      """{"job":{"jobId":"00000000-0000-0000-0000-000000000204","skills":["Cats","Scala"],"status":"Open"}}"""
    ),
    Golden(
      "JOB_UPDATED escaped and unicode skill",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"JOB_UPDATED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Job","aggregateId":"00000000-0000-0000-0000-000000000204","actorId":"00000000-0000-0000-0000-000000000202","payload":{"job":{"jobId":"00000000-0000-0000-0000-000000000204","skills":["C\"++\\","Zażółć ☃ 😀"],"status":"Open"}}}""",
      """{"job":{"jobId":"00000000-0000-0000-0000-000000000204","skills":["C\"++\\","Zażółć ☃ 😀"],"status":"Open"}}"""
    ),
    Golden(
      "JOB_CLOSED",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"JOB_CLOSED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Job","aggregateId":"00000000-0000-0000-0000-000000000204","actorId":"00000000-0000-0000-0000-000000000202","payload":{"job":{"jobId":"00000000-0000-0000-0000-000000000204","skills":["Scala"],"status":"Closed"}}}""",
      """{"job":{"jobId":"00000000-0000-0000-0000-000000000204","skills":["Scala"],"status":"Closed"}}"""
    ),
    Golden(
      "JOB_VIEWED without searchId",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"JOB_VIEWED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Search","aggregateId":"00000000-0000-0000-0000-000000000204","actorId":"00000000-0000-0000-0000-000000000202","payload":{"searchId":null,"resultId":"00000000-0000-0000-0000-000000000204","searchKind":null,"rank":null}}""",
      """{"searchId":null,"resultId":"00000000-0000-0000-0000-000000000204","searchKind":null,"rank":null}"""
    ),
    Golden(
      "JOB_VIEWED with searchId",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"JOB_VIEWED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Search","aggregateId":"00000000-0000-0000-0000-000000000204","actorId":"00000000-0000-0000-0000-000000000202","payload":{"searchId":"00000000-0000-0000-0000-000000000205","resultId":"00000000-0000-0000-0000-000000000204","searchKind":null,"rank":3}}""",
      """{"searchId":"00000000-0000-0000-0000-000000000205","resultId":"00000000-0000-0000-0000-000000000204","searchKind":null,"rank":3}"""
    ),
    Golden(
      "SEARCH_PERFORMED scores 0.5 1.0 1.0E-4",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"SEARCH_PERFORMED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Search","aggregateId":"00000000-0000-0000-0000-000000000205","actorId":"00000000-0000-0000-0000-000000000202","payload":{"searchId":"00000000-0000-0000-0000-000000000205","searchKind":"semanticJobSearch","results":[{"resultId":"00000000-0000-0000-0000-000000000301","rank":1,"score":0.5},{"resultId":"00000000-0000-0000-0000-000000000302","rank":2,"score":1.0},{"resultId":"00000000-0000-0000-0000-000000000303","rank":3,"score":1.0E-4}]}}""",
      """{"searchId":"00000000-0000-0000-0000-000000000205","searchKind":"semanticJobSearch","results":[{"resultId":"00000000-0000-0000-0000-000000000301","rank":1,"score":0.5},{"resultId":"00000000-0000-0000-0000-000000000302","rank":2,"score":1.0},{"resultId":"00000000-0000-0000-0000-000000000303","rank":3,"score":1.0E-4}]}"""
    ),
    Golden(
      "SEARCH_PERFORMED candidateMatches",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"SEARCH_PERFORMED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Search","aggregateId":"00000000-0000-0000-0000-000000000205","actorId":"00000000-0000-0000-0000-000000000202","payload":{"searchId":"00000000-0000-0000-0000-000000000205","searchKind":"candidateMatches","results":[{"resultId":"00000000-0000-0000-0000-000000000301","rank":1,"score":0.5},{"resultId":"00000000-0000-0000-0000-000000000302","rank":2,"score":0.25}]}}""",
      """{"searchId":"00000000-0000-0000-0000-000000000205","searchKind":"candidateMatches","results":[{"resultId":"00000000-0000-0000-0000-000000000301","rank":1,"score":0.5},{"resultId":"00000000-0000-0000-0000-000000000302","rank":2,"score":0.25}]}"""
    ),
    Golden(
      "SEARCH_RESULT_CLICKED",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"SEARCH_RESULT_CLICKED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Search","aggregateId":"00000000-0000-0000-0000-000000000205","actorId":"00000000-0000-0000-0000-000000000202","payload":{"searchId":"00000000-0000-0000-0000-000000000205","resultId":"00000000-0000-0000-0000-000000000204","searchKind":"jobs","rank":2}}""",
      """{"searchId":"00000000-0000-0000-0000-000000000205","resultId":"00000000-0000-0000-0000-000000000204","searchKind":"jobs","rank":2}"""
    ),
    Golden(
      "APPLICATION_CREATED",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"APPLICATION_CREATED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Application","aggregateId":"00000000-0000-0000-0000-000000000203","actorId":"00000000-0000-0000-0000-000000000202","payload":{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","status":"Created"}}""",
      """{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","status":"Created"}"""
    ),
    Golden(
      "APPLICATION_STATUS_CHANGED null previousStatus",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"APPLICATION_STATUS_CHANGED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Application","aggregateId":"00000000-0000-0000-0000-000000000203","actorId":"00000000-0000-0000-0000-000000000202","payload":{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","previousStatus":null,"newStatus":"Created"}}""",
      """{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","previousStatus":null,"newStatus":"Created"}"""
    ),
    Golden(
      "APPLICATION_STATUS_CHANGED",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"APPLICATION_STATUS_CHANGED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Application","aggregateId":"00000000-0000-0000-0000-000000000203","actorId":"00000000-0000-0000-0000-000000000202","payload":{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","previousStatus":"Interview","newStatus":"Hired"}}""",
      """{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","previousStatus":"Interview","newStatus":"Hired"}"""
    ),
    Golden(
      "CANDIDATE_HIRED",
      """{"eventId":"00000000-0000-0000-0000-000000000201","eventType":"CANDIDATE_HIRED","occurredAt":"2026-09-19T10:15:30Z","aggregateType":"Application","aggregateId":"00000000-0000-0000-0000-000000000203","actorId":"00000000-0000-0000-0000-000000000202","payload":{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","status":"Hired"}}""",
      """{"applicationId":"00000000-0000-0000-0000-000000000203","candidateId":"00000000-0000-0000-0000-000000000202","jobId":"00000000-0000-0000-0000-000000000204","status":"Hired"}"""
    )
  )

  test("every event type serializes to the exact golden envelope bytes and payload string") {
    assertEquals(cases.map(_._1), goldens.map(_.name))
    cases.zip(goldens).foreach { case ((name, event), golden) =>
      assertEquals(utf8(event), golden.envelopeBytes, name)
      assertEquals(OperationalEventWireCodec.payloadJson(event.payload).noSpaces, golden.payload, name)
      assertEquals(
        OperationalEventWireCodec.bytes(event).toList,
        golden.envelopeBytes.getBytes(StandardCharsets.UTF_8).toList,
        name
      )
    }
  }

  test("golden envelopes decode back to the same event and re-encode to the same bytes") {
    cases.zip(goldens).foreach { case ((name, event), golden) =>
      val decoded = OperationalEventWireCodec.decode(golden.envelopeBytes.getBytes(StandardCharsets.UTF_8))
      assertEquals(decoded, Right(event), name)
      assertEquals(decoded.map(utf8), Right(golden.envelopeBytes), name)
    }
  }

  test("all nine operational event types are covered") {
    import com.example.graphQL.cats.service.events.OperationalEventType
    assertEquals(cases.map(_._2.eventType).toSet, OperationalEventType.values.toSet)
  }
}
