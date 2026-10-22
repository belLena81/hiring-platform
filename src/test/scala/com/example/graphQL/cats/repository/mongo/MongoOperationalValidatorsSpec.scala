package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.domain.InterviewLifecycleFixtures
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{ApplicationStatus, CandidateProfile, User, UserProfile, UserRole}
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowPhase
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventPayload,
  OperationalEventType,
  SearchFilterSnapshot,
  SearchSession,
  SearchSessionResult
}
import com.example.graphQL.cats.service.port.{
  EmbeddingWorkFailure,
  EmbeddingWorkKind,
  EmbeddingWorkState,
  MutationReceiptState,
  SearchSessionWorkFailure,
  SearchSessionWorkState
}
import java.time.Instant
import java.util.UUID
import munit.FunSuite

/** Stored-shape rules that need no database: omitted optional fields, and validator enumerations that follow the
  * domain.
  */
final class MongoOperationalValidatorsSpec extends FunSuite {
  private val now = Instant.parse("2026-09-16T10:15:30Z")
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000201"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000203"))
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000208")

  private val operational = OperationalEventEnvelope(
    UUID.fromString("00000000-0000-0000-0000-000000000207"),
    OperationalEventType.SEARCH_PERFORMED,
    now,
    OperationalAggregateType.Search,
    searchId.toString,
    candidateId,
    OperationalEventPayload.SearchPerformed(
      searchId,
      SearchKind.SemanticJobSearch,
      List(SearchSessionResult(jobId.value.toString, 1, 0.95))
    )
  )

  test("MSC-04.1 an outbox row without a lease omits leaseOwner and leaseToken instead of storing null") {
    val outbox = MongoHiringCodecs.outboxRecord(operational, now).fold(error => fail(error), identity)
    assert(!outbox.containsKey(MongoFields.LeaseOwner), clue(outbox))
    assert(!outbox.containsKey(MongoFields.LeaseToken), clue(outbox))
    assert(!outbox.values().contains(null), clue(outbox))
  }

  test("MSC-04.1 a search session without a query or model omits both fields") {
    val session = SearchSession(
      searchId,
      candidateId,
      SearchKind.SemanticJobSearch,
      None,
      SearchFilterSnapshot.JobCriteria(None, Nil, None),
      None,
      List(SearchSessionResult("job-203", 1, 0.95d)),
      now,
      now.plusSeconds(60)
    )
    val stored = MongoHiringCodecs.searchSession(session)
    assert(!stored.containsKey(MongoFields.Query), clue(stored))
    assert(!stored.containsKey(MongoFields.Model), clue(stored))
    assertEquals(MongoHiringCodecs.readSearchSession(stored).toEither, Right(session))
  }

  test("MSC-04.1 a new user stores updatedAt equal to createdAt") {
    val user = User(
      candidateId,
      Some(email("candidate@example.com")),
      "Candidate",
      UserRole.Candidate,
      Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
      now
    )
    val stored = MongoHiringCodecs.user(user)
    assertEquals(stored.get(MongoFields.UpdatedAt), stored.get(MongoFields.CreatedAt))
    assertEquals(MongoHiringCodecs.readUser(stored).toEither, Right(user))
  }

  test("MSC-04.1 an interview workflow document no longer stores the unread subjectIds copy") {
    val stored = MongoInterviewWorkflowCodec.workflowDocument(InterviewLifecycleFixtures.completed)
    assert(!stored.containsKey(MongoFields.SubjectIds), clue(stored))
    assertEquals(stored.getString(MongoFields.DocumentType), MongoInterviewFields.WorkflowType)
  }

  test("MSC-05 validator enumerations list exactly the domain cases") {
    assertEquals(MongoOperationalValidators.ApplicationStatuses, ApplicationStatus.values.toList.map(_.toString))
    assertEquals(MongoOperationalValidators.UserRoles, UserRole.values.toList.map(_.toString))
    assertEquals(MongoOperationalValidators.EmbeddingWorkKinds, EmbeddingWorkKind.values.toList.map(_.toString))
    assertEquals(MongoOperationalValidators.EmbeddingWorkStates, EmbeddingWorkState.values.toList.map(_.toString))
    assertEquals(
      MongoOperationalValidators.EmbeddingWorkFailures,
      EmbeddingWorkFailure.values.toList.map(_.toString)
    )
    assertEquals(
      MongoOperationalValidators.SearchSessionWorkStates,
      SearchSessionWorkState.values.toList.map(_.toString)
    )
    assertEquals(
      MongoOperationalValidators.SearchSessionWorkFailures,
      SearchSessionWorkFailure.values.toList.map(_.toString)
    )
    assertEquals(MongoOperationalValidators.MutationReceiptStates, MutationReceiptState.values.toList.map(_.toString))
  }

  test("MSC-05 every phase the workflow writer can store is a string the open workflow branch admits") {
    // Phases are not enumerated in the validator (the workflow grows), so the branch must only require a string.
    val phase = MongoOperationalValidators.interviewWorkflowsValidator.toBsonDocument.toJson
    InterviewWorkflowPhase.values.foreach(value => assert(!phase.contains(s"\"${value.toString}\""), clue(value)))
  }

  test("MSC-05 migration 025 installs one definition per collection, users included, without duplicates") {
    val names = MongoOperationalValidators.all.map(_._1)
    assertEquals(names.distinct, names)
    assertEquals(names.size, 12)
    assert(names.contains(MongoCollections.Users))
    assertNotEquals(MongoHiringValidators.userValidator, MongoHiringValidators.userValidatorV1)
  }

  test("AC-04.2 AC-05.1 the validators replaced after migration 025 differ from the generation 025 installed") {
    val replaced = Set(
      MongoCollections.Users,
      MongoCollections.ApplicationEvents,
      MongoCollections.EmbeddingWork,
      MongoCollections.SearchSessionWork,
      MongoCollections.InterviewWorkflows,
      MongoCollections.InterviewWorkflowInbox
    )
    MongoOperationalValidators.all.zip(MongoOperationalValidators.installedBy025).foreach {
      case ((name, current), (installedName, installed)) =>
        assertEquals(name, installedName)
        assertEquals(current != installed, replaced(name), name)
    }
  }

  test("AC-04.2 AC-05.1 the completed proof of 025 accepts every generation of a replaced validator, oldest first") {
    val accepted = MongoOperationalValidators.acceptedByCompletedProof.toMap
    assertEquals(
      accepted(MongoCollections.SearchSessionWork),
      List(
        MongoOperationalValidators.searchSessionWorkValidatorThrough025,
        MongoOperationalValidators.searchSessionWorkValidatorThrough026,
        MongoOperationalValidators.searchSessionWorkValidator
      )
    )
    assertEquals(
      accepted(MongoCollections.InterviewWorkflows),
      List(
        MongoOperationalValidators.interviewWorkflowsValidatorThrough025,
        MongoOperationalValidators.interviewWorkflowsValidatorThrough030,
        MongoOperationalValidators.interviewWorkflowsValidator
      )
    )
    assertEquals(
      accepted(MongoCollections.Users),
      List(MongoHiringValidators.userValidatorV2, MongoHiringValidators.userValidator)
    )
    assertEquals(accepted.values.map(_.distinct.size).toList, accepted.values.map(_.size).toList)
  }

  test("AC-05.1 the search session work validator no longer knows an event copy") {
    val current = MongoOperationalValidators.searchSessionWorkValidator.toJson
    val previous = MongoOperationalValidators.searchSessionWorkValidatorThrough025.toJson
    assert(!current.contains("\"event\""), clue(current))
    assert(previous.contains("\"event\""), clue(previous))
  }

  test("MR-12 the embedding work validator does not require the kind and entity that its identity already holds") {
    val current = MongoOperationalValidators.embeddingWorkValidator.toJson
    assert(!current.contains("\"kind\"") && !current.contains("\"entityId\""), clue(current))
    assert(MongoOperationalValidators.embeddingWorkValidatorThrough025.toJson.contains("\"entityId\""))
  }

  test("MR-07 MR-08 the interview validators accept neither hiring receipts nor request receipts any more") {
    val inbox = MongoOperationalValidators.interviewWorkflowInboxValidator.toJson
    val workflows = MongoOperationalValidators.interviewWorkflowsValidator.toJson
    assert(!inbox.contains("hiringReceipt"), clue(inbox))
    assert(!workflows.contains("requestReceipt"), clue(workflows))
    assert(MongoOperationalValidators.interviewWorkflowInboxValidatorThrough025.toJson.contains("hiringReceipt"))
    assert(MongoOperationalValidators.interviewWorkflowsValidatorThrough025.toJson.contains("requestReceipt"))
  }
}
