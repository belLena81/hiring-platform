package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.port.{ApplicationWriteActor, MutationWriteContext}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{
  Application,
  ApplicationEvent,
  ApplicationStatus,
  CandidateProfile,
  JobStatus,
  JobSubmissionSnapshot,
  RecruiterProfile,
  User,
  UserProfile,
  UserRole
}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.mongodb.client.model.{Filters, Updates}
import java.time.Instant
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import org.bson.Document
import java.util.UUID

final class MongoJobSubmissionIntegrationSpec extends MongoIntegrationSuite {
  private val id = JobId(UUID.fromString("00000000-0000-0000-0000-000000000007"))

  test("submission reads only identity status revision without aggregate or embedding hydration") {
    mongoResource.use { fixture =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      for {
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Jobs,
          new Document("_id", id.value.toString)
            .append("status", "Open")
            .append("version", 7L)
            .append("embedding", "malformed unrelated vector")
            .append("description", "x" * 100000)
        )
        _ <- fixture.commands.clear
        result <- repository.findSubmissionSnapshot(id).value
        commands <- fixture.commands.snapshot
        full <- repository.findVersioned(id).value
      } yield {
        assertEquals(result, Right(Some(JobSubmissionSnapshot(id, JobStatus.Open, 7L))))
        val projections = commands.filter(_.containsKey("find")).map(_.getDocument("projection").keySet())
        assertEquals(projections.size, 1)
        assertEquals(projections.head, java.util.Set.of("_id", "status", "version"))
        assertEquals(full, Left(RepositoryError.InvalidStoredData))
      }
    }
  }

  test("missing and malformed revision fail closed while absent job stays absent") {
    mongoResource.use { fixture =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      for {
        absent <- repository.findSubmissionSnapshot(id).value
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Jobs,
          new Document("_id", id.value.toString).append("status", "Open").append("version", -1L)
        )
        malformed <- repository.findSubmissionSnapshot(id).value
      } yield {
        assertEquals(absent, Right(None))
        assertEquals(malformed, Left(RepositoryError.InvalidStoredData))
      }
    }
  }

  test("projected snapshot preserves closure duplicate and atomic revision guards") {
    mongoResource.use { fixture =>
      val jobs = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val repository = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      val now = Instant.parse("2026-10-07T12:00:00Z")
      val recruiterId = UserId(UUID.randomUUID())
      val candidate = User(
        UserId(UUID.randomUUID()),
        None,
        "Submission candidate",
        UserRole.Candidate,
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
        now
      )
      val candidateActor = ApplicationWriteActor(candidate.id, candidate.role)
      val application = Application.create(ApplicationId(UUID.randomUUID()), candidate.id, id, now)
      val event = ApplicationEvent(
        ApplicationEventId(UUID.randomUUID()),
        application.id,
        None,
        ApplicationStatus.Created,
        application.candidateId,
        now,
        None,
        None,
        None
      )
      val readSnapshot = jobs
        .findSubmissionSnapshot(id)
        .value
        .flatMap(result =>
          IO.fromEither(
            result
              .flatMap(_.toRight(RepositoryError.InvalidStoredData))
              .leftMap(error => new AssertionError(s"Missing snapshot: $error"))
          )
        )
      for {
        _ <- MongoHiringIndexSetup.create(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Users,
          MongoHiringCodecs.user(candidate)
        )
        // A submission also requires the job's recruiter to be an active account.
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Users,
          MongoHiringCodecs.user(
            User(
              recruiterId,
              None,
              "Submission recruiter",
              UserRole.Recruiter,
              Some(UserProfile.Recruiter(RecruiterProfile("Submission organization", None))),
              now
            )
          )
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Jobs,
          new Document("_id", id.value.toString)
            .append("status", "Open")
            .append("version", 0L)
            .append("recruiterId", recruiterId.value.toString)
        )
        snapshot <- readSnapshot
        first <- repository
          .createForOpenJobWithEvents(
            snapshot,
            application,
            event,
            Nil,
            candidateActor,
            MutationWriteContext.directWrite
          )
          .value
        // The first submission advanced the job revision, so a duplicate is attempted against the fresh snapshot.
        fresh <- readSnapshot
        duplicate <- repository
          .createForOpenJobWithEvents(fresh, application, event, Nil, candidateActor, MutationWriteContext.directWrite)
          .value
        afterDuplicate <- jobs.findSubmissionSnapshot(id).value
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Jobs)
        _ <- collection.updateOne(Filters.eq("_id", id.value.toString), Updates.set("status", "Closed"))
        closedApplication = application.copy(id = ApplicationId(UUID.randomUUID()))
        closedEvent = event.copy(id = ApplicationEventId(UUID.randomUUID()), applicationId = closedApplication.id)
        closed <- repository
          .createForOpenJobWithEvents(
            fresh,
            closedApplication,
            closedEvent,
            Nil,
            candidateActor,
            MutationWriteContext.directWrite
          )
          .value
        count <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.Applications)
        history <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.ApplicationEvents)
      } yield {
        assertEquals(first, Right(()))
        assertEquals(duplicate, Left(RepositoryError.DuplicateApplication))
        assertEquals(afterDuplicate.map(_.map(_.revision)), Right(Some(1L)))
        assertEquals(closed, Left(RepositoryError.Conflict))
        assertEquals(count, 1L)
        assertEquals(history, 1L)
      }
    }
  }

}
