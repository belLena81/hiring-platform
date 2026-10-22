package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.mongodb.client.model.Filters

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** The status write (AC-04): one `$set` of status and `updatedAt` guarded by the previous status, the history row and
  * the outbox rows in one transaction, so the stored status is always the latest history row.
  */
final class ApplicationStatusWriteIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val at = Instant.parse("2026-10-20T09:00:00Z")

  private def user(role: UserRole, name: String): User =
    User(
      UserId(UUID.randomUUID()),
      None,
      name,
      role,
      role match {
        case UserRole.Candidate => Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
        case UserRole.Recruiter => Some(UserProfile.Recruiter(RecruiterProfile("Status write organization", None)))
        case UserRole.Admin     => None
      },
      at
    )

  private final class Harness(val fixture: MongoAccessEvaluationSupport.Fixture) {
    val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
    val recruiter: User = user(UserRole.Recruiter, s"Status recruiter ${UUID.randomUUID()}")
    val candidate: User = user(UserRole.Candidate, s"Status candidate ${UUID.randomUUID()}")
    val job: Job = Job(
      JobId(UUID.randomUUID()),
      recruiter.id,
      "Status job",
      "Status description",
      List("Scala"),
      Set("Scala"),
      Location("Cyprus", Some("Nicosia"), remote = false),
      JobState.Open,
      at,
      at
    )
    val application: Application = Application.create(ApplicationId(UUID.randomUUID()), candidate.id, job.id, at)

    def seed: IO[Unit] =
      List(recruiter, candidate).traverse_(value =>
        MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(value))
      ) *> MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(job)) *>
        MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Applications,
          MongoHiringCodecs.application(application)
        ) *> MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.ApplicationEvents,
          MongoHiringCodecs.event(
            ApplicationEvent(
              ApplicationEventId(UUID.randomUUID()),
              application.id,
              None,
              ApplicationStatus.Created,
              candidate.id,
              at,
              None,
              None,
              UserRole.Candidate
            )
          )
        )

    def change(
        from: ApplicationStatus,
        to: ApplicationStatus,
        when: Instant,
        eventId: UUID = UUID.randomUUID(),
        subject: Application = application,
        feedback: Option[String] = None
    ): IO[Either[RepositoryError, Unit]] = {
      val moved = subject.copy(status = to, updatedAt = when)
      val event = ApplicationEvent(
        ApplicationEventId(eventId),
        subject.id,
        Some(from),
        to,
        recruiter.id,
        when,
        feedback,
        None,
        UserRole.Recruiter
      )
      applications
        .updateStatusWithEvents(
          moved,
          event,
          List(OperationalEvents.statusChanged(eventId, moved, event)),
          ApplicationWriteActor(recruiter.id, UserRole.Recruiter),
          MutationWriteContext.directWrite
        )
        .value
    }

    def count(collection: String): IO[Long] = MongoRepositoryTestSupport.count(fixture.database, collection)

    def stored: IO[Document] =
      MongoRepositoryTestSupport
        .findOne(
          fixture.database,
          MongoCollections.Applications,
          Filters.eq(MongoFields.Id, application.id.value.toString)
        )
        .flatMap(IO.fromOption(_)(new AssertionError("application missing")))
  }

  private type Document = org.bson.Document

  private def harness[A](body: Harness => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val h = new Harness(fixture)
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *> h.seed *> body(h)
    }

  test("AC-04.1 a chain of status writes keeps the stored status equal to the latest history row") {
    harness { h =>
      for {
        accepted <- h.change(ApplicationStatus.Created, ApplicationStatus.Accepted, at.plusSeconds(10))
        _ <- ApplicationStatusInvariant.assertAgrees(h.fixture.database, h.application.id)
        interview <- h.change(ApplicationStatus.Accepted, ApplicationStatus.Interview, at.plusSeconds(20))
        _ <- ApplicationStatusInvariant.assertAgrees(h.fixture.database, h.application.id)
        rejected <- h.change(
          ApplicationStatus.Interview,
          ApplicationStatus.Rejected,
          at.plusSeconds(30),
          feedback = Some("Not a fit")
        )
        _ <- ApplicationStatusInvariant.assertAgrees(h.fixture.database, h.application.id)
        events <- h.count(MongoCollections.ApplicationEvents)
        outbox <- h.count(MongoCollections.EventOutbox)
        stored <- h.stored
      } yield {
        assertEquals(List(accepted, interview, rejected), List.fill(3)(Right(())))
        assertEquals(events, 4L)
        assertEquals(outbox, 3L)
        assertEquals(stored.getString(MongoFields.Status), "Rejected")
      }
    }
  }

  test("AC-04.3 the write sets status and updatedAt only: no replacement of the document is issued") {
    harness { h =>
      for {
        _ <- h.fixture.commands.clear
        _ <- h.change(ApplicationStatus.Created, ApplicationStatus.Accepted, at.plusSeconds(10))
        commands <- h.fixture.commands.snapshot
        updates = commands.filter(command =>
          command.getFirstKey == "update" &&
            command.getString("update").getValue == MongoCollections.Applications
        )
      } yield {
        assertEquals(updates.size, 1)
        val statement = updates.head.getArray("updates").get(0).asDocument()
        val replacement = statement.getDocument("u")
        // An update document with operators only is a targeted write; a replacement would carry plain field names.
        assertEquals(replacement.keySet().asScala.toList, List("$set"))
        assertEquals(replacement.getDocument("$set").keySet().asScala.toSet, Set("status", "updatedAt"))
        // The guard names the previous status and the stored owner facts the fence relied on.
        val filter = statement.getDocument("q").toJson
        List("\"_id\"", "\"jobId\"", "\"candidateId\"", "\"status\": \"Created\"").foreach(part =>
          assert(filter.contains(part), clue(filter))
        )
      }
    }
  }

  test("AC-04.1 a write from a status the application has left is a Conflict that stores nothing") {
    harness { h =>
      for {
        first <- h.change(ApplicationStatus.Created, ApplicationStatus.Accepted, at.plusSeconds(10))
        eventsBefore <- h.count(MongoCollections.ApplicationEvents)
        outboxBefore <- h.count(MongoCollections.EventOutbox)
        stale <- h.change(ApplicationStatus.Created, ApplicationStatus.Declined, at.plusSeconds(20))
        _ <- ApplicationStatusInvariant.assertAgrees(h.fixture.database, h.application.id)
        eventsAfter <- h.count(MongoCollections.ApplicationEvents)
        outboxAfter <- h.count(MongoCollections.EventOutbox)
        stored <- h.stored
      } yield {
        assertEquals(first, Right(()))
        assertEquals(stale, Left(RepositoryError.Conflict))
        assertEquals((eventsAfter, outboxAfter), (eventsBefore, outboxBefore))
        assertEquals(stored.getString(MongoFields.Status), "Accepted")
      }
    }
  }

  test("AC-04.1 two writers racing from the same status commit exactly one and keep the history a chain") {
    harness { h =>
      for {
        outcomes <- List(ApplicationStatus.Accepted, ApplicationStatus.Declined, ApplicationStatus.Rejected)
          .parTraverse(target =>
            h.change(ApplicationStatus.Created, target, at.plusSeconds(10), feedback = Some("race"))
          )
        _ <- ApplicationStatusInvariant.assertAgrees(h.fixture.database, h.application.id)
        events <- h.count(MongoCollections.ApplicationEvents)
        outbox <- h.count(MongoCollections.EventOutbox)
      } yield {
        assertEquals(outcomes.count(_.isRight), 1, clue(outcomes))
        assert(outcomes.filter(_.isLeft).forall(_ == Left(RepositoryError.Conflict)), clue(outcomes))
        assertEquals((events, outbox), (2L, 1L))
      }
    }
  }

  test("AC-04.1 a history row that cannot be stored rolls the status back") {
    harness { h =>
      val duplicate = UUID.randomUUID()
      for {
        first <- h.change(
          ApplicationStatus.Created,
          ApplicationStatus.Accepted,
          at.plusSeconds(10),
          eventId = duplicate
        )
        // The same history identity again: the append-only insert fails after the status update ran in the transaction.
        replay <- h.change(
          ApplicationStatus.Accepted,
          ApplicationStatus.Interview,
          at.plusSeconds(20),
          eventId = duplicate
        )
        _ <- ApplicationStatusInvariant.assertAgrees(h.fixture.database, h.application.id)
        stored <- h.stored
        outbox <- h.count(MongoCollections.EventOutbox)
      } yield {
        assertEquals(first, Right(()))
        assertEquals(replay, Left(RepositoryError.Conflict))
        assertEquals(stored.getString(MongoFields.Status), "Accepted")
        assertEquals(outbox, 1L)
      }
    }
  }

  test("AC-04.1 a write that names another owner of the application matches nothing") {
    harness { h =>
      val forged = h.application.copy(candidateId = UserId(UUID.randomUUID()))
      for {
        result <- h.change(ApplicationStatus.Created, ApplicationStatus.Accepted, at.plusSeconds(10), subject = forged)
        _ <- ApplicationStatusInvariant.assertAgrees(h.fixture.database, h.application.id)
        stored <- h.stored
        events <- h.count(MongoCollections.ApplicationEvents)
      } yield {
        assertEquals(result, Left(RepositoryError.Conflict))
        assertEquals(stored.getString(MongoFields.Status), "Created")
        assertEquals(events, 1L)
      }
    }
  }
}
