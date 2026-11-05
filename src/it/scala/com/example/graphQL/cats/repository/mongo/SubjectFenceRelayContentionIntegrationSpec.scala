package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventPayload,
  OperationalEventType
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** DDR-01: a sole recruiter's sequential status changes (the user fence `$inc` on `outbox_subject_fences`) are not
  * refused with `Conflict` while the outbox relay cycles the same subject's lease (acquire, renew, release). The relay
  * writes the same document, so collisions cost a retry, but its writes are short and never exhaust the transaction
  * runner's retries.
  */
final class SubjectFenceRelayContentionIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val at = Instant.parse("2026-10-05T12:00:00Z")
  private val lease = 30.seconds
  private val changes = 60
  private val backlog = 100
  private val userPause = 30.millis

  private def user(role: UserRole): User =
    User(
      UserId(UUID.randomUUID()),
      None,
      s"Contention ${UUID.randomUUID()}",
      role,
      Some(
        if (role == UserRole.Recruiter) UserProfile.Recruiter(RecruiterProfile("Contention organization", None))
        else UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))
      ),
      at
    )

  private def openJob(owner: User): Job =
    Job(
      JobId(UUID.randomUUID()),
      owner.id,
      s"Contention job ${UUID.randomUUID()}",
      "Description",
      List("Requirement"),
      Set("Scala"),
      Location("Cyprus", Some("Nicosia"), remote = false),
      JobState.Open,
      at,
      at
    )

  private def jobEvent(actor: UserId, index: Int): OperationalEventEnvelope = {
    val job = UUID.randomUUID()
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      at.plusMillis(index.toLong),
      OperationalAggregateType.Job,
      job.toString,
      actor,
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Created,
        OperationalEventPayload.JobSnapshot(job, List("Scala"), JobStatus.Open)
      )
    )
  }

  private final case class Outcome(refused: Int, accepted: Int, relayCycles: Int)

  private def run(relays: Int): IO[Outcome] =
    mongoResource.use { fixture =>
      val applications =
        MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      val recruiter = user(UserRole.Recruiter)
      val candidates = List.fill(changes)(user(UserRole.Candidate))
      val job = openJob(recruiter)
      val apps = candidates.map(c => Application.create(ApplicationId(UUID.randomUUID()), c.id, job.id, at))

      def accept(application: Application): IO[Either[RepositoryError, Unit]] = {
        val event = ApplicationEvent(
          ApplicationEventId(UUID.randomUUID()),
          application.id,
          Some(ApplicationStatus.Created),
          ApplicationStatus.Accepted,
          recruiter.id,
          at.plusSeconds(60),
          None,
          None,
          recruiter.role
        )
        applications
          .updateStatusWithEvents(
            application.copy(status = ApplicationStatus.Accepted, updatedAt = at.plusSeconds(60)),
            event,
            Nil,
            ApplicationWriteActor(recruiter.id, recruiter.role),
            MutationWriteContext.directWrite
          )
          .value
      }

      def stored(value: OperationalEventEnvelope) =
        MongoHiringCodecs.outboxRecord(value, value.occurredAt).fold(message => fail(message), identity)

      // The relay cycle as the publisher drives it: claim (acquire), renew, then publish (release).
      def relayCycle(cycles: Ref[IO, Int]): IO[Unit] =
        outbox
          .claim("relay", ProducerGenerationFixtures.of("hiring-publisher-relay"), at.plusSeconds(1000), lease, 1)
          .value
          .flatMap {
            case Right(claimed) if claimed.nonEmpty =>
              val head = claimed.head
              outbox.renewLease(head.event.eventId, head.leaseToken, head.subjectIds, lease).value *>
                outbox
                  .markPublished(head.event.eventId, head.leaseToken, at.plusSeconds(1001), at.plusSeconds(4000))
                  .value *> cycles.update(_ + 1)
            case _ => IO.sleep(5.millis)
          }

      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- (recruiter :: candidates).traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(value))
        )
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(job))
        _ <- apps.traverse_(value =>
          MongoRepositoryTestSupport
            .insertOne(fixture.database, MongoCollections.Applications, MongoHiringCodecs.application(value))
        )
        _ <- List
          .tabulate(backlog)(index => jobEvent(recruiter.id, index))
          .traverse_(value =>
            MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, stored(value))
          )
        cycles <- Ref.of[IO, Int](0)
        relay = List.fill(relays)(relayCycle(cycles).foreverM.void).parSequence_
        // Two cycles first, so the relay has demonstrably taken and released the subject lease before the race.
        _ <- relayCycle(cycles) *> relayCycle(cycles)
        results <- relay.background.use(_ => apps.traverse(app => accept(app) <* IO.sleep(userPause)))
        done <- cycles.get
      } yield Outcome(results.count(_ == Left(RepositoryError.Conflict)), results.count(_.isRight), done)
    }

  test("DDR-01 a sole recruiter's status changes are not refused while three relays cycle the subject lease") {
    run(relays = 3).map { outcome =>
      assertEquals(outcome.refused, 0, clue(outcome))
      assertEquals(outcome.accepted, changes, clue(outcome))
      assert(outcome.relayCycles > 2, clue(outcome))
    }
  }
}
