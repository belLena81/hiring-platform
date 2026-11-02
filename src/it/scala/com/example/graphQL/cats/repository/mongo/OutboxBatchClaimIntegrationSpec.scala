package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventPayload,
  OperationalEventType
}
import com.example.graphQL.cats.service.port.{ClaimedOperationalEvent, MutationWriteContext}
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.mongodb.client.model.Filters
import org.bson.Document

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** MR-06 (AC-08.4): the outbox claim leases a bounded batch with one claim token. Per event the cost falls with the
  * batch size, a subject has one in-flight event, and a stale holder is fenced by its generation of the lease.
  */
final class OutboxBatchClaimIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private val now = Instant.parse("2026-10-20T10:00:00Z")
  private val lease = 30.seconds

  private def event(actor: UserId, index: Int): OperationalEventEnvelope = {
    val job = UUID.randomUUID()
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      now.plusMillis(index.toLong),
      OperationalAggregateType.Job,
      job.toString,
      actor,
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Created,
        OperationalEventPayload.JobSnapshot(job, List("Scala"), JobStatus.Open)
      )
    )
  }

  private def stored(value: OperationalEventEnvelope): Document =
    MongoHiringCodecs
      .outboxRecord(value, value.occurredAt)
      .fold(message => fail(message), identity)

  private def seed(fixture: MongoAccessEvaluationSupport.Fixture, values: List[OperationalEventEnvelope]): IO[Unit] =
    values.traverse_(value =>
      MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, stored(value))
    )

  private def outbox(fixture: MongoAccessEvaluationSupport.Fixture) =
    MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])

  private def claim(
      repository: MongoOperationalEventOutboxRepository,
      worker: String,
      at: Instant,
      limit: Int
  ): IO[List[ClaimedOperationalEvent]] =
    repository
      .claim(worker, s"hiring-publisher-$worker", at, lease, limit)
      .value
      .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"claim failed: $error")), IO.pure))

  /** The holder stopped renewing: the event leases and the subject leases of its claim are in the database's past. */
  private def expireClaims(
      fixture: MongoAccessEvaluationSupport.Fixture,
      claimed: List[ClaimedOperationalEvent]
  ): IO[Unit] =
    claimed.traverse_ { claim =>
      LeaseTestSupport.expire(fixture.database, MongoCollections.EventOutbox, claim.event.eventId.toString) *>
        claim.subjectIds.traverse_(LeaseTestSupport.expire(fixture.database, MongoCollections.OutboxSubjectFences, _))
    }

  private def row(fixture: MongoAccessEvaluationSupport.Fixture, id: UUID): IO[Document] =
    MongoRepositoryTestSupport
      .findOne(fixture.database, MongoCollections.EventOutbox, Filters.eq(MongoFields.Id, id.toString))
      .flatMap(IO.fromOption(_)(new AssertionError("outbox row missing")))

  private def fence(fixture: MongoAccessEvaluationSupport.Fixture, subject: UserId): IO[Option[Document]] =
    MongoRepositoryTestSupport.findOne(
      fixture.database,
      MongoCollections.OutboxSubjectFences,
      Filters.eq(MongoFields.Id, subject.value.toString)
    )

  private def ids(claims: List[ClaimedOperationalEvent]): List[UUID] = claims.map(_.event.eventId)

  private val DataCommands = Set("find", "aggregate", "findAndModify", "update", "delete", "insert", "getMore")

  /** Every data command and transaction commit the claim issued, as the driver reported it. */
  private def issued(fixture: MongoAccessEvaluationSupport.Fixture): IO[List[String]] =
    fixture.commands.snapshot.map(
      _.map(_.getFirstKey).filter(name => DataCommands(name) || name == "commitTransaction")
    )

  test("AC-08.4 a claim of forty events issues a fixed handful of commands, not a transaction per event") {
    mongoResource.use { fixture =>
      val values = (0 until 40).toList.map(index => event(UserId(UUID.randomUUID()), index))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, values)
        _ <- fixture.commands.clear
        claimed <- claim(repository, "batch", now.plusSeconds(1), limit = 40)
        commands <- issued(fixture)
        perEvent = commands.size.toDouble / claimed.size
        _ = println(
          f"AC-08.4 outbox claim: ${claimed.size} events, ${commands.size} commands ($perEvent%.2f per event): " +
            commands.groupBy(identity).view.mapValues(_.size).toList.sortBy(_._1).mkString(", ")
        )
        _ <- fixture.commands.clear
        // The per-row claim this replaced issued five commands per event with one subject (the claim findAndModify,
        // the deletion read, the subject lease, the producer registration and the commit) plus three per page.
        empty <- claim(repository, "batch", now.plusSeconds(2), limit = 40)
      } yield {
        assertEquals(ids(claimed), values.map(_.eventId))
        assertEquals(claimed.map(_.leaseToken).distinct.size, 1, "the whole batch shares one claim token")
        assert(commands.size <= 16, clue(commands))
        assert(perEvent < 0.5, clue(perEvent))
        assertEquals(empty, Nil)
      }
    }
  }

  test("AC-08.4 the batch is bounded by the requested limit and resumes after it") {
    mongoResource.use { fixture =>
      val values = (0 until 10).toList.map(index => event(UserId(UUID.randomUUID()), index))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, values)
        first <- claim(repository, "w", now.plusSeconds(1), limit = 3)
        second <- claim(repository, "w", now.plusSeconds(1), limit = 3)
        rest <- claim(repository, "w", now.plusSeconds(1), limit = 100)
      } yield {
        assertEquals(ids(first), values.take(3).map(_.eventId))
        assertEquals(ids(second), values.slice(3, 6).map(_.eventId))
        assertEquals(ids(rest), values.drop(6).map(_.eventId))
      }
    }
  }

  test("AC-08.4 a subject has one in-flight event: later events of the batch's subjects wait for its release") {
    mongoResource.use { fixture =>
      val shared = UserId(UUID.randomUUID())
      val others = List.fill(2)(UserId(UUID.randomUUID()))
      val values = List(
        event(shared, 0),
        event(shared, 1),
        event(others.head, 2),
        event(shared, 3),
        event(others(1), 4)
      )
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, values)
        first <- claim(repository, "w", now.plusSeconds(1), limit = 5)
        waiting <- values.traverse(value => row(fixture, value.eventId).map(_.getString(MongoFields.State)))
        heldSubject <- fence(fixture, shared)
        // While the first event of the shared subject is in flight, nobody else gets another one.
        blocked <- claim(repository, "other", now.plusSeconds(2), limit = 5)
        published <- repository
          .markPublished(values.head.eventId, first.head.leaseToken, now.plusSeconds(3), now.plusSeconds(3600))
          .value
        released <- fence(fixture, shared)
        next <- claim(repository, "w", now.plusSeconds(4), limit = 5)
      } yield {
        assertEquals(ids(first), List(values(0), values(2), values(4)).map(_.eventId))
        assertEquals(waiting, List("InFlight", "Retryable", "InFlight", "Retryable", "InFlight"))
        assertEquals(heldSubject.map(_.getString(MongoFields.LeaseEventId)), Some(values.head.eventId.toString))
        assertEquals(blocked, Nil)
        assertEquals(published, Right(()))
        assert(released.exists(!_.containsKey(MongoFields.LeaseToken)), clue(released))
        assertEquals(ids(next), List(values(1).eventId))
      }
    }
  }

  test("AC-08.4 completing one event of a batch releases only its own subject leases") {
    mongoResource.use { fixture =>
      val subjects = List.fill(3)(UserId(UUID.randomUUID()))
      val values = subjects.zipWithIndex.map { case (subject, index) => event(subject, index) }
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, values)
        claimed <- claim(repository, "w", now.plusSeconds(1), limit = 3)
        token = claimed.head.leaseToken
        _ <- repository.markPublished(values(1).eventId, token, now.plusSeconds(2), now.plusSeconds(3600)).value
        fences <- subjects.traverse(fence(fixture, _))
        stillOwned <- repository
          .renewLease(values.head.eventId, token, List(subjects.head.value.toString), 60.seconds)
          .value
        otherOwner <- repository
          .renewLease(values.head.eventId, token, List(subjects(2).value.toString), 60.seconds)
          .value
      } yield {
        assert(fences(0).exists(_.containsKey(MongoFields.LeaseToken)), clue(fences))
        assert(fences(1).exists(!_.containsKey(MongoFields.LeaseToken)), clue(fences))
        assert(fences(2).exists(_.containsKey(MongoFields.LeaseToken)), clue(fences))
        assertEquals(stillOwned, Right(()))
        // The shared token alone does not extend a lease held for another event of the batch.
        assertEquals(otherOwner, Left(RepositoryError.Conflict))
      }
    }
  }

  test("AC-08.4 a stale holder is fenced by its claim token once another worker reclaims the batch") {
    mongoResource.use { fixture =>
      val values = List.fill(3)(UserId(UUID.randomUUID())).zipWithIndex.map { case (subject, index) =>
        event(subject, index)
      }
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, values)
        stale <- claim(repository, "slow", now.plusSeconds(1), limit = 3)
        beforeExpiry <- claim(repository, "fast", now.plusSeconds(10), limit = 3)
        _ <- expireClaims(fixture, stale)
        reclaimed <- claim(repository, "fast", now.plusSeconds(1 + lease.toSeconds), limit = 3)
        late <- repository
          .markPublished(values.head.eventId, stale.head.leaseToken, now.plusSeconds(40), now.plusSeconds(3600))
          .value
        renewed <- repository
          .renewLease(
            values.head.eventId,
            stale.head.leaseToken,
            stale.head.subjectIds,
            90.seconds
          )
          .value
        current <- repository
          .markPublished(values.head.eventId, reclaimed.head.leaseToken, now.plusSeconds(41), now.plusSeconds(3600))
          .value
        state <- row(fixture, values.head.eventId).map(_.getString(MongoFields.State))
      } yield {
        assertEquals(beforeExpiry, Nil)
        assertEquals(ids(reclaimed), values.map(_.eventId))
        assertEquals(stale.map(_.leaseToken).distinct.size, 1)
        assert(reclaimed.head.leaseToken != stale.head.leaseToken)
        assertEquals(reclaimed.map(_.attempts), List(2, 2, 2))
        assertEquals(late, Left(RepositoryError.Conflict))
        assertEquals(renewed, Left(RepositoryError.Conflict))
        assertEquals(current, Right(()))
        assertEquals(state, "Published")
      }
    }
  }

  test("AC-08.4 events of a deleted account and unreadable events are failed without blocking the batch") {
    mongoResource.use { fixture =>
      val live = UserId(UUID.randomUUID())
      val deleted = User(
        UserId(UUID.randomUUID()),
        None,
        s"Batch candidate ${UUID.randomUUID()}",
        UserRole.Candidate,
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
        now
      )
      val users = MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val values = List(event(live, 0), event(deleted.id, 1), event(live, 2), event(UserId(UUID.randomUUID()), 3))
      val corrupt = stored(values(3))
      corrupt.put(MongoFields.EnvelopeBytes, new org.bson.types.Binary(Array[Byte](1, 2, 3)))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Users,
          MongoHiringCodecs.user(deleted)
        )
        _ <- seed(fixture, values.take(3))
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, corrupt)
        _ <- users
          .deleteAccount(deleted.id, now, s"deleted-${deleted.id.value}", MutationWriteContext.directWrite)
          .value
          .flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))
        claimed <- claim(repository, "w", now.plusSeconds(1), limit = 10)
        suppressed <- row(fixture, values(1).eventId)
        unreadable <- row(fixture, values(3).eventId)
        deletedFence <- fence(fixture, deleted.id)
      } yield {
        // The live subject has two events; only the first can be in flight.
        assertEquals(ids(claimed), List(values.head.eventId))
        assertEquals(suppressed.getString(MongoFields.State), "Failed")
        assertEquals(suppressed.getString(MongoFields.LastError), "SUBJECT_DELETED")
        assert(!suppressed.containsKey(MongoFields.LeaseToken), clue(suppressed))
        assertEquals(unreadable.getString(MongoFields.State), "Failed")
        assertEquals(unreadable.getString(MongoFields.LastError), "INVALID_EVENT_CONTRACT")
        // A deleted subject never receives a publisher lease.
        assert(deletedFence.forall(!_.containsKey(MongoFields.LeaseToken)), clue(deletedFence))
      }
    }
  }

  test("AC-08.4 concurrent workers never share an event or a subject lease") {
    mongoResource.use { fixture =>
      val subjects = List.fill(8)(UserId(UUID.randomUUID()))
      // Four workers claim from the same pool; subjects repeat so batches contend for the same leases.
      val values = (0 until 64).toList.map(index => event(subjects(index % subjects.size), index))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, values)
        batches <- List("a", "b", "c", "d").parTraverse(worker =>
          claim(repository, worker, now.plusSeconds(1), limit = 16)
        )
      } yield {
        val all = batches.flatten
        assertEquals(all.map(_.event.eventId).distinct.size, all.size, "no event is claimed twice")
        // At most one in-flight event per subject across every worker.
        val bySubject = all.flatMap(claimed => claimed.subjectIds.map(_ -> claimed.event.eventId)).groupBy(_._1)
        assert(bySubject.values.forall(_.map(_._2).distinct.size == 1), clue(bySubject.view.mapValues(_.size).toMap))
        assert(all.nonEmpty)
      }
    }
  }

  test(
    "AC-08.4 a poison row between healthy events is failed alone: the healthy ones publish and the cursor passes it"
  ) {
    mongoResource.use { fixture =>
      val values = (0 until 3).toList.map(index => event(UserId(UUID.randomUUID()), index))
      val poison = stored(values(1))
      poison.put(MongoFields.SubjectIds, java.util.List.of("not-a-subject"))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(values.head))
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, poison)
        _ <- seed(fixture, List(values(2)))
        claimed <- claim(repository, "w", now.plusSeconds(1), limit = 3)
        published <- claimed.traverse(value =>
          repository
            .markPublished(value.event.eventId, value.leaseToken, now.plusSeconds(2), now.plusSeconds(3600))
            .value
        )
        failed <- row(fixture, values(1).eventId)
        later <- claim(repository, "w", now.plusSeconds(3), limit = 3)
        states <- values.traverse(value => row(fixture, value.eventId).map(_.getString(MongoFields.State)))
      } yield {
        assertEquals(ids(claimed), List(values(0), values(2)).map(_.eventId))
        assertEquals(published, List.fill(2)(Right(())))
        assertEquals(failed.getString(MongoFields.LastError), "INVALID_EVENT_CONTRACT")
        assert(!failed.containsKey(MongoFields.LeaseToken), clue(failed))
        assertEquals(later, Nil)
        assertEquals(states, List("Published", "Failed", "Published"))
      }
    }
  }

  test("AC-08.4 a claim racing an account deletion never leaves a publisher lease on the deleted subject") {
    mongoResource.use { fixture =>
      val users = MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val repository = outbox(fixture)
      def round(index: Int): IO[(Boolean, Boolean)] = {
        val account = User(
          UserId(UUID.randomUUID()),
          None,
          s"Racing candidate ${UUID.randomUUID()}",
          UserRole.Candidate,
          Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
          now
        )
        val value = event(account.id, index)
        def delete: IO[Unit] = users
          .deleteAccount(account.id, now, s"deleted-${account.id.value}", MutationWriteContext.directWrite)
          .value
          .flatMap {
            case Right(_)                       => IO.unit
            case Left(RepositoryError.Conflict) => IO.sleep(20.millis) *> delete
            case Left(error)                    => IO.raiseError(new AssertionError(s"deletion failed: $error"))
          }
        for {
          _ <- MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.Users,
            MongoHiringCodecs.user(account)
          )
          _ <- seed(fixture, List(value))
          (claimed, _) <- (claim(repository, s"racer-$index", now.plusSeconds(1), limit = 5), delete).parTupled
          after <- row(fixture, value.eventId)
          lease <- fence(fixture, account.id)
        } yield {
          val suppressed = after.getString(MongoFields.State) == "Failed"
          // Claimed first: the in-flight event keeps its lease and the deletion drains it. Otherwise the event is
          // suppressed and the deleted subject never holds a lease.
          assert(claimed.nonEmpty || suppressed || after.getString(MongoFields.State) == "Retryable", clue(after))
          if (suppressed) {
            assertEquals(after.getString(MongoFields.LastError), "SUBJECT_DELETED")
            assert(lease.forall(!_.containsKey(MongoFields.LeaseToken)), clue(lease))
          }
          assert(lease.exists(_.getBoolean("deleted", false)), clue(lease))
          (claimed.nonEmpty, suppressed)
        }
      }
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        results <- (0 until 15).toList.traverse(round)
      } yield println(
        s"AC-08.4 claim racing deletion over 15 rounds: claimedFirst=${results.count(_._1)} suppressed=${results.count(_._2)}"
      )
    }
  }

  /** A job event of an existing aggregate; distinct actors keep the per-user subject leases out of the picture. */
  private def eventOn(job: UUID, actor: UserId, at: Instant): OperationalEventEnvelope =
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_UPDATED,
      at,
      OperationalAggregateType.Job,
      job.toString,
      actor,
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Updated,
        OperationalEventPayload.JobSnapshot(job, List("Scala"), JobStatus.Open)
      )
    )

  private def aggregateEvents(job: UUID, count: Int, from: Int = 0): List[OperationalEventEnvelope] =
    (from until from + count).toList.map(index => eventOn(job, UserId(UUID.randomUUID()), now.plusMillis(index.toLong)))

  private def succeeded(result: cats.data.EitherT[IO, RepositoryError, Unit]): IO[Unit] =
    result.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"repository failed: $error")), IO.pure))

  test("DDR-06.1 a later event of an aggregate is not claimed while an earlier one waits for retry") {
    mongoResource.use { fixture =>
      val job = UUID.randomUUID()
      val List(a, b) = aggregateEvents(job, 2)
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(a, b))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 10)
        // The send of A failed transiently: it is released for retry in the future and its subject lease is gone.
        _ <- succeeded(
          repository.releaseForRetry(a.eventId, first.head.leaseToken, now.plusSeconds(2), now.plusSeconds(60))
        )
        whileRetrying <- claim(repository, "w", now.plusSeconds(3), limit = 10)
        retried <- claim(repository, "w", now.plusSeconds(61), limit = 10)
        _ <- succeeded(
          repository.markPublished(a.eventId, retried.head.leaseToken, now.plusSeconds(62), now.plusSeconds(3600))
        )
        afterPublish <- claim(repository, "w", now.plusSeconds(63), limit = 10)
      } yield {
        assertEquals(ids(first), List(a.eventId), "B is not claimed in the same batch as the earlier A")
        assertEquals(whileRetrying, Nil)
        assertEquals(ids(retried), List(a.eventId))
        assertEquals(ids(afterPublish), List(b.eventId))
      }
    }
  }

  test("DDR-06.1 a later event is not claimed while the lease of an earlier in-flight event is still running") {
    mongoResource.use { fixture =>
      val job = UUID.randomUUID()
      val List(a, b) = aggregateEvents(job, 2)
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(a, b))
        first <- claim(repository, "slow", now.plusSeconds(1), limit = 1)
        // A's lease is held (its subject lease protects only the actor); B must still wait.
        held <- claim(repository, "other", now.plusSeconds(5), limit = 10)
        // The holder died: the expired lease makes A claimable again, and A, not B, is the event returned.
        _ <- expireClaims(fixture, first)
        expired <- claim(repository, "other", now.plusSeconds(1 + lease.toSeconds), limit = 10)
      } yield {
        assertEquals(ids(first), List(a.eventId))
        assertEquals(held, Nil)
        assertEquals(ids(expired), List(a.eventId))
      }
    }
  }

  test("DDR-06.1 a failed event does not block its aggregate and stays visible as Failed") {
    mongoResource.use { fixture =>
      val job = UUID.randomUUID()
      val List(a, b) = aggregateEvents(job, 2)
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(a, b))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 10)
        _ <- succeeded(repository.markFailed(a.eventId, first.head.leaseToken, now.plusSeconds(2), "RetriesExhausted"))
        next <- claim(repository, "w", now.plusSeconds(3), limit = 10)
        failed <- row(fixture, a.eventId).map(_.getString(MongoFields.State))
      } yield {
        assertEquals(ids(next), List(b.eventId))
        assertEquals(failed, "Failed")
      }
    }
  }

  test("DDR-06.2 other aggregates are claimed while one aggregate is blocked") {
    mongoResource.use { fixture =>
      val blockedJob = UUID.randomUUID()
      val List(a, b) = aggregateEvents(blockedJob, 2)
      val c = eventOn(UUID.randomUUID(), UserId(UUID.randomUUID()), now.plusMillis(5))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(a, b))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 1)
        _ <- succeeded(
          repository.releaseForRetry(a.eventId, first.head.leaseToken, now.plusSeconds(2), now.plusSeconds(600))
        )
        _ <- seed(fixture, List(c))
        next <- claim(repository, "w", now.plusSeconds(3), limit = 10)
      } yield assertEquals(ids(next), List(c.eventId))
    }
  }

  test("DDR-06.2 a full page of blocked rows does not hide an event of another aggregate behind it") {
    mongoResource.use { fixture =>
      val blockedJob = UUID.randomUUID()
      val a = eventOn(blockedJob, UserId(UUID.randomUUID()), now)
      val waiting = aggregateEvents(blockedJob, MongoProducerRegistrations.BatchSize + 6, from = 1)
      val free = eventOn(UUID.randomUUID(), UserId(UUID.randomUUID()), now.plusSeconds(10))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(a))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 1)
        _ <- succeeded(
          repository.releaseForRetry(a.eventId, first.head.leaseToken, now.plusSeconds(2), now.plusSeconds(600))
        )
        _ <- seed(fixture, waiting :+ free)
        next <- claim(repository, "w", now.plusSeconds(20), limit = 1)
      } yield {
        assert(waiting.size > MongoProducerRegistrations.BatchSize)
        assertEquals(ids(next), List(free.eventId))
      }
    }
  }

  test("DDR-06.3 the Kafka record key of an event is its aggregate id") {
    val job = UUID.randomUUID()
    val value = eventOn(job, UserId(UUID.randomUUID()), now)
    assertEquals(value.partitionKey, job.toString)
    assertEquals(stored(value).getString(MongoFields.PartitionKey), job.toString)
  }

  private def setState(
      fixture: MongoAccessEvaluationSupport.Fixture,
      eventIds: List[UUID],
      state: String
  ): IO[Unit] =
    MongoAccessEvaluationSupport
      .command(
        fixture.database,
        new Document("update", MongoCollections.EventOutbox).append(
          "updates",
          java.util.List.of(
            new Document("q", new Document(MongoFields.Id, new Document("$in", eventIds.map(_.toString).asJava)))
              .append("u", new Document("$set", new Document(MongoFields.State, state)))
              .append("multi", true)
          )
        )
      )
      .void

  test("DDR-06.1 the earliest-pending read is an index-only DISTINCT_SCAN with no SORT stage") {
    mongoResource.use { fixture =>
      val jobs = List.fill(20)(UUID.randomUUID())
      val perJob = jobs.zipWithIndex.map { case (job, jobIndex) => aggregateEvents(job, 10, from = jobIndex * 10) }
      val values = perJob.flatten
      // Three events per aggregate stay pending; the other seven are terminal and not in the partial index.
      val terminal = perJob.flatMap(_.drop(3)).map(_.eventId)
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, values)
        _ <- setState(fixture, terminal, "Published")
        _ <- fixture.commands.clear
        // The production claim issues the read; its recorded pipeline is the one explained, so the two cannot drift.
        _ <- claim(repository, "w", now.plusSeconds(1), limit = 20)
        issuedCommands <- fixture.commands.snapshot
        pipeline = issuedCommands
          .find(command => command.getFirstKey == "aggregate" && command.containsKey("pipeline"))
          .map(_.getArray("pipeline"))
          .getOrElse(fail("the claim issued no aggregate command"))
        explained <- MongoAccessEvaluationSupport.command(
          fixture.database,
          new Document(
            "explain",
            new Document("aggregate", MongoCollections.EventOutbox)
              .append("pipeline", pipeline)
              .append("cursor", new Document())
          ).append("verbosity", "executionStats")
        )
      } yield {
        val plan = explained.toJson
        def stat(name: String): Long =
          s""""$name": (\\d+)""".r.findFirstMatchIn(plan).map(_.group(1).toLong).getOrElse(-1L)
        println(
          s"DDR-06.1 explain earliest-pending rows=${values.size} keys=${jobs.size} " +
            s"keysExamined=${stat("totalKeysExamined")} docsExamined=${stat("totalDocsExamined")} " +
            s"returned=${stat("nReturned")}"
        )
        assert(plan.contains("DISTINCT_SCAN"), clue(plan))
        assert(plan.contains(MongoIndexNames.EventOutboxAggregateOrder), clue(plan))
        assert(!plan.contains("COLLSCAN"), clue(plan))
        assert(!plan.contains("\"SORT\""), clue(plan))
        assertEquals(stat("totalDocsExamined"), 0L)
        assert(stat("totalKeysExamined") <= jobs.size.toLong * 3, clue(plan))
      }
    }
  }

  test("DDR-06.1 a blocked event is reached again after its predecessor completes while new events keep arriving") {
    mongoResource.use { fixture =>
      val job = UUID.randomUUID()
      val List(a, b) = aggregateEvents(job, 2)
      // Arrivals sit between B and A's retry time; `late` sits after it, beyond anything a cursor that passed B would
      // have to revisit: only a cursor that never passed B returns B before `late`.
      val arrivals = (1 to 3).toList.map(i =>
        eventOn(UUID.randomUUID(), UserId(UUID.randomUUID()), now.plusSeconds(2).plusMillis(i.toLong))
      )
      val late = eventOn(UUID.randomUUID(), UserId(UUID.randomUUID()), now.plusSeconds(5).plusMillis(500))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(a, b))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 1)
        _ <- succeeded(
          repository.releaseForRetry(a.eventId, first.head.leaseToken, now.plusSeconds(1), now.plusSeconds(5))
        )
        // New events keep arriving and are claimed one by one; the worker passes the blocked B every time.
        passed <- arrivals.traverse { arrival =>
          for {
            _ <- seed(fixture, List(arrival))
            claimed <- claim(repository, "w", now.plusSeconds(4), limit = 1)
            _ <- succeeded(
              repository.markPublished(
                arrival.eventId,
                claimed.head.leaseToken,
                now.plusSeconds(4),
                now.plusSeconds(3600)
              )
            )
          } yield claimed
        }
        retried <- claim(repository, "w", now.plusSeconds(6), limit = 1)
        _ <- succeeded(
          repository.markPublished(a.eventId, retried.head.leaseToken, now.plusSeconds(6), now.plusSeconds(3600))
        )
        _ <- seed(fixture, List(late))
        // The next claim returns B, ahead of the newer arrival.
        next <- claim(repository, "w", now.plusSeconds(7), limit = 1)
      } yield {
        assertEquals(passed.map(ids), arrivals.map(value => List(value.eventId)))
        assertEquals(ids(retried), List(a.eventId))
        assertEquals(ids(next), List(b.eventId))
      }
    }
  }

  test(
    "DDR-06.1 an event skipped for a shared subject is reached again although every follower of its aggregate is blocked"
  ) {
    mongoResource.use { fixture =>
      // One recruiter acts on two jobs. X (job 2) comes first, so the claim of [X, A, B] takes X and skips A, which
      // shares X's subject; B waits for A. A cursor that passed A would find only the blocked B after it, would not
      // wrap to the start and would never return A (the stall of the acceptance story's outbox).
      val recruiter = UserId(UUID.randomUUID())
      val jobOne = UUID.randomUUID()
      val x = eventOn(UUID.randomUUID(), recruiter, now)
      val a = eventOn(jobOne, recruiter, now.plusMillis(1))
      val b = eventOn(jobOne, recruiter, now.plusMillis(2))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(x, a, b))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 10)
        _ <- succeeded(
          repository.markPublished(x.eventId, first.head.leaseToken, now.plusSeconds(2), now.plusSeconds(60))
        )
        second <- claim(repository, "w", now.plusSeconds(3), limit = 10)
        _ <- succeeded(
          repository.markPublished(a.eventId, second.head.leaseToken, now.plusSeconds(4), now.plusSeconds(60))
        )
        third <- claim(repository, "w", now.plusSeconds(5), limit = 10)
      } yield {
        assertEquals(ids(first), List(x.eventId), "A shares X's subject, so only X is leased")
        assertEquals(ids(second), List(a.eventId), "A is not lost behind its blocked follower")
        assertEquals(ids(third), List(b.eventId))
      }
    }
  }

  test("DDR-06.1 the in-transaction check alone refuses a later event of a pending aggregate") {
    mongoResource.use { fixture =>
      val job = UUID.randomUUID()
      val List(a, b) = aggregateEvents(job, 2)
      val repository = outbox(fixture)
      def candidate(value: OperationalEventEnvelope) =
        MongoOutboxClaimBatch.Candidate(value.eventId.toString, List(value.actorId.value.toString), Some(job.toString))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(a, b))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 1)
        _ <- succeeded(
          repository.releaseForRetry(a.eventId, first.head.leaseToken, now.plusSeconds(2), now.plusSeconds(600))
        )
        // A stale page read could still offer B; the claim transaction refuses it without the page filter.
        stale <- repository
          .claimCandidates("w", "hiring-publisher-w", now.plusSeconds(3), 30.seconds, List(candidate(b)))
          .value
        state <- row(fixture, b.eventId).map(_.getString(MongoFields.State))
        // Both offered together: only the earlier one is leased.
        both <- repository
          .claimCandidates(
            "w",
            "hiring-publisher-w",
            now.plusSeconds(601),
            30.seconds,
            List(candidate(a), candidate(b))
          )
          .value
      } yield {
        assertEquals(stale.map(ids), Right(Nil))
        assertEquals(state, "Retryable")
        assertEquals(both.map(ids), Right(List(a.eventId)))
      }
    }
  }

  test("DDR-06.1 events with an equal occurredAt are ordered by event id") {
    mongoResource.use { fixture =>
      val job = UUID.randomUUID()
      def at(n: Long) =
        eventOn(job, UserId(UUID.randomUUID()), now).copy(eventId = new UUID(0L, n))
      val (small, large) = (at(1L), at(2L))
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        // Inserted in the opposite order: the claim order is the key, not the insertion.
        _ <- seed(fixture, List(large, small))
        first <- claim(repository, "w", now.plusSeconds(1), limit = 10)
        _ <- succeeded(
          repository.markPublished(small.eventId, first.head.leaseToken, now.plusSeconds(2), now.plusSeconds(3600))
        )
        second <- claim(repository, "w", now.plusSeconds(3), limit = 10)
      } yield {
        assertEquals(ids(first), List(small.eventId))
        assertEquals(ids(second), List(large.eventId))
      }
    }
  }

  test("DDR-06.1 two workers claiming at once never release a later event of an aggregate before its predecessor") {
    mongoResource.use { fixture =>
      val repository = outbox(fixture)
      def round(n: Int): IO[Unit] = {
        val pairs = List.fill(10)(UUID.randomUUID()).map(job => aggregateEvents(job, 2, from = n * 100))
        val firsts = pairs.map(_.head.eventId).toSet
        val seconds = pairs.map(_(1).eventId).toSet
        for {
          _ <- seed(fixture, pairs.flatten)
          claimed <- IO.both(
            claim(repository, "left", now.plusSeconds(1), limit = 20),
            claim(repository, "right", now.plusSeconds(1), limit = 20)
          )
          all = ids(claimed._1) ++ ids(claimed._2)
        } yield {
          assert(all.forall(firsts), clue(all.filter(seconds)))
          assertEquals(all.distinct.size, all.size, "no event is claimed by both workers")
          assert(all.nonEmpty)
        }
      }
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *> (0 until 3).toList.traverse_(round)
    }
  }

  test("DDR-06.3 a claim read back from Mongo carries the aggregate id into the producer record key") {
    mongoResource.use { fixture =>
      val job = UUID.randomUUID()
      val value = eventOn(job, UserId(UUID.randomUUID()), now)
      val repository = outbox(fixture)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- seed(fixture, List(value))
        claimed <- claim(repository, "w", now.plusSeconds(1), limit = 1)
      } yield {
        val record = com.example.graphQL.cats.infrastructure.kafka.OperationalEventKafkaRuntime
          .producerRecord(OperationalEventEnvelope.Topic, claimed.head)
        assertEquals(claimed.head.partitionKey, value.aggregateId)
        assertEquals(record.key, job.toString)
        assertEquals(record.topic, OperationalEventEnvelope.Topic)
      }
    }
  }
}
