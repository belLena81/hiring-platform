package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, FiberIO, IO, Ref}
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

import com.mongodb.ReadPreference
import org.bson.{BsonDocument, BsonValue, Document}

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** DDR-01: a sole recruiter's sequential status changes (the user fence `$inc` on `outbox_subject_fences`) are not
  * refused with `Conflict` while the outbox relay cycles the same subject's lease (acquire, renew, release). The relay
  * writes the same document, so collisions cost a retry, but its writes are short. This is a bounded-contention
  * observation, not a guarantee: docs/mongodb-design.md ("Measurements: contention on the shared subject fence")
  * records one refusal (refused=1 of 60) observed on 2026-11-12.
  */
final class SubjectFenceRelayContentionIntegrationSpec extends MongoIntegrationSuite {
  // The scenario normally takes a few seconds. A stall must fail fast and say where it stopped: the scenario
  // deadline fires first (with a progress snapshot and server evidence), the suite timeout is only a backstop.
  private val scenarioDeadline = 60.seconds
  override val munitIOTimeout: FiniteDuration = 90.seconds
  private val evidenceBudget = 5.seconds
  private val evidenceCommandBudget = 2500.millis
  private val relayStopBound = 20.seconds
  private val slowOperationSeconds = 1
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

  private final case class Outcome(refused: Int, accepted: Int, relayCycles: Int, cyclesAfterStop: Int)

  private final class ScenarioStalled(message: String) extends RuntimeException(message)
  private final class ScenarioFailed(message: String, cause: Throwable) extends RuntimeException(message, cause)

  /** What one actor is doing now, how long it has been doing it, what it finished, and its last error. */
  private final case class ActorState(
      operation: String,
      since: FiniteDuration,
      completed: Map[String, Int],
      lastError: Option[String],
      stopRequestedAt: Option[FiniteDuration] = None,
      cycleEndedAt: Option[FiniteDuration] = None
  )

  /** Shared, process-local progress of the accepter and the relays, read only to diagnose a stall or failure. */
  private final class Progress(state: Ref[IO, Map[String, ActorState]], origin: FiniteDuration) {
    private def relative(at: FiniteDuration): String = s"${at.toMillis}ms(+${(at - origin).toMillis}ms from start)"

    private def kind(operation: String): String = operation.takeWhile(_ != '#')

    def track[A](actor: String, operation: String)(action: IO[A]): IO[A] =
      IO.monotonic.flatMap { started =>
        upsert(actor, started)(_.copy(operation = operation, since = started)) *>
          action.flatTap(_ => finish(actor, operation)).onError { case error =>
            note(actor, s"$operation: ${error.getClass.getName}: ${error.getMessage}")
          }
      }

    /** Expected `Left` outcomes are recorded but do not change what the test asserts. */
    def trackEither[E, A](actor: String, operation: String)(action: IO[Either[E, A]]): IO[Either[E, A]] =
      track(actor, operation)(action).flatTap(_.fold(error => note(actor, s"$operation: $error"), _ => IO.unit))

    private def upsert(actor: String, now: FiniteDuration)(change: ActorState => ActorState): IO[Unit] =
      state.update(_.updatedWith(actor) { current =>
        Some(change(current.getOrElse(ActorState("not started", now, Map.empty, None))))
      })

    // `since` restarts when an operation ends, so "after X" counts from the end of X.
    private def finish(actor: String, operation: String): IO[Unit] =
      IO.monotonic.flatMap { now =>
        upsert(actor, now) { current =>
          current.copy(
            operation = s"after $operation",
            since = now,
            completed = current.completed.updatedWith(kind(operation))(count => Some(count.getOrElse(0) + 1))
          )
        }
      }

    /** Recorded before the stop flag is set, so a relay that sees the flag always ends its cycle at or after it. */
    def requestStop(actors: List[String]): IO[Unit] =
      IO.monotonic.flatMap(now => actors.traverse_(actor => upsert(actor, now)(_.copy(stopRequestedAt = Some(now)))))

    /** A relay ended a cycle, or left its loop after seeing the stop flag. */
    def cycleEnded(actor: String): IO[Unit] =
      IO.monotonic.flatMap(now => upsert(actor, now)(_.copy(cycleEndedAt = Some(now))))

    def operation(actor: String): IO[String] = state.get.map(_.get(actor).fold("unknown")(_.operation))

    def note(actor: String, text: String): IO[Unit] =
      state.update(_.updatedWith(actor)(_.map(_.copy(lastError = Some(text)))))

    def snapshot: IO[String] =
      (IO.monotonic, state.get).mapN { (now, all) =>
        all.toList
          .sortBy(_._1)
          .map { case (actor, current) =>
            val counts = current.completed.toList.sortBy(_._1).map((name, n) => s"$name=$n").mkString(",")
            s"$actor op=${current.operation} elapsedMs=${(now - current.since).toMillis} " +
              s"completed={$counts} lastError=${current.lastError.getOrElse("-")}" +
              current.stopRequestedAt.fold("") { stopAt =>
                val inside = current.cycleEndedAt.forall(_ < stopAt)
                val ended = current.cycleEndedAt.fold("-")(relative)
                s" stopRequestedAt=${relative(stopAt)} cycleEndedAt=$ended" +
                  (if (inside) s" STILL-IN-OPERATION ${(now - stopAt).toMillis}ms after stop"
                   else " finished-after-stop")
              }
          }
          .mkString("\n  ")
      }
  }

  private object Progress {
    def create: IO[Progress] =
      (Ref.of[IO, Map[String, ActorState]](Map.empty), IO.monotonic).mapN(new Progress(_, _))
  }

  /** Best-effort server evidence for a stall, read on a separate client so that pool or session exhaustion on the
    * scenario's client cannot blank it. Hard-capped by `timeoutAndForget` (it does not wait for cancellation), errors
    * swallowed, only counters and operation metadata.
    */
  private def serverEvidence(scenarioFixture: MongoAccessEvaluationSupport.Fixture): IO[String] = {
    def describe(op: BsonValue): String =
      List("op", "ns", "secs_running", "waitingForLock", "planSummary", "truncated")
        .flatMap(key => Try(op.asDocument().get(key)).toOption.flatMap(Option(_)).map(value => s"$key=$value"))
        .mkString(",")
    def section(name: String)(read: IO[String]): IO[String] =
      read
        .timeoutAndForget(evidenceCommandBudget)
        .attempt
        .map(
          _.fold(error => s"$name unavailable (${error.getClass.getSimpleName})", value => s"$name: $value")
        )
    def child(document: BsonDocument, key: String): String =
      Try(document.getDocument(key).toJson).getOrElse("absent")
    def gather(fixture: MongoAccessEvaluationSupport.Fixture): IO[String] =
      for {
        admin <- fixture.client.getDatabase("admin")
        slow <- section("currentOp(active, running >= " + slowOperationSeconds + "s)")(
          admin
            .runCommand(
              new Document("currentOp", 1)
                .append("active", true)
                .append("secs_running", new Document("$gte", slowOperationSeconds)),
              ReadPreference.primary()
            )
            .map { reply =>
              val result = BsonDocument.parse(reply.toJson)
              val ops = Try(result.getArray("inprog").getValues.asScala.toList).getOrElse(Nil)
              s"${ops.size} " + ops.take(10).map(describe).mkString("[", " | ", "]")
            }
        )
        lock <- section("serverStatus")(
          admin
            .runCommand(new Document("serverStatus", 1), ReadPreference.primary())
            .map { reply =>
              val value = BsonDocument.parse(reply.toJson)
              s"globalLock.currentQueue: " +
                s"${Try(child(value.getDocument("globalLock"), "currentQueue")).getOrElse("absent")}; " +
                s"connections: ${child(value, "connections")}"
            }
        )
      } yield s"$slow; $lock"
    MongoAccessEvaluationSupport
      .isolatedFixture(scenarioFixture)
      .use(gather)
      .timeoutAndForget(evidenceBudget)
      .handleError(error => s"unavailable (${error.getClass.getSimpleName}: ${error.getMessage})")
  }

  private def run(relays: Int): IO[(Outcome, String)] =
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
      def relayCycle(actor: String, cycles: Ref[IO, Int], progress: Progress): IO[Unit] =
        progress
          .trackEither(actor, "claim")(
            outbox
              .claim("relay", ProducerGenerationFixtures.of("hiring-publisher-relay"), at.plusSeconds(1000), lease, 1)
              .value
          )
          .flatMap {
            case Right(claimed) if claimed.nonEmpty =>
              val head = claimed.head
              progress.trackEither(actor, "renewLease")(
                outbox.renewLease(head.event.eventId, head.leaseToken, head.subjectIds, lease).value
              ) *>
                progress.trackEither(actor, "markPublished")(
                  outbox
                    .markPublished(head.event.eventId, head.leaseToken, at.plusSeconds(1001), at.plusSeconds(4000))
                    .value
                ) *> cycles.update(_ + 1)
            case _ => progress.track(actor, "idle")(IO.sleep(5.millis))
          }

      def scenario(progress: Progress): IO[Outcome] =
        for {
          _ <- progress.track("setup", "seed data") {
            for {
              _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
              _ <- (recruiter :: candidates).traverse_(value =>
                MongoRepositoryTestSupport
                  .insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(value))
              )
              _ <- MongoRepositoryTestSupport
                .insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(job))
              _ <- apps.traverse_(value =>
                MongoRepositoryTestSupport
                  .insertOne(fixture.database, MongoCollections.Applications, MongoHiringCodecs.application(value))
              )
              _ <- List
                .tabulate(backlog)(index => jobEvent(recruiter.id, index))
                .traverse_(value =>
                  MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, stored(value))
                )
            } yield ()
          }
          cycles <- Ref.of[IO, Int](0)
          stop <- Ref.of[IO, Boolean](false)
          relayFailed <- Deferred[IO, Throwable]
          relayNames = List.tabulate(relays)(index => s"relay-${index + 1}")
          // Two cycles first, so the relay has demonstrably taken and released the subject lease before the race.
          _ <- relayCycle("warmup", cycles, progress) *> relayCycle("warmup", cycles, progress)
          // Cycles completed while the accepter ran (read before the stop flag is set); the total after the relays
          // stopped is reported separately as `cyclesAfterStop`.
          cyclesAtStop <- Ref.of[IO, Option[Int]](None)
          accepter = apps.zipWithIndex.traverse { (app, index) =>
            progress.trackEither("accepter", s"accept#${index + 1}")(accept(app)) <*
              progress.track("accepter", "pause")(IO.sleep(userPause))
          }
          // Starting the fibers and registering their cleanup is uncancelable, so a watchdog cancel cannot leave
          // started relays unmanaged; only the wait is cancelable.
          results <- IO.uncancelable { poll =>
            for {
              // Relays are never cancelled mid-operation: they check the stop flag between cycles only.
              relayFibers <- relayNames.traverse { name =>
                relayLoop(name, cycles, progress, stop).attempt
                  .flatTap(_.fold(error => relayFailed.complete(error).void, _ => IO.unit))
                  .start
                  .map(name -> _)
              }
              accepterFiber <- accepter
                .guarantee(
                  cycles.get.flatMap(count => cyclesAtStop.set(Some(count))) *>
                    progress.requestStop(relayNames) *> stop.set(true)
                )
                .start
              cancelAll = (accepterFiber.cancel :: relayFibers.map(_._2.cancel)).traverse_(_.start.void)
              finished <- poll(for {
                raced <- IO.race(relayFailed.get, accepterFiber.join)
                // A relay exception fails the scenario at once; everything else is cancelled in the background.
                finished <- raced.fold(
                  error => cancelAll *> IO.raiseError[List[Either[RepositoryError, Unit]]](error),
                  _.embed(IO.raiseError(new IllegalStateException("accepter cancelled"))).onError(_ => cancelAll)
                )
                _ <- awaitRelays(relayFibers, progress)
              } yield finished).onCancel(cancelAll)
            } yield finished
          }
          total <- cycles.get
          duringChanges <- cyclesAtStop.get.map(_.getOrElse(total))
        } yield Outcome(
          results.count(_ == Left(RepositoryError.Conflict)),
          results.count(_.isRight),
          duringChanges,
          total
        )

      def relayLoop(actor: String, cycles: Ref[IO, Int], progress: Progress, stop: Ref[IO, Boolean]): IO[Unit] =
        stop.get.ifM(
          progress.cycleEnded(actor),
          relayCycle(actor, cycles, progress) *> progress.cycleEnded(actor) *> relayLoop(actor, cycles, progress, stop)
        )

      def errorText(error: Throwable): String = s"${error.getClass.getName}: ${error.getMessage}"

      // Joins the relays after the accepter completed. A relay still inside an operation after the bound is
      // reported with the progress snapshot and server evidence, and only then cancelled in the background.
      def awaitRelays(relayFibers: List[(String, FiberIO[Either[Throwable, Unit]])], progress: Progress): IO[Unit] =
        relayFibers
          .parTraverse((name, fiber) => fiber.join.timeoutAndForget(relayStopBound).attempt.map(name -> _))
          .flatMap { joined =>
            val stalled = joined.collect { case (name, Left(_: TimeoutException)) => name }
            if (stalled.isEmpty)
              joined.traverse_ {
                case (_, Right(outcome)) =>
                  outcome.embed(IO.raiseError(new IllegalStateException("relay cancelled"))).rethrow
                case (_, Left(error)) => IO.raiseError(error)
              }
            else
              for {
                operations <- stalled.traverse(name => progress.operation(name).map(op => s"$name (op=$op)"))
                snapshot <- progress.snapshot
                evidence <- serverEvidence(fixture)
                ended <- joined.collect { case (name, Right(outcome)) => name -> outcome }.traverse { (name, outcome) =>
                  outcome
                    .embed(IO.pure(Left(new IllegalStateException("relay cancelled"))))
                    .map(result => result.left.toOption.map(error => s"$name ended with ${errorText(error)}"))
                }
                failedRelays = joined.collect {
                  case (name, Left(error)) if !stalled.contains(name) =>
                    s"$name ended with ${errorText(error)}"
                } ++ ended.flatten
                report =
                  (stalled.zip(operations).map { (name, described) =>
                    s"$name did not finish its current cycle (claim/renewLease/markPublished) within " +
                      s"${relayStopBound.toSeconds} s after the accepter completed " +
                      described.drop(name.length + 1)
                  } ++ failedRelays).mkString("DDR-01 scenario stalled: ", "; ", "") +
                    s"\nProgress:\n  $snapshot\nMongoDB: $evidence"
                _ <- IO.consoleForIO.errorln(report).attempt.void
                _ <- relayFibers.filter((name, _) => stalled.contains(name)).traverse_(_._2.cancel.start.void)
                _ <- IO.raiseError[Unit](new ScenarioStalled(report))
              } yield ()
          }

      Progress.create.flatMap { progress =>
        val body = scenario(progress).flatMap(outcome => progress.snapshot.map(outcome -> _)).handleErrorWith {
          case stalled: ScenarioStalled => IO.raiseError(stalled)
          case error                    =>
            progress.snapshot.flatMap { snapshot =>
              IO.raiseError(
                new ScenarioFailed(
                  s"DDR-01 scenario failed with ${error.getClass.getName}: ${error.getMessage}\nProgress:\n  $snapshot",
                  error
                )
              )
            }
        }
        // The watchdog reads the evidence while the stalled work is still running and the client still open. It
        // prints the report to stderr before failing, so the evidence survives even if cancelling the stuck work
        // never completes (non-cancelable releases or driver callbacks) and only the suite timeout fires.
        val watchdog: IO[(Outcome, String)] =
          IO.sleep(scenarioDeadline) *> (progress.snapshot, serverEvidence(fixture)).flatMapN { (snapshot, evidence) =>
            val report =
              s"DDR-01 scenario stalled: not finished within $scenarioDeadline\n" +
                s"Progress:\n  $snapshot\nMongoDB: $evidence"
            IO.consoleForIO.errorln(report).attempt.void *> IO.raiseError(new ScenarioStalled(report))
          }
        // racePair so the loser is cancelled in the background and the winner's result returns at once.
        IO.racePair(watchdog, body).flatMap {
          case Left((watchdogOutcome, bodyFiber)) =>
            bodyFiber.cancel.start *>
              watchdogOutcome.embed(IO.raiseError(new IllegalStateException("watchdog cancelled")))
          case Right((watchdogFiber, bodyOutcome)) =>
            watchdogFiber.cancel.start *>
              bodyOutcome.embed(IO.raiseError(new IllegalStateException("scenario cancelled")))
        }
      }
    }

  test("DDR-01 a sole recruiter's status changes are not refused while three relays cycle the subject lease") {
    run(relays = 3).map { (outcome, snapshot) =>
      assertEquals(outcome.refused, 0, clues(outcome, snapshot))
      assertEquals(outcome.accepted, changes, clues(outcome, snapshot))
      assert(outcome.relayCycles > 2, clues(outcome, snapshot))
    }
  }
}
