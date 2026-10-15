package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** EPS-05.3 and EPS-05.4: idle polling and the heartbeat-versus-publish race, observed in virtual time. */
final class InterviewWorkflowWorkerTimingSpec extends CatsEffectSuite {
  private val observedAt = Instant.parse("2026-10-06T12:00:00Z")
  private val workflow = InterviewWorkflow(
    InterviewWorkflowId(new UUID(0L, 1L)),
    com.example.graphQL.cats.domain.model.Identifiers.ApplicationId(new UUID(0L, 2L)),
    UserId(new UUID(0L, 3L)),
    UserId(new UUID(0L, 4L)),
    InterviewInterval(observedAt.plusSeconds(600), observedAt.plusSeconds(1200)),
    observedAt.plusSeconds(300),
    new UUID(0L, 5L),
    0L,
    InterviewWorkflowPhase.ReservationPending,
    Set.empty,
    UserId(new UUID(0L, 4L))
  )
  private val command = InterviewWorkflowCommandRecord(
    workflow.id,
    "reservation-step",
    0L,
    InterviewWorkflow.initialCommand(workflow),
    InterviewWorkflowCommandState.Published,
    1,
    observedAt,
    observedAt
  )
  private val claim = ClaimedInterviewWorkflowCommand(
    command.copy(state = InterviewWorkflowCommandState.Claimed, publicationAttempts = 0),
    "worker",
    new UUID(0L, 77L),
    observedAt.plusSeconds(60)
  )

  private def passes(sequence: List[InterviewPublicationPass], pollInterval: FiniteDuration) =
    for {
      remaining <- Ref.of[IO, List[InterviewPublicationPass]](sequence)
      times <- Ref.of[IO, Vector[FiniteDuration]](Vector.empty)
      publish = IO.monotonic.flatMap(at => times.update(_ :+ at)) *>
        remaining.modify(list => (list.drop(1), list.headOption.getOrElse(InterviewPublicationPass.Idle)))
      _ <- InterviewWorkflowWorker.publicationLoop[IO](publish, pollInterval).take(sequence.size.toLong).compile.drain
      observed <- times.get
    } yield observed

  test("EPS-05.3 idle and deferred passes wait pollInterval and a full pass continues without waiting") {
    val program = passes(
      List(
        InterviewPublicationPass.Full,
        InterviewPublicationPass.Idle,
        InterviewPublicationPass.Deferred,
        InterviewPublicationPass.Idle
      ),
      10.seconds
    )
    TestControl
      .executeEmbed(program)
      .map(times => assertEquals(times, Vector(0.seconds, 0.seconds, 10.seconds, 20.seconds)))
  }

  private final case class Observed(renewals: Int, published: Int, retried: Int, cancelled: Boolean)

  /** Claim lease 30 s, so the heartbeat renews every 10 s; `renewals(n)` answers the n-th renewal (zero based). */
  private def publication(sendFor: FiniteDuration, renewals: Int => Either[RepositoryError, Boolean]) =
    for {
      renewed <- Ref.of[IO, Int](0)
      published <- Ref.of[IO, Int](0)
      retried <- Ref.of[IO, Int](0)
      cancelled <- Ref.of[IO, Boolean](false)
      store = new TestInterviewWorkflowRepository {
        private var served = false
        override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
          RepositoryF.lift(IO { val due = if (served) Nil else List(claim); served = true; due })
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryF.at[IO].pure(Some(workflow))
        override def authorizePublication(
            value: ClaimedInterviewWorkflowCommand,
            generation: InterviewPublisherGeneration,
            now: Instant
        ) = RepositoryF.at[IO].pure(true)
        override def renewPublication(value: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant) =
          RepositoryF.fromFEither(renewed.getAndUpdate(_ + 1).map(renewals))
        override def markPublished(value: ClaimedInterviewWorkflowCommand, at: Instant) =
          RepositoryF.lift(published.update(_ + 1))
        override def retry(
            value: ClaimedInterviewWorkflowCommand,
            now: Instant,
            availableAt: Instant,
            failureCode: InterviewFailureCode
        ) = RepositoryF.lift(retried.update(_ + 1))
      }
      transport = new InterviewTransport[IO] {
        def generationFor(value: InterviewMessage) =
          InterviewPublisherGeneration(InterviewPublisherRole.Orchestrator, new UUID(0L, 78L))
        def publish(value: InterviewMessage) = IO.sleep(sendFor).onCancel(cancelled.set(true))
      }
      worker = new InterviewWorkflowWorker[IO](
        store,
        new UnusedProviders().calendar,
        new UnusedProviders().notifications,
        InterviewWorkerSettings(
          "worker",
          1.second,
          30.seconds,
          10.seconds,
          5,
          1.second,
          30.seconds,
          publicationBatchSize = 1
        ),
        Diagnostics.noop[IO],
        IO.pure(observedAt)
      )
      _ <- worker.publishDue(transport)
      r <- renewed.get
      p <- published.get
      t <- retried.get
      c <- cancelled.get
    } yield Observed(r, p, t, c)

  test("EPS-05.4 a publish that finishes before the first heartbeat is marked published and the heartbeat never runs") {
    TestControl
      .executeEmbed(publication(sendFor = 5.seconds, renewals = _ => Right(true)))
      .map(observed => assertEquals(observed, Observed(renewals = 1, published = 1, retried = 0, cancelled = false)))
  }

  test("EPS-05.4 a lost lease at the heartbeat cancels the publish and does not mark it published") {
    TestControl
      .executeEmbed(publication(sendFor = 100.seconds, renewals = n => Right(n == 0)))
      .map(observed => assertEquals(observed, Observed(renewals = 2, published = 0, retried = 0, cancelled = true)))
  }

  test("EPS-05.4 an unavailable lease renewal at the heartbeat cancels the publish and defers it for retry") {
    TestControl
      .executeEmbed(
        publication(
          sendFor = 100.seconds,
          renewals = n => if (n == 0) Right(true) else Left(RepositoryError.Unavailable)
        )
      )
      .map(observed => assertEquals(observed, Observed(renewals = 2, published = 0, retried = 1, cancelled = true)))
  }

  private final class UnusedProviders {
    private def unexpected[A]: InterviewProviderF[IO, A] =
      cats.data.EitherT.liftF(IO.raiseError[A](new AssertionError("Unexpected provider operation")))
    val calendar: InterviewCalendarProvider[IO] = new InterviewCalendarProvider[IO] {
      override def reserve(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String,
          candidateId: UserId,
          recruiterId: UserId,
          interval: InterviewInterval,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand]
      ) = unexpected[InterviewCalendarReservation]
      override def lookup(workflowId: InterviewWorkflowId, idempotencyKey: String) =
        unexpected[Option[InterviewCalendarReservation]]
      override def cancel(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand]
      ) = unexpected[InterviewCalendarCancellation]
      override def release(idempotencyKey: String, now: Instant, execution: Option[ClaimedInterviewWorkflowCommand]) =
        unexpected[Unit]
    }
    val notifications: InterviewNotificationProvider[IO] = new InterviewNotificationProvider[IO] {
      override def notify(
          workflowId: InterviewWorkflowId,
          recipientId: UserId,
          participant: InterviewParticipant,
          kind: InterviewNotificationKind,
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand]
      ) = unexpected[InterviewNotificationReceipt]
      override def lookup(idempotencyKey: String) = unexpected[Option[InterviewNotificationReceipt]]
    }
  }
}
