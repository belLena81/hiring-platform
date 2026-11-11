package com.example.graphQL.cats.service.port

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{InterviewNotificationKind, InterviewParticipant, InterviewWorkflowId}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import munit.CatsEffectSuite

/** Who a contract notification is addressed to. An adapter whose provider has no workflow precondition returns any
  * identifiers; the platform's ledger needs a stored workflow.
  */
final case class InterviewNotificationTarget(workflowId: InterviewWorkflowId, recipientId: UserId)

/** How a notification implementation is arranged for the shared contract. */
trait InterviewNotificationContractSubject {
  def provider: InterviewNotificationProvider[IO]

  /** A fresh target the provider may notify now. */
  def newTarget: IO[InterviewNotificationTarget]
}

/** Behavior every notification provider with exactly-once receipts (the platform ledger) must satisfy before it may be
  * activated: one effect per idempotency key, the first receipt returned for every repeat or concurrent call, `lookup`
  * finding an effect the caller never saw, and independent keys. A provider without receipts or a remote lookup (email
  * through SES) satisfies [[InterviewNotificationAtLeastOnceContract]] instead.
  */
trait InterviewNotificationProviderContract { self: CatsEffectSuite =>

  /** Each test receives an isolated subject. */
  protected def withSubject[A](use: InterviewNotificationContractSubject => IO[A]): IO[A]

  private val kind = InterviewNotificationKind.RescheduleProposed
  private val concurrentCalls = 8

  private def success[A](value: InterviewProviderF[IO, A]): IO[A] =
    value.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Provider failure: $error")), IO.pure))

  private def key(): String = s"contract-notify-${UUID.randomUUID()}"

  private def send(
      subject: InterviewNotificationContractSubject,
      target: InterviewNotificationTarget,
      idempotencyKey: String,
      at: Instant
  ) =
    subject.provider.notify(
      target.workflowId,
      target.recipientId,
      InterviewParticipant.Candidate,
      kind,
      idempotencyKey,
      at,
      None
    )

  private def clock: IO[Instant] = IO.realTimeInstant.map(_.truncatedTo(ChronoUnit.MILLIS))

  test("DDC-04.1 the same notification key twice gives one delivery and the same receipt") {
    withSubject { subject =>
      val idempotencyKey = key()
      for {
        target <- subject.newTarget
        at <- clock
        first <- success(send(subject, target, idempotencyKey, at))
        again <- success(send(subject, target, idempotencyKey, at.plusSeconds(60)))
        found <- success(subject.provider.lookup(idempotencyKey))
      } yield {
        assertEquals(first.idempotencyKey, idempotencyKey)
        assertEquals(first.deliveredAt, at)
        assertEquals(again, first)
        assertEquals(found, Some(first))
      }
    }
  }

  test("DDC-04.2 concurrent notification calls with one key give one delivery") {
    withSubject { subject =>
      val idempotencyKey = key()
      for {
        target <- subject.newTarget
        at <- clock
        receipts <- (0 until concurrentCalls).toList.parTraverse(attempt =>
          success(send(subject, target, idempotencyKey, at.plusSeconds(attempt.toLong)))
        )
        found <- success(subject.provider.lookup(idempotencyKey))
      } yield {
        // Each caller sent a different delivery time: one distinct receipt means one effect won and all others saw it.
        assertEquals(receipts.distinct.size, 1)
        assertEquals(found, receipts.headOption)
      }
    }
  }

  test("DDC-04.3 lookup returns the delivery after an outcome the caller did not see, and nothing before it") {
    withSubject { subject =>
      val idempotencyKey = key()
      for {
        target <- subject.newTarget
        at <- clock
        before <- success(subject.provider.lookup(idempotencyKey))
        // The response is lost: the caller only knows the call was made.
        _ <- send(subject, target, idempotencyKey, at).value.void
        found <- success(subject.provider.lookup(idempotencyKey))
        retried <- success(send(subject, target, idempotencyKey, at.plusSeconds(30)))
      } yield {
        assertEquals(before, None)
        assertEquals(found.map(_.idempotencyKey), Some(idempotencyKey))
        assertEquals(found.map(_.deliveredAt), Some(at))
        assertEquals(Some(retried), found)
      }
    }
  }

  test("DDC-04.4 different notification keys stay independent") {
    withSubject { subject =>
      val (firstKey, secondKey) = (key(), key())
      for {
        target <- subject.newTarget
        at <- clock
        first <- success(send(subject, target, firstKey, at))
        second <- success(send(subject, target, secondKey, at.plusSeconds(5)))
        foundFirst <- success(subject.provider.lookup(firstKey))
        foundSecond <- success(subject.provider.lookup(secondKey))
      } yield {
        assertNotEquals(first, second)
        assertEquals(second.deliveredAt, at.plusSeconds(5))
        assertEquals(foundFirst, Some(first))
        assertEquals(foundSecond, Some(second))
      }
    }
  }
}

/** A provider whose delivery can be made to look lost to the caller. */
trait InterviewNotificationAtLeastOnceSubject extends InterviewNotificationContractSubject {

  /** Arranges that the next call reaches the provider but the caller sees an unknown outcome; later calls succeed. */
  def loseNextOutcome: IO[Unit]

  /** How many messages the provider actually sent so far. */
  def deliveries: IO[Int]
}

/** The assertions that make the at-least-once contract able to detect a silent loss. They are separate from the suite
  * so a self-check can prove that a deliberately key-deduplicating provider fails them.
  */
object InterviewNotificationAtLeastOnceChecks extends munit.Assertions {

  /** A call whose outcome the caller never saw leaves no receipt, and the retry with the same key must reach the
    * provider again: the delivery count rises by exactly one after the failed first call. A provider that remembers the
    * key and answers the retry without sending would lose the message silently.
    */
  def retryAfterLostOutcomeSendsAgain(subject: InterviewNotificationAtLeastOnceSubject): IO[Unit] = {
    val idempotencyKey = s"contract-notify-${UUID.randomUUID()}"
    def send(target: InterviewNotificationTarget) =
      subject.provider.notify(
        target.workflowId,
        target.recipientId,
        InterviewParticipant.Candidate,
        InterviewNotificationKind.RescheduleProposed,
        idempotencyKey,
        Instant.parse("2026-11-10T10:00:00Z"),
        None
      )
    for {
      target <- subject.newTarget
      _ <- subject.loseNextOutcome
      first <- send(target).value
      afterFirst <- subject.deliveries
      known <- subject.provider.lookup(idempotencyKey).value
      retried <- send(target).value
      afterRetry <- subject.deliveries
    } yield {
      assert(first.isLeft, clues(first))
      assertEquals(known, Right(None))
      assertEquals(retried.map(_.idempotencyKey), Right(idempotencyKey))
      assertEquals(afterRetry, afterFirst + 1, "the retry must call the provider again")
    }
  }
}

/** The at-least-once contract (owner decision 2026-11-10) for a provider with no remote lookup and no local receipt: an
  * unknown outcome followed by an absent receipt may be sent again; a duplicate is allowed, a silent loss is not. The
  * exactly-once tests of [[InterviewNotificationProviderContract]] deliberately do not apply to such a provider.
  */
trait InterviewNotificationAtLeastOnceContract { self: CatsEffectSuite =>

  /** Each test receives an isolated subject. */
  protected def withAtLeastOnceSubject[A](use: InterviewNotificationAtLeastOnceSubject => IO[A]): IO[A]

  private def key(): String = s"contract-notify-${UUID.randomUUID()}"

  private def send(subject: InterviewNotificationAtLeastOnceSubject, target: InterviewNotificationTarget, k: String) =
    subject.provider.notify(
      target.workflowId,
      target.recipientId,
      InterviewParticipant.Candidate,
      InterviewNotificationKind.RescheduleProposed,
      k,
      Instant.parse("2026-11-10T10:00:00Z"),
      None
    )

  test("DDC-04.6 at-least-once: an unknown outcome with no receipt is resolved by sending again, so nothing is lost") {
    withAtLeastOnceSubject(InterviewNotificationAtLeastOnceChecks.retryAfterLostOutcomeSendsAgain)
  }

  test("DDC-04.6 at-least-once: a repeated key may deliver a duplicate but each call still answers a receipt") {
    withAtLeastOnceSubject { subject =>
      val idempotencyKey = key()
      for {
        target <- subject.newTarget
        first <- send(subject, target, idempotencyKey).value
        afterFirst <- subject.deliveries
        again <- send(subject, target, idempotencyKey).value
      } yield {
        assertEquals(first.map(_.idempotencyKey), Right(idempotencyKey))
        assertEquals(again.map(_.idempotencyKey), Right(idempotencyKey))
        // The first call really sent a message; the repeat is free to send a duplicate.
        assertEquals(afterFirst, 1)
      }
    }
  }

  test("DDC-04.4 at-least-once: different notification keys stay independent") {
    withAtLeastOnceSubject { subject =>
      val (firstKey, secondKey) = (key(), key())
      for {
        target <- subject.newTarget
        first <- send(subject, target, firstKey).value
        second <- send(subject, target, secondKey).value
      } yield {
        assertEquals(first.map(_.idempotencyKey), Right(firstKey))
        assertEquals(second.map(_.idempotencyKey), Right(secondKey))
      }
    }
  }
}
