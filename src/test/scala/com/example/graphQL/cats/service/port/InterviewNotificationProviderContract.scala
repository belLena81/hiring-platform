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

/** Behavior every notification provider must satisfy before it may be activated: one effect per idempotency key, the
  * first receipt returned for every repeat or concurrent call, `lookup` finding an effect the caller never saw, and
  * independent keys.
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
