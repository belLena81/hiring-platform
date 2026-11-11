package com.example.graphQL.cats.service.port

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{InterviewNotificationKind, InterviewParticipant, InterviewWorkflowId}
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import munit.CatsEffectSuite
import scala.collection.concurrent.TrieMap

/** Proves the at-least-once contract has teeth: a provider that remembers the key in memory and answers the retry
  * without sending (a silent loss when the first outcome was lost) must fail it, while a provider that sends again must
  * pass it.
  */
class InterviewNotificationAtLeastOnceContractSelfCheckSpec extends CatsEffectSuite {
  private val workflowId = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-000000000a02"))
  private val recipientId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000b02"))

  private def subject(deduplicateByKey: Boolean): InterviewNotificationAtLeastOnceSubject = {
    val lose = new AtomicBoolean(false)
    val delivered = new AtomicInteger(0)
    val remembered = TrieMap.empty[String, Unit]
    new InterviewNotificationAtLeastOnceSubject {
      val provider: InterviewNotificationProvider[IO] = new InterviewNotificationProvider[IO] {
        def notify(
            workflowId: InterviewWorkflowId,
            recipientId: UserId,
            participant: InterviewParticipant,
            kind: InterviewNotificationKind,
            idempotencyKey: String,
            now: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ): InterviewProviderF[IO, InterviewNotificationReceipt] = {
          val receipt = InterviewNotificationReceipt(workflowId, recipientId, participant, kind, idempotencyKey, now)
          if (deduplicateByKey && remembered.contains(idempotencyKey)) EitherT.rightT(receipt)
          else {
            remembered.put(idempotencyKey, ())
            delivered.incrementAndGet()
            if (lose.getAndSet(false)) EitherT.leftT(InterviewProviderError.Unavailable) else EitherT.rightT(receipt)
          }
        }
        def lookup(idempotencyKey: String): InterviewProviderF[IO, Option[InterviewNotificationReceipt]] =
          EitherT.rightT(None)
      }
      def newTarget: IO[InterviewNotificationTarget] = IO.pure(InterviewNotificationTarget(workflowId, recipientId))
      def loseNextOutcome: IO[Unit] = IO(lose.set(true))
      def deliveries: IO[Int] = IO(delivered.get)
    }
  }

  test("DDC-04.6 self-check: a provider that sends again after a lost outcome satisfies the at-least-once contract") {
    InterviewNotificationAtLeastOnceChecks.retryAfterLostOutcomeSendsAgain(subject(deduplicateByKey = false))
  }

  test("DDC-04.6 self-check: a key-deduplicating provider silently loses the message and fails the contract") {
    InterviewNotificationAtLeastOnceChecks
      .retryAfterLostOutcomeSendsAgain(subject(deduplicateByKey = true))
      .attempt
      .map {
        case Left(_: AssertionError) => ()
        case Left(other)             => fail(s"Expected an assertion failure, got $other")
        case Right(_)                => fail("A key-deduplicating provider must not satisfy the at-least-once contract")
      }
  }
}
