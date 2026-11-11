package com.example.graphQL.cats.infrastructure.ses

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.EmailAddress
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{
  InterviewInterval,
  InterviewNotificationKind,
  InterviewNotificationSendRefusal,
  InterviewParticipant,
  InterviewWorkflowId
}
import com.example.graphQL.cats.runtime.HiringLogValues
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.port.{
  InterviewNotificationAtLeastOnceContract,
  InterviewNotificationAtLeastOnceSubject,
  InterviewNotificationSendFence,
  ClaimedInterviewWorkflowCommand,
  InterviewNotificationTarget,
  InterviewProviderError
}
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
import munit.CatsEffectSuite

/** In-memory stand-in for the SES client: scripted outcomes, a call counter and the requests it received. */
final class StubSesEmailClient(
    script: List[SesSendOutcome],
    failure: Option[Throwable] = None,
    order: java.util.concurrent.ConcurrentLinkedQueue[String] = new java.util.concurrent.ConcurrentLinkedQueue[String]()
) extends SesEmailClient {
  private val outcomes = new java.util.concurrent.ConcurrentLinkedQueue[SesSendOutcome](script.asJava)
  private val calls = new java.util.concurrent.CopyOnWriteArrayList[SesEmailRequest]()

  def send(request: SesEmailRequest): SesSendOutcome = {
    order.add("client")
    calls.add(request)
    failure.foreach(throw _)
    Option(outcomes.poll()).getOrElse(SesSendOutcome.Accepted("stub-message-id"))
  }

  def received: IO[Vector[SesEmailRequest]] = IO(calls.asScala.toVector)
}

/** Scripted pre-send fence that records when it ran; `verdict` is what the durable checks would have answered. */
final class StubSendFence(
    verdict: IO[Either[InterviewNotificationSendRefusal, Unit]],
    order: java.util.concurrent.ConcurrentLinkedQueue[String]
) extends InterviewNotificationSendFence[IO] {
  private val seen = new java.util.concurrent.atomic.AtomicReference(Vector.empty[(InterviewNotificationKind, Boolean)])

  def verify(
      workflowId: InterviewWorkflowId,
      kind: InterviewNotificationKind,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): IO[Either[InterviewNotificationSendRefusal, Unit]] =
    IO(order.add("fence")) *> IO(seen.updateAndGet(_ :+ (kind -> execution.nonEmpty))) *> verdict

  def calls: Vector[(InterviewNotificationKind, Boolean)] = seen.get
}

class SesInterviewNotificationProviderSpec extends CatsEffectSuite with InterviewNotificationAtLeastOnceContract {
  private val secretAddress = "candidate.secret@example.test"
  private val workflowId = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-000000000a01"))
  private val recipientId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000b01"))
  private val now = Instant.parse("2026-11-10T10:00:00Z")
  private val interval = InterviewInterval(Instant.parse("2026-11-20T09:00:00Z"), Instant.parse("2026-11-20T09:30:00Z"))
  private val deliverable = SesRecipient.Deliverable(EmailAddress.from(secretAddress).toOption.get)

  private final class Harness(
      val provider: SesInterviewNotificationProvider,
      val client: StubSesEmailClient,
      val fence: StubSendFence,
      val order: java.util.concurrent.ConcurrentLinkedQueue[String],
      logs: Ref[IO, Vector[(LogEvent, Map[LogField, String])]]
  ) {
    def received: IO[Vector[SesEmailRequest]] = client.received
    def emitted: IO[Vector[(LogEvent, Map[LogField, String])]] = logs.get
    def deliver(key: String = "key-1", kind: InterviewNotificationKind = InterviewNotificationKind.Scheduled) =
      provider.notify(workflowId, recipientId, InterviewParticipant.Candidate, kind, key, now, None).value
  }

  private def harness(
      outcomes: List[SesSendOutcome] = Nil,
      recipient: IO[SesRecipient] = IO.pure(deliverable),
      context: IO[Option[InterviewNotificationContext]] = IO.pure(Some(InterviewNotificationContext(interval, None))),
      clientFailure: Option[Throwable] = None,
      verdict: IO[Either[InterviewNotificationSendRefusal, Unit]] = IO.pure(Right(()))
  ): IO[Harness] =
    for {
      logs <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          logs.update(_ :+ (event, fields))
      }
      order = new java.util.concurrent.ConcurrentLinkedQueue[String]()
      client = new StubSesEmailClient(outcomes, clientFailure, order)
      fence = new StubSendFence(verdict, order)
    } yield new Harness(
      new SesInterviewNotificationProvider(client, _ => recipient, _ => context, fence, diagnostics),
      client,
      fence,
      order,
      logs
    )

  test("DHW-26 an accepted message with an id gives a receipt for the notification and one client call") {
    for {
      h <- harness(List(SesSendOutcome.Accepted("message-1")))
      result <- h.deliver("key-accepted", InterviewNotificationKind.Rescheduled)
      sent <- h.received
    } yield {
      val receipt = result.fold(error => fail(s"unexpected $error"), identity)
      assertEquals(receipt.workflowId, workflowId)
      assertEquals(receipt.recipientId, recipientId)
      assertEquals(receipt.participant, InterviewParticipant.Candidate)
      assertEquals(receipt.kind, InterviewNotificationKind.Rescheduled)
      assertEquals(receipt.idempotencyKey, "key-accepted")
      assertEquals(receipt.deliveredAt, now)
      assertEquals(sent.size, 1)
    }
  }

  private val classifications: List[(SesSendOutcome, InterviewProviderError, String)] = List(
    (SesSendOutcome.AcceptedWithoutMessageId, InterviewProviderError.InvalidReceipt, "SES_INVALID_RECEIPT"),
    (SesSendOutcome.Accepted("  "), InterviewProviderError.InvalidReceipt, "SES_INVALID_RECEIPT"),
    (SesSendOutcome.NotSent, InterviewProviderError.Unavailable, "SES_NOT_SENT"),
    (SesSendOutcome.Throttled, InterviewProviderError.Unavailable, "SES_THROTTLED"),
    (SesSendOutcome.ServerError, InterviewProviderError.Unavailable, "SES_SERVER_ERROR"),
    (SesSendOutcome.TimedOut, InterviewProviderError.Unavailable, "SES_TIMED_OUT"),
    (SesSendOutcome.RecipientRejected, InterviewProviderError.Conflict, "SES_RECIPIENT_REJECTED"),
    (SesSendOutcome.AccessDenied, InterviewProviderError.Unavailable, "SES_ACCESS_DENIED"),
    (SesSendOutcome.SendingPaused, InterviewProviderError.Unavailable, "SES_SENDING_PAUSED")
  )

  classifications.foreach { case (outcome, expected, code) =>
    test(s"DHW-26 outcome $outcome maps to $expected with the safe code $code and exactly one client call") {
      for {
        h <- harness(List(outcome, SesSendOutcome.Accepted("never-used")))
        result <- h.deliver()
        sent <- h.received
        logs <- h.emitted
      } yield {
        assertEquals(result.swap.toOption, Some(expected))
        // One logical attempt: the adapter never retries, the interpreter owns the budget.
        assertEquals(sent.size, 1)
        assertEquals(logs.map(_._2.get(LogField.ErrorCode)), Vector(Some(code)))
      }
    }
  }

  test("DHW-26 access denied and a paused account are distinct codes, both Unavailable and neither a Conflict") {
    assertNotEquals(SesNotificationFailure.AccessDenied.code, SesNotificationFailure.SendingPaused.code)
    assertNotEquals(SesNotificationFailure.AccessDenied.code, SesNotificationFailure.ServerError.code)
  }

  List(
    ("a user with no email address", SesRecipient.NoAddress),
    ("a deleted or tombstoned subject", SesRecipient.Deleted)
  )
    .foreach { case (label, recipient) =>
      test(s"DHW-24 $label is a definitive no-send decided before any client call") {
        for {
          h <- harness(recipient = IO.pure(recipient))
          result <- h.deliver()
          sent <- h.received
        } yield {
          assertEquals(result.swap.toOption, Some(InterviewProviderError.Conflict))
          assertEquals(sent.size, 0)
        }
      }
    }

  test("DHW-24 a user-store failure is Unavailable and still sends nothing") {
    for {
      h <- harness(recipient = IO.raiseError(new IllegalStateException(s"store down for $secretAddress")))
      result <- h.deliver()
      sent <- h.received
      logs <- h.emitted
    } yield {
      assertEquals(result.swap.toOption, Some(InterviewProviderError.Unavailable))
      assertEquals(sent.size, 0)
      assert(!logs.toString.contains(secretAddress), clues(logs))
    }
  }

  test("DHW-24 forbidden input: an address with a line break or a second recipient is refused before any client call") {
    List(
      "a@example.test\r\nBcc: x@example.test",
      "a@example.test, b@example.test",
      "<a@example.test>",
      "a b@example.test"
    )
      .traverse_ { hostile =>
        val address = EmailAddress.from(hostile).toOption.get
        for {
          h <- harness(recipient = IO.pure(SesRecipient.Deliverable(address)))
          result <- h.deliver()
          sent <- h.received
        } yield {
          assertEquals(result.swap.toOption, Some(InterviewProviderError.Conflict), hostile)
          assertEquals(sent.size, 0, hostile)
        }
      }
  }

  test("DHW-24 forbidden input: a recipient with a Unicode separator, control or format character is never sent to") {
    List(
      "NEL" -> "\u0085",
      "line separator" -> "\u2028",
      "paragraph separator" -> "\u2029",
      "no-break space" -> "\u00A0",
      "bidi override" -> "\u202E",
      "zero-width space" -> "\u200B"
    ).flatMap { case (label, character) =>
      List(label -> s"a${character}b@example.test", label -> s"a@exa${character}mple.test")
    }.traverse_ { case (label, hostile) =>
      for {
        h <- harness(recipient = IO.pure(SesRecipient.Deliverable(EmailAddress.from(hostile).toOption.get)))
        result <- h.deliver()
        sent <- h.received
      } yield {
        assertEquals(result.swap.toOption, Some(InterviewProviderError.Conflict), label)
        assertEquals(sent.size, 0, label)
      }
    }
  }

  List(
    ("TimedOut", SesSendOutcome.TimedOut),
    ("InvalidReceipt (2xx without a message id)", SesSendOutcome.AcceptedWithoutMessageId)
  ).foreach { case (label, first) =>
    test(s"DDC-04.6 a retry with the same key after $label calls the client again") {
      for {
        h <- harness(List(first, SesSendOutcome.Accepted("second")))
        failed <- h.deliver("key-retry")
        retried <- h.deliver("key-retry")
        sent <- h.received
      } yield {
        assert(failed.isLeft, clues(failed))
        assertEquals(retried.map(_.idempotencyKey), Right("key-retry"))
        assertEquals(sent.size, 2)
      }
    }
  }

  test("DHW-24 a title is cut by code points, never inside a surrogate pair") {
    val emoji = "\uD83D\uDE00"
    val cut = SesInterviewMessage.sanitize("x" * 119 + emoji + "tail")
    assertEquals(cut, "x" * 119 + emoji)
    assertEquals(cut.codePointCount(0, cut.length), 120)
    assert(!Character.isHighSurrogate(cut.last), clues(cut))
  }

  test("DHW-24 a workflow with no readable context sends nothing and is a definitive refusal") {
    for {
      h <- harness(context = IO.pure(None))
      result <- h.deliver()
      sent <- h.received
    } yield {
      assertEquals(result.swap.toOption, Some(InterviewProviderError.Conflict))
      assertEquals(sent.size, 0)
    }
  }

  test("DHW-24 the address and the body appear in no receipt, error, log field or request rendering") {
    val title = "Senior Engineer UNIQUE-TITLE"
    val context = IO.pure(Some(InterviewNotificationContext(interval, Some(title))))
    for {
      ok <- harness(context = context)
      receipt <- ok.deliver("key-privacy")
      sentOk <- ok.received
      bad <- harness(List(SesSendOutcome.Throttled), context = context)
      failed <- bad.deliver("key-privacy-2")
      logs <- bad.emitted
    } yield {
      val rendered = List(receipt.toString, failed.toString, logs.toString, sentOk.toString)
      rendered.foreach { text =>
        assert(!text.contains(secretAddress), clues(text))
        assert(!text.contains("UNIQUE-TITLE"), clues(text))
        assert(!text.contains("Starts (UTC)"), clues(text))
      }
      // The body does reach the client, which is the one place allowed to hold it.
      assert(sentOk.head.textBody.contains("UNIQUE-TITLE"))
      logs.foreach { case (_, fields) =>
        fields.foreach { case (field, value) =>
          assert(LogFields.validPublic(field, value, HiringLogValues.allowed), clues(field, value))
        }
      }
    }
  }

  test("DHW-24 recruiter-controlled text loses CR, LF and other control characters and cannot add lines or headers") {
    val hostile = "Engineer\r\nBcc: evil@example.test\u0000\u0007 ‮Body-injection\n\nInjected"
    for {
      h <- harness(context = IO.pure(Some(InterviewNotificationContext(interval, Some(hostile)))))
      _ <- h.deliver()
      sent <- h.received
    } yield {
      val request = sent.head
      val positionLines = request.textBody.linesIterator.filter(_.startsWith("Position:")).toList
      assertEquals(positionLines.size, 1)
      assert(!positionLines.head.exists(c => Character.isISOControl(c) || c == ' ' || c == '‮'), clues(positionLines))
      assert(!request.subject.exists(Character.isISOControl), clues(request.subject))
      assert(!request.textBody.contains("\r"), clues(request.textBody))
      assertEquals(request.textBody.linesIterator.count(_.startsWith("Bcc:")), 0)
      assertEquals(request.recipient, EmailAddress.from(secretAddress).toOption.get)
    }
  }

  test("DHW-24 a title is bounded and a blank title is omitted") {
    assertEquals(SesInterviewMessage.sanitize("x" * 500).length, 120)
    assertEquals(SesInterviewMessage.sanitize("\r\n\t \u0000"), "")
    val rendered = SesInterviewMessage.render(
      InterviewNotificationKind.Cancelled,
      InterviewNotificationContext(interval, Some("\r\n")),
      EmailAddress.from(secretAddress).toOption.get
    )
    assert(!rendered.textBody.contains("Position:"))
    assert(rendered.textBody.contains("2026-11-20T09:00:00Z"))
  }

  test("DHW-26 a client that throws is Unavailable and logs only the class and location, never the message") {
    for {
      h <- harness(clientFailure = Some(new IllegalStateException(s"boom with $secretAddress and SECRET-TOKEN")))
      result <- h.deliver()
      sent <- h.received
      logs <- h.emitted
    } yield {
      assertEquals(result.swap.toOption, Some(InterviewProviderError.Unavailable))
      assertEquals(sent.size, 1)
      val fields = logs.flatMap(_._2).toMap
      assertEquals(fields.get(LogField.ErrorCode), Some("SES_CLIENT_FAILED"))
      assertEquals(fields.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
      assert(!logs.toString.contains("SECRET-TOKEN"), clues(logs))
      assert(!logs.toString.contains(secretAddress), clues(logs))
    }
  }

  // DHW-26 / Security finding F1: the pre-send fence. Each refusal is a definitive no-send (Conflict) except a check that
  // could not run (Unavailable); in every case the client is never called and no recipient or content is logged.
  private val refusals: List[(InterviewNotificationSendRefusal, InterviewProviderError, String, String)] = List(
    (
      InterviewNotificationSendRefusal.ClaimNotHeld,
      InterviewProviderError.Conflict,
      "SES_FENCE_CLAIM_NOT_HELD",
      "a lost or replaced claim"
    ),
    (
      InterviewNotificationSendRefusal.ParticipantInactive,
      InterviewProviderError.Conflict,
      "SES_FENCE_PARTICIPANT_INACTIVE",
      "a deleted other participant"
    ),
    (
      InterviewNotificationSendRefusal.KindNotAllowedInPhase,
      InterviewProviderError.Conflict,
      "SES_FENCE_KIND_NOT_ALLOWED",
      "a stale kind after a cancellation"
    ),
    (
      InterviewNotificationSendRefusal.HiringNotCommitted,
      InterviewProviderError.Conflict,
      "SES_FENCE_HIRING_NOT_COMMITTED",
      "Scheduled before the hiring commit"
    ),
    (
      InterviewNotificationSendRefusal.WorkflowNotFound,
      InterviewProviderError.Conflict,
      "SES_FENCE_WORKFLOW_NOT_FOUND",
      "a missing workflow"
    ),
    (
      InterviewNotificationSendRefusal.CheckUnavailable,
      InterviewProviderError.Unavailable,
      "SES_FENCE_UNAVAILABLE",
      "a check that could not run"
    )
  )

  refusals.foreach { case (refusal, expected, code, label) =>
    test(s"DHW-26 F1 $label ($refusal) sends nothing: $expected with the safe code $code and zero client calls") {
      for {
        h <- harness(verdict = IO.pure(Left(refusal)))
        result <- h.deliver()
        sent <- h.received
        logs <- h.emitted
      } yield {
        assertEquals(result.swap.toOption, Some(expected))
        assertEquals(sent.size, 0)
        assertEquals(logs.map(_._2.get(LogField.ErrorCode)), Vector(Some(code)))
        assert(!logs.toString.contains(secretAddress), clues(logs))
      }
    }
  }

  test("DHW-26 F1 when every check passes exactly one client call is made, after the fence ran for that kind") {
    for {
      h <- harness(List(SesSendOutcome.Accepted("message-1")))
      result <- h.deliver("key-fenced", InterviewNotificationKind.Cancelled)
      sent <- h.received
    } yield {
      assert(result.isRight, clues(result))
      assertEquals(sent.size, 1)
      assertEquals(h.fence.calls.map(_._1), Vector(InterviewNotificationKind.Cancelled))
    }
  }

  test("DHW-26 F1 the fence runs before the client call, never after") {
    for {
      h <- harness()
      _ <- h.deliver()
    } yield assertEquals(h.order.toArray.toList, List("fence", "client"))
  }

  test("DHW-26 F1 a fence that throws is Unavailable, sends nothing and logs no message or recipient") {
    for {
      h <- harness(verdict =
        IO.raiseError(new IllegalStateException(s"fence store down for $secretAddress SECRET-TOKEN"))
      )
      result <- h.deliver()
      sent <- h.received
      logs <- h.emitted
    } yield {
      assertEquals(result.swap.toOption, Some(InterviewProviderError.Unavailable))
      assertEquals(sent.size, 0)
      assertEquals(logs.flatMap(_._2).toMap.get(LogField.ErrorCode), Some("SES_FENCE_UNAVAILABLE"))
      assert(!logs.toString.contains("SECRET-TOKEN"), clues(logs))
      assert(!logs.toString.contains(secretAddress), clues(logs))
    }
  }

  test("DHW-26 F1 a retry after a refusal re-checks: once the check passes the message is sent, never earlier") {
    for {
      verdicts <- Ref.of[IO, List[Either[InterviewNotificationSendRefusal, Unit]]](
        List(Left(InterviewNotificationSendRefusal.ClaimNotHeld), Right(()))
      )
      h <- harness(verdict = verdicts.modify {
        case head :: tail => (tail, head)
        case Nil          => (Nil, Right(()))
      })
      first <- h.deliver("key-retry-fence")
      afterFirst <- h.received
      second <- h.deliver("key-retry-fence")
      afterSecond <- h.received
    } yield {
      assert(first.isLeft, clues(first))
      assertEquals(afterFirst.size, 0)
      assert(second.isRight, clues(second))
      assertEquals(afterSecond.size, 1)
    }
  }

  test("DHW-26 lookup never claims a delivery: SES has no remote lookup and keeps no receipt") {
    for {
      h <- harness()
      _ <- h.deliver("key-looked-up")
      found <- h.provider.lookup("key-looked-up").value
    } yield assertEquals(found, Right(None))
  }

  // DDC-04.6 at-least-once contract variant, run against a counting in-memory client.
  protected def withAtLeastOnceSubject[A](use: InterviewNotificationAtLeastOnceSubject => IO[A]): IO[A] = {
    val lose = new java.util.concurrent.atomic.AtomicBoolean(false)
    val delivered = new java.util.concurrent.atomic.AtomicInteger(0)
    val counting = new SesEmailClient {
      def send(request: SesEmailRequest): SesSendOutcome = {
        delivered.incrementAndGet()
        if (lose.getAndSet(false)) SesSendOutcome.TimedOut else SesSendOutcome.Accepted("id")
      }
    }
    use(new InterviewNotificationAtLeastOnceSubject {
      val provider = new SesInterviewNotificationProvider(
        counting,
        _ => IO.pure(deliverable),
        _ => IO.pure(Some(InterviewNotificationContext(interval, None))),
        new StubSendFence(IO.pure(Right(())), new java.util.concurrent.ConcurrentLinkedQueue[String]()),
        Diagnostics.noop[IO]
      )
      def newTarget: IO[InterviewNotificationTarget] = IO.pure(InterviewNotificationTarget(workflowId, recipientId))
      def loseNextOutcome: IO[Unit] = IO(lose.set(true))
      def deliveries: IO[Int] = IO(delivered.get)
    })
  }
}
