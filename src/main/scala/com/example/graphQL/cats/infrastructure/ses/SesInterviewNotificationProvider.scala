package com.example.graphQL.cats.infrastructure.ses

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.config.SesMailboxAddress
import com.example.graphQL.cats.domain.model.EmailAddress
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{
  InterviewInterval,
  InterviewNotificationKind,
  InterviewNotificationSendRefusal,
  InterviewParticipant,
  InterviewWorkflowId
}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.port.{
  ClaimedInterviewWorkflowCommand,
  InterviewNotificationProvider,
  InterviewNotificationReceipt,
  InterviewNotificationSendFence,
  InterviewProviderError,
  InterviewProviderF
}
import java.time.Instant

/** What the user store may tell the adapter about a recipient: a deliverable address, or a definitive reason there is
  * none. The lookup that produces it projects only the email and the deleted/tombstone state.
  */
enum SesRecipient {
  case Deliverable(address: EmailAddress)
  case NoAddress
  case Deleted
}

/** The only workflow facts a message may use: the UTC interval and, at most, the job title. */
final case class InterviewNotificationContext(interval: InterviewInterval, jobTitle: Option[String])

/** Safe, closed classification codes. They are the only failure detail that is logged. */
enum SesNotificationFailure(val code: String, val error: InterviewProviderError) {
  case NoRecipient extends SesNotificationFailure("SES_NO_RECIPIENT", InterviewProviderError.Conflict)
  case InvalidRecipient extends SesNotificationFailure("SES_INVALID_RECIPIENT", InterviewProviderError.Conflict)
  case NoContext extends SesNotificationFailure("SES_NO_CONTEXT", InterviewProviderError.Conflict)
  case RecipientRejected extends SesNotificationFailure("SES_RECIPIENT_REJECTED", InterviewProviderError.Conflict)
  case InvalidReceipt extends SesNotificationFailure("SES_INVALID_RECEIPT", InterviewProviderError.InvalidReceipt)
  case NotSent extends SesNotificationFailure("SES_NOT_SENT", InterviewProviderError.Unavailable)
  case Throttled extends SesNotificationFailure("SES_THROTTLED", InterviewProviderError.Unavailable)
  case ServerError extends SesNotificationFailure("SES_SERVER_ERROR", InterviewProviderError.Unavailable)
  case TimedOut extends SesNotificationFailure("SES_TIMED_OUT", InterviewProviderError.Unavailable)
  case AccessDenied extends SesNotificationFailure("SES_ACCESS_DENIED", InterviewProviderError.Unavailable)
  case SendingPaused extends SesNotificationFailure("SES_SENDING_PAUSED", InterviewProviderError.Unavailable)
  case ClientFailed extends SesNotificationFailure("SES_CLIENT_FAILED", InterviewProviderError.Unavailable)
  case LookupFailed extends SesNotificationFailure("SES_LOOKUP_FAILED", InterviewProviderError.Unavailable)
  case ClaimNotHeld extends SesNotificationFailure("SES_FENCE_CLAIM_NOT_HELD", InterviewProviderError.Conflict)
  case ParticipantInactive
      extends SesNotificationFailure("SES_FENCE_PARTICIPANT_INACTIVE", InterviewProviderError.Conflict)
  case KindNotAllowed extends SesNotificationFailure("SES_FENCE_KIND_NOT_ALLOWED", InterviewProviderError.Conflict)
  case HiringNotCommitted
      extends SesNotificationFailure("SES_FENCE_HIRING_NOT_COMMITTED", InterviewProviderError.Conflict)
  case WorkflowNotFound extends SesNotificationFailure("SES_FENCE_WORKFLOW_NOT_FOUND", InterviewProviderError.Conflict)
  case FenceUnavailable extends SesNotificationFailure("SES_FENCE_UNAVAILABLE", InterviewProviderError.Unavailable)
}

/** Interview notifications sent as email through SES.
  *
  * Delivery guarantee: AT LEAST ONCE (owner decision 2026-11-10). SES has no idempotency key and no lookup, and this
  * adapter keeps no receipt, so `lookup` always answers "no known delivery". An unknown outcome (timeout, lost
  * response, crash after the service accepted) therefore leads the interpreter to send again: a duplicate message is
  * possible, a silent loss is not. The ledger provider keeps its exactly-once receipts.
  *
  * Notification send fence: immediately before the one client call, `fence` checks that the caller still holds its
  * claim, the candidate, the recruiter and the initiating actor are active, the kind is allowed in the workflow phase
  * and hiring is committed before `Scheduled` (the ledger's four checks, without recording a delivery). Any failed
  * check, an absent claim or a failing check is a typed refusal with a safe code and zero client calls, so a replay, a
  * lost claim or a cancellation never sends a stale notice. The fence is a required constructor parameter; there is no
  * way to build the provider without it. Remaining gaps, which no external send can close: (1) a cancellation, deletion
  * or lost claim committed between the check and the client call can still produce one message; (2) the send can
  * succeed and the process die before the receipt or acknowledgement is recorded, so the interpreter sends again after
  * recovery: a duplicate is possible, a silent loss is not.
  *
  * Privacy: the recipient address is resolved from the user store at send time through `recipient`, used for the one
  * client call and never stored, returned, logged, tagged or placed in a header. Neither the body nor the message id is
  * kept. A user without an address, or a deleted or tombstoned subject, is a definitive no-send decided before any
  * client call.
  *
  * One logical attempt per call: no retry here. The blocking client call runs under `IO.blocking`. The outcome is
  * classified once, by [[SesNotificationFailure]], onto [[InterviewProviderError]].
  */
final class SesInterviewNotificationProvider(
    client: SesEmailClient,
    recipient: UserId => IO[SesRecipient],
    context: InterviewWorkflowId => IO[Option[InterviewNotificationContext]],
    fence: InterviewNotificationSendFence[IO],
    diagnostics: Diagnostics[IO]
) extends InterviewNotificationProvider[IO] {

  def notify(
      workflowId: InterviewWorkflowId,
      recipientId: UserId,
      participant: InterviewParticipant,
      kind: InterviewNotificationKind,
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[IO, InterviewNotificationReceipt] =
    for {
      address <- resolveAddress(recipientId)
      facts <- resolveContext(workflowId)
      request = SesInterviewMessage.render(kind, facts, address)
      _ <- verifyBeforeSend(workflowId, kind, execution)
      outcome <- send(request)
      _ <- classify(outcome)
    } yield InterviewNotificationReceipt(workflowId, recipientId, participant, kind, idempotencyKey, now)

  /** No remote lookup and no local receipt exist, so an unknown outcome is resolved by sending again. */
  def lookup(idempotencyKey: String): InterviewProviderF[IO, Option[InterviewNotificationReceipt]] =
    EitherT.rightT(None)

  /** The one place a failure is recorded: a safe code, and for an unexpected throwable only its class and location. */
  private def fail[A](failure: SesNotificationFailure, cause: Option[Throwable] = None): InterviewProviderF[IO, A] =
    EitherT.left(
      diagnostics
        .emit(
          LogEvent.InterviewNotificationFailed,
          fields = Map(LogField.ErrorCode -> failure.code) ++ cause.fold(Map.empty[LogField, String])(LogFields.failure)
        )
        .as(failure.error)
    )

  private def guarded[A](effect: IO[A], failure: SesNotificationFailure): InterviewProviderF[IO, A] =
    EitherT(effect.attempt).biflatMap(cause => fail[A](failure, Some(cause)), value => EitherT.rightT(value))

  private def resolveAddress(recipientId: UserId): InterviewProviderF[IO, EmailAddress] =
    guarded(recipient(recipientId), SesNotificationFailure.LookupFailed).flatMap {
      case SesRecipient.Deliverable(address) =>
        if (SesMailboxAddress.matches(address.value)) EitherT.rightT(address)
        else fail(SesNotificationFailure.InvalidRecipient)
      case SesRecipient.NoAddress | SesRecipient.Deleted => fail(SesNotificationFailure.NoRecipient)
    }

  private def resolveContext(workflowId: InterviewWorkflowId): InterviewProviderF[IO, InterviewNotificationContext] =
    guarded(context(workflowId), SesNotificationFailure.LookupFailed).flatMap {
      case Some(facts) => EitherT.rightT(facts)
      case None        => fail(SesNotificationFailure.NoContext)
    }

  /** The last step before the client call; a refusal or a failure of the check itself sends nothing. */
  private def verifyBeforeSend(
      workflowId: InterviewWorkflowId,
      kind: InterviewNotificationKind,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): InterviewProviderF[IO, Unit] =
    guarded(fence.verify(workflowId, kind, execution), SesNotificationFailure.FenceUnavailable).flatMap {
      case Right(())     => EitherT.rightT(())
      case Left(refusal) => fail(refused(refusal))
    }

  private def refused(refusal: InterviewNotificationSendRefusal): SesNotificationFailure = {
    import InterviewNotificationSendRefusal as R
    refusal match {
      case R.ClaimNotHeld          => SesNotificationFailure.ClaimNotHeld
      case R.ParticipantInactive   => SesNotificationFailure.ParticipantInactive
      case R.KindNotAllowedInPhase => SesNotificationFailure.KindNotAllowed
      case R.HiringNotCommitted    => SesNotificationFailure.HiringNotCommitted
      case R.WorkflowNotFound      => SesNotificationFailure.WorkflowNotFound
      case R.CheckUnavailable      => SesNotificationFailure.FenceUnavailable
    }
  }

  private def send(request: SesEmailRequest): InterviewProviderF[IO, SesSendOutcome] =
    // IO.blocking cannot be cancelled: after the provider timeout the send may still complete, so a message can be
    // delivered although the caller saw a failure. That fits the at-least-once guarantee (a duplicate, never a loss).
    guarded(IO.blocking(client.send(request)), SesNotificationFailure.ClientFailed)

  private def classify(outcome: SesSendOutcome): InterviewProviderF[IO, Unit] = {
    import SesNotificationFailure as F
    outcome match {
      case SesSendOutcome.Accepted(messageId) =>
        if (messageId.isBlank) fail(F.InvalidReceipt) else EitherT.rightT(())
      case SesSendOutcome.AcceptedWithoutMessageId => fail(F.InvalidReceipt)
      case SesSendOutcome.NotSent                  => fail(F.NotSent)
      case SesSendOutcome.Throttled                => fail(F.Throttled)
      case SesSendOutcome.ServerError              => fail(F.ServerError)
      case SesSendOutcome.TimedOut                 => fail(F.TimedOut)
      case SesSendOutcome.RecipientRejected        => fail(F.RecipientRejected)
      case SesSendOutcome.AccessDenied             => fail(F.AccessDenied)
      case SesSendOutcome.SendingPaused            => fail(F.SendingPaused)
    }
  }
}

/** The minimal plain-text template: the kind, the UTC interval and, at most, the job title. Recruiter-controlled text
  * has control characters (including CR and LF) removed and is truncated, so it cannot inject lines or headers. The
  * subject is fixed text per kind.
  */
object SesInterviewMessage {
  private val MaxTitleChars = 120

  def render(
      kind: InterviewNotificationKind,
      facts: InterviewNotificationContext,
      address: EmailAddress
  ): SesEmailRequest = {
    val headline = describe(kind)
    val title = facts.jobTitle.map(sanitize).filter(_.nonEmpty).map(value => s"Position: $value\n").getOrElse("")
    val body =
      s"$headline\n\n${title}Starts (UTC): ${facts.interval.startsAt}\nEnds (UTC): ${facts.interval.endsAt}\n"
    SesEmailRequest(address, s"Interview update: $headline", body)
  }

  /** Replaces control, separator and format characters, collapses whitespace and bounds the length by code points, so a
    * surrogate pair is never split.
    */
  def sanitize(text: String): String = {
    val cleaned = text.codePoints.toArray
      .map { point =>
        val characterType = Character.getType(point)
        val unsafe =
          Character.isISOControl(point) || characterType == Character.LINE_SEPARATOR.toInt ||
            characterType == Character.PARAGRAPH_SEPARATOR.toInt || characterType == Character.FORMAT.toInt
        if (unsafe) ' '.toInt else point
      }
      .map(Character.toString)
      .mkString
      .replaceAll("\\s+", " ")
      .trim
    cleaned.codePoints.toArray.take(MaxTitleChars).map(Character.toString).mkString.trim
  }

  private def describe(kind: InterviewNotificationKind): String = kind match {
    case InterviewNotificationKind.Scheduled             => "Your interview is scheduled"
    case InterviewNotificationKind.Cancelled             => "Your interview was cancelled"
    case InterviewNotificationKind.Rescheduled           => "Your interview was rescheduled"
    case InterviewNotificationKind.RescheduleProposed    => "A new interview time was proposed"
    case InterviewNotificationKind.RescheduleDeclined    => "A proposed interview time was declined"
    case InterviewNotificationKind.RescheduleWithdrawn   => "A proposed interview time was withdrawn"
    case InterviewNotificationKind.RescheduleExpired     => "A proposed interview time expired"
    case InterviewNotificationKind.RescheduleRequested   => "A new interview time was requested"
    case InterviewNotificationKind.RescheduleUnavailable => "A requested interview time is not available"
  }
}
