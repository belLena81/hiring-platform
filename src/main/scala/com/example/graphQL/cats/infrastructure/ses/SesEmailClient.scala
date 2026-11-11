package com.example.graphQL.cats.infrastructure.ses

import com.example.graphQL.cats.domain.model.EmailAddress

/** One plain-text message to one recipient. The sender, region and configuration set belong to the client, so the
  * request carries nothing that could redirect or re-address a send. `toString` never shows the address or the body.
  */
final case class SesEmailRequest(recipient: EmailAddress, subject: String, textBody: String) {
  override def toString: String = "SesEmailRequest([REDACTED])"
}

/** The one classified result of one send call. Mapping a vendor response or exception onto this closed set happens
  * once, inside the client; the provider never inspects vendor types or messages.
  */
enum SesSendOutcome {

  /** The service accepted the message and returned its message id. */
  case Accepted(messageId: String)

  /** A 2xx answer without a message id: acceptance cannot be confirmed. */
  case AcceptedWithoutMessageId

  /** The request never left this process (connection, DNS, client-side failure). */
  case NotSent

  /** Throttling or 429. */
  case Throttled

  /** A 5xx answer. */
  case ServerError

  /** The call timed out; whether the service acted is unknown. */
  case TimedOut

  /** The service refused the message or the recipient (invalid, suppressed, rejected content). */
  case RecipientRejected

  /** Missing or invalid authorization (the runtime identity is not accepted), or a 403. */
  case AccessDenied

  /** The sending account or identity is paused or suspended. */
  case SendingPaused
}

/** A thin, synchronous, one-attempt client for sending one email. It may block; the caller wraps it in `IO.blocking`.
  * The implementation retries nothing (the interpreter owns the retry budget), applies its own timeouts below the
  * provider timeout, and is built only when SES is selected. No vendor type crosses this interface.
  */
trait SesEmailClient {
  def send(request: SesEmailRequest): SesSendOutcome
}
