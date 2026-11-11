package com.example.graphQL.cats.runtime

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.config.{
  InterviewNotificationConfig,
  InterviewNotificationSelection,
  SesNotificationSettings
}
import com.example.graphQL.cats.infrastructure.ses.{
  InterviewNotificationContext,
  SesEmailClient,
  SesInterviewNotificationProvider,
  SesRecipient
}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{
  InterviewNotificationKind,
  InterviewNotificationSendRefusal,
  InterviewWorkflowId
}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{
  ClaimedInterviewWorkflowCommand,
  InterviewNotificationProvider,
  InterviewNotificationReceiptLedger,
  InterviewNotificationSendFence,
  LedgerInterviewNotificationProvider
}

/** Raised at startup when SES is selected but this build has no SES client. It is a typed startup error: the runtime
  * never falls back to the ledger provider, because that would silently stop real email while the configuration says
  * email is on.
  */
final class SesClientNotAvailableError extends IllegalStateException("SES client not available in this build")

/** The narrow user-store and workflow projections the SES provider reads at send time, and the required pre-send fence
  * (the notification send fence). The fence has no default: a wiring cannot be built without one.
  */
final case class SesNotificationLookups(
    recipient: UserId => IO[SesRecipient],
    context: InterviewWorkflowId => IO[Option[InterviewNotificationContext]],
    fence: InterviewNotificationSendFence[IO]
)

object SesNotificationLookups {

  /** Placeholder until the SES client exists: the real projections (email and deleted/tombstone state only, and the
    * workflow interval with at most the job title) are wired together with the client. Selecting SES stops at
    * [[SesClients.notAvailableInThisBuild]] before these could run.
    */
  val awaitingWiring: SesNotificationLookups = SesNotificationLookups(
    _ => IO.raiseError(new SesClientNotAvailableError),
    _ => IO.raiseError(new SesClientNotAvailableError),
    SendFences.refuseEverything
  )
}

object SendFences {

  /** Fail-closed placeholder: every check reports that it could not run, so nothing can be sent through it. */
  val refuseEverything: InterviewNotificationSendFence[IO] = new InterviewNotificationSendFence[IO] {
    def verify(
        workflowId: InterviewWorkflowId,
        kind: InterviewNotificationKind,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ): IO[Either[InterviewNotificationSendRefusal, Unit]] =
      IO.pure(Left(InterviewNotificationSendRefusal.CheckUnavailable))
  }
}

/** How the SES client is built. */
object SesClients {

  /** NOT AVAILABLE IN THIS BUILD (explicit seam). The AWS SDK dependency is not part of this build yet (it waits for
    * the dependency evidence the owner must produce), so the real client constructor fails closed with a typed startup
    * error instead of falling back to the ledger. When the dependency lands, this is the single place that builds the
    * SDK client (explicit region from the settings, no endpoint override, credential chain only here, retries disabled,
    * timeouts below the provider timeout, released by the `Resource`).
    */
  val notAvailableInThisBuild: SesNotificationSettings => Resource[IO, SesEmailClient] =
    _ => Resource.eval(IO.raiseError(new SesClientNotAvailableError))
}

/** Everything the SES branch needs, supplied as one value so that a client without its lookups (or the reverse) cannot
  * be wired. It is only used when the configuration selected SES.
  */
final case class SesProviderWiring(
    client: SesNotificationSettings => Resource[IO, SesEmailClient],
    lookups: SesNotificationLookups
)

object SesProviderWiring {

  /** The state of this build: no SES client exists and the lookups are not wired, so selecting SES stops startup. */
  val notAvailableInThisBuild: SesProviderWiring =
    SesProviderWiring(SesClients.notAvailableInThisBuild, SesNotificationLookups.awaitingWiring)
}

private[runtime] object InterviewNotificationProviderSelection {

  /** The ledger provider is the default and builds no SES client. The SES client `Resource` is built only when the
    * configuration selected SES (provider and enable flag agreed during config validation).
    */
  def resource(
      selection: InterviewNotificationConfig,
      ledger: InterviewNotificationReceiptLedger[IO],
      diagnostics: Diagnostics[IO],
      ses: SesProviderWiring
  ): Resource[IO, InterviewNotificationProvider[IO]] =
    selection.selection match {
      case InterviewNotificationSelection.Ledger =>
        Resource.pure(LedgerInterviewNotificationProvider.durable(ledger))
      case InterviewNotificationSelection.Ses(settings) =>
        ses
          .client(settings)
          .map(client =>
            new SesInterviewNotificationProvider(
              client,
              ses.lookups.recipient,
              ses.lookups.context,
              ses.lookups.fence,
              diagnostics
            )
          )
    }
}
