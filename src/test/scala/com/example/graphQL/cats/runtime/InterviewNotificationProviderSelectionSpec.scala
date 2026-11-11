package com.example.graphQL.cats.runtime

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.config.{
  InterviewNotificationConfig,
  InterviewNotificationSelection,
  SesNotificationSettings,
  SesRegion
}
import com.example.graphQL.cats.domain.model.EmailAddress
import com.example.graphQL.cats.infrastructure.ses.{
  SesEmailClient,
  SesEmailRequest,
  SesInterviewNotificationProvider,
  SesSendOutcome
}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{
  ClaimedInterviewWorkflowCommand,
  InterviewNotificationReceipt,
  InterviewNotificationReceiptLedger,
  InterviewProviderF
}
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{InterviewNotificationKind, InterviewParticipant, InterviewWorkflowId}
import java.time.Instant
import java.util.UUID
import cats.effect.Resource
import io.github.iltotore.iron.autoRefine
import munit.CatsEffectSuite

class InterviewNotificationProviderSelectionSpec extends CatsEffectSuite {
  private val ledger = new InterviewNotificationReceiptLedger[IO] {
    def deliverOnce(
        receipt: InterviewNotificationReceipt,
        execution: Option[ClaimedInterviewWorkflowCommand]
    ): InterviewProviderF[IO, InterviewNotificationReceipt] = EitherT.rightT(receipt)
    def find(idempotencyKey: String): InterviewProviderF[IO, Option[InterviewNotificationReceipt]] =
      EitherT.rightT(None)
  }
  private val region: SesRegion = "eu-west-1"
  private val settings = SesNotificationSettings(
    region,
    EmailAddress.from("hiring@example.invalid").toOption.get,
    None
  )
  private val sesSelected = InterviewNotificationConfig(InterviewNotificationSelection.Ses(settings))

  private val noopClient = new SesEmailClient {
    def send(request: SesEmailRequest): SesSendOutcome = SesSendOutcome.Accepted("id")
  }

  test("DHW-26 the ledger default builds no SES client") {
    for {
      built <- Ref.of[IO, Int](0)
      provider <- InterviewNotificationProviderSelection
        .resource(
          InterviewNotificationConfig(),
          ledger,
          Diagnostics.noop[IO],
          SesProviderWiring(
            _ => Resource.eval(built.update(_ + 1).as(noopClient)),
            SesNotificationLookups.awaitingWiring
          )
        )
        .use(IO.pure)
      clients <- built.get
    } yield {
      assertEquals(clients, 0)
      assert(!provider.isInstanceOf[SesInterviewNotificationProvider])
    }
  }

  test("DHW-26 a selected SES builds the client Resource once and releases it") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      provider <- InterviewNotificationProviderSelection
        .resource(
          sesSelected,
          ledger,
          Diagnostics.noop[IO],
          SesProviderWiring(
            _ => Resource.make(events.update(_ :+ "built").as(noopClient))(_ => events.update(_ :+ "released")),
            SesNotificationLookups.awaitingWiring
          )
        )
        .use(provider => events.get.map(during => (provider, during)))
      after <- events.get
    } yield {
      assert(provider._1.isInstanceOf[SesInterviewNotificationProvider])
      assertEquals(provider._2, Vector("built"))
      assertEquals(after, Vector("built", "released"))
    }
  }

  test("DHW-26 a selected SES without a client in this build fails closed, never falling back to the ledger") {
    InterviewNotificationProviderSelection
      .resource(sesSelected, ledger, Diagnostics.noop[IO], SesProviderWiring.notAvailableInThisBuild)
      .use(IO.pure)
      .attempt
      .map {
        case Left(error) =>
          assert(error.isInstanceOf[SesClientNotAvailableError], clues(error))
          assertEquals(error.getMessage, "SES client not available in this build")
        case Right(provider) =>
          fail(s"Expected a startup error, got a provider: ${provider.getClass.getName}")
      }
  }

  test("DHW-26 the default provider is the receipt ledger: it answers lookups from the ledger, not from SES") {
    val stored = InterviewNotificationReceipt(
      InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-000000000c26")),
      UserId(UUID.fromString("00000000-0000-0000-0000-000000000d26")),
      InterviewParticipant.Candidate,
      InterviewNotificationKind.RescheduleProposed,
      "ledger-key",
      Instant.parse("2026-11-10T10:00:00Z")
    )
    val storingLedger = new InterviewNotificationReceiptLedger[IO] {
      def deliverOnce(
          receipt: InterviewNotificationReceipt,
          execution: Option[ClaimedInterviewWorkflowCommand]
      ): InterviewProviderF[IO, InterviewNotificationReceipt] = EitherT.rightT(receipt)
      def find(idempotencyKey: String): InterviewProviderF[IO, Option[InterviewNotificationReceipt]] =
        EitherT.rightT(Some(stored).filter(_.idempotencyKey == idempotencyKey))
    }
    for {
      sesClients <- Ref.of[IO, Int](0)
      wiring = SesProviderWiring(
        _ => Resource.eval(sesClients.update(_ + 1).as(noopClient)),
        SesNotificationLookups.awaitingWiring
      )
      result <- InterviewNotificationProviderSelection
        .resource(InterviewNotificationConfig(), storingLedger, Diagnostics.noop[IO], wiring)
        .use(provider => (provider.lookup("ledger-key").value, provider.lookup("absent").value).tupled)
      builtClients <- sesClients.get
    } yield {
      assertEquals(result._1, Right(Some(stored)))
      assertEquals(result._2, Right(None))
      assertEquals(builtClients, 0)
    }
  }
}
