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
    InterviewNotificationProviderSelection
      .resource(InterviewNotificationConfig(), ledger, Diagnostics.noop[IO], SesProviderWiring.notAvailableInThisBuild)
      .use(provider => provider.lookup("absent").value)
      .map(found => assertEquals(found, Right(None)))
  }
}
