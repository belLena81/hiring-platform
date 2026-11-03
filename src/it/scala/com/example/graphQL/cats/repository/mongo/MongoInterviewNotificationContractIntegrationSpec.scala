package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.InterviewInterval
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.duration.*

/** Runs the shared notification contract against the platform's durable receipt ledger on a real replica set. */
final class MongoInterviewNotificationContractIntegrationSpec
    extends MongoIntegrationSuite
    with InterviewNotificationProviderContract {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  override protected def withSubject[A](use: InterviewNotificationContractSubject => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop[IO])
      val subject = new InterviewNotificationContractSubject {
        override val provider: InterviewNotificationProvider[IO] =
          LedgerInterviewNotificationProvider.durable(repository)

        override def newTarget: IO[InterviewNotificationTarget] = {
          val start = Instant.now().truncatedTo(ChronoUnit.MILLIS).plus(3, ChronoUnit.DAYS)
          MongoInterviewContractWorkflows
            .create(
              fixture,
              repository,
              UserId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              InterviewInterval(start, start.plus(1, ChronoUnit.HOURS))
            )
            .map(workflow => InterviewNotificationTarget(workflow.id, workflow.candidateId))
        }
      }
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *> use(subject)
    }
}
