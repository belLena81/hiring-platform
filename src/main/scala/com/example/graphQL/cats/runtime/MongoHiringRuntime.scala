package com.example.graphQL.cats.runtime

import cats.effect.{Clock, IO, Resource, Ref}
import cats.effect.std.{Semaphore, UUIDGen}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewProposalTtl
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices}
import com.example.graphQL.cats.service.port.EmbeddingService
import com.example.graphQL.cats.service.{
  AnalyticsReportingService,
  DatabaseProbe,
  EmbeddingCoverageService,
  Diagnostics,
  HiringReadService,
  LogEvent,
  LogField,
  LogFields,
  ProbeResult
}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.auth.{UserAccountService, UserAuthenticationService}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import com.example.graphQL.cats.service.events.{
  OperationalTelemetryService,
  SearchSessionHandoff,
  SearchSessionHandoffConfig
}
import com.example.graphQL.cats.service.search.{
  DurableRetrySettings,
  EmbeddingPipeline,
  EmbeddingWorkPublisher,
  SearchSessionRecording,
  SemanticSearchService
}
import com.example.graphQL.cats.config.{AppConfig, EmbeddingConfig, VectorSearchConfig, VoyageConfig}
import com.example.graphQL.cats.infrastructure.auth.{
  JwtAccessTokenIssuer,
  Argon2PasswordHasher,
  HmacAuthenticationFingerprint
}
import com.example.graphQL.cats.infrastructure.kafka.OperationalEventKafkaRuntime
import scala.concurrent.duration.*
import com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService
import com.example.graphQL.cats.repository.mongo.{
  MongoApplicationRepository,
  MongoAdminSeed,
  MongoInterviewWorkflowRepository,
  MongoInterviewSubjectCleanup,
  MongoConsumerReceiptRepository,
  MongoDatabaseProbe,
  MongoEventQuarantineRepository,
  MongoHiringSetup,
  MongoJobRepository,
  DiscoveryQueryPolicy,
  MongoAnalyticsErasureRequestRepository,
  MongoAnalyticsReportRepository,
  MongoEmbeddingCoverageRepository,
  MongoMutationReceiptRepository,
  MongoOperationalEventOutboxRepository,
  MongoSearchSessionRepository,
  MongoSearchSessionWorkRepository,
  MongoSemanticSearchRepository,
  MongoUserRepository,
  MongoEmbeddingWorkRepository,
  MongoEmbeddingWorkEnqueuer,
  AtlasSearchIndexConfig
}
import mongo4cats.database.MongoDatabase

final case class MongoHiringRuntime(
    probe: DatabaseProbe,
    services: HiringGraphQLServices,
    userAuthenticator: UserAuthenticator,
    hiringReadiness: IO[ProbeResult]
)

object MongoHiringRuntime {
  private val disabledEmbeddingPublisher = new EmbeddingWorkPublisher {
    override def wake: IO[Unit] = IO.unit
  }

  /** `embeddingService` is the test seam for the embedding provider; production uses Voyage. */
  def resource(
      config: AppConfig,
      diagnostics: Diagnostics,
      embeddingService: (VoyageConfig, EmbeddingConfig, Diagnostics) => Resource[IO, EmbeddingService] =
        voyageEmbeddingService
  ): Resource[IO, MongoHiringRuntime] =
    for {
      passwordHashPermits <- Resource.eval(Semaphore[IO](Runtime.getRuntime.availableProcessors.toLong))
      embeddingHealth <- Resource.eval(Ref.of[IO, Boolean](config.vectorSearch == VectorSearchConfig.Disabled))
      discoveryPolicy <- Resource.eval(
        DiscoveryQueryPolicy.create(config.discovery.maxTimeMillis.millis, config.discovery.permits)
      )
      client <- MongoDatabaseProbe.clientResource(config.mongoUri)
      database <- Resource.eval(client.getDatabase(config.mongoDatabase))
      // Startup fails closed: nothing below runs, and readiness never opens, unless setup succeeded.
      _ <- Resource.eval(
        setupEffect(database, config, diagnostics).onError { case error =>
          diagnostics.emit(LogEvent.MongoSetupFailed, fields = LogFields.failure(error))
        }
      )
      hasher <- Resource.eval(Argon2PasswordHasher.create(config.passwordHash, passwordHashPermits))
      _ <- Resource.eval(
        MongoAdminSeed.run(
          database,
          MongoUserRepository.transactional(database, client, MongoEmbeddingWorkEnqueuer.disabled, diagnostics),
          hasher,
          config.adminSeed,
          Clock[IO],
          UUIDGen[IO]
        )
      )
      embeddingWork = new MongoEmbeddingWorkRepository(database, diagnostics, UUIDGen[IO])
      enqueuer =
        if (config.vectorSearch == VectorSearchConfig.Disabled) MongoEmbeddingWorkEnqueuer.disabled
        else embeddingWork
      users = MongoUserRepository.transactional(database, client, enqueuer, diagnostics)
      jobs = MongoJobRepository.transactional(database, client, enqueuer, diagnostics, Some(discoveryPolicy))
      applications = MongoApplicationRepository.transactional(database, client, diagnostics)
      searchSessions = MongoSearchSessionRepository.transactional(database, client, diagnostics)
      searchSessionWork = MongoSearchSessionWorkRepository.transactional(database, client, diagnostics, UUIDGen[IO])
      outbox = MongoOperationalEventOutboxRepository.transactional(database, client, diagnostics, UUIDGen[IO])
      receipts = new MongoConsumerReceiptRepository(database, diagnostics)
      mutationReceipts = MongoMutationReceiptRepository.transactional(database, client, diagnostics)
      erasureRequests = MongoAnalyticsErasureRequestRepository.transactional(
        database,
        client,
        diagnostics,
        config.kafka.interview.topics
      )
      analyticsReports = MongoAnalyticsReportRepository.transactional(database, client, diagnostics)
      quarantine = new MongoEventQuarantineRepository(database, diagnostics)
      interviewRepository = MongoInterviewWorkflowRepository.live(
        database,
        client,
        diagnostics,
        config.kafka.interview.completedDedupRetentionSeconds.seconds
      )
      // Fail closed: a lifetime the domain rejects stops startup instead of silently becoming the default.
      proposalTtl <- Resource.eval(
        IO.fromEither(
          InterviewProposalTtl
            .from(config.kafka.interview.proposalTtl)
            .leftMap(error => new IllegalStateException(s"Invalid interview proposal lifetime: $error"))
        )
      )
      searchSessionHandoff <- SearchSessionHandoff
        .resource(searchSessionWork, SearchSessionHandoffConfig(), diagnostics, Clock[IO])
      // One branch decides the embedding capability; everything below is shared by both modes.
      embedding <- config.vectorSearch match {
        case VectorSearchConfig.Disabled =>
          Resource.pure[IO, (EmbeddingWorkPublisher, Option[SemanticSearchService], EmbeddingCoverageService)](
            (disabledEmbeddingPublisher, None, EmbeddingCoverageService.vectorSearchDisabled(users))
          )
        case VectorSearchConfig.Enabled(
              voyage,
              embeddingSettings,
              indexes,
              numCandidates,
              branchLimit,
              fusion,
              rerank
            ) =>
          for {
            provider <- embeddingService(voyage, embeddingSettings, diagnostics)
            publisher <- EmbeddingPipeline.resource(
              embeddingWork,
              users,
              jobs,
              provider,
              voyage.model,
              embeddingSettings.queueSize,
              embeddingSettings.parallelism,
              embeddingSettings.retryAttempts,
              embeddingSettings.retryDelayMs.millis,
              (embeddingSettings.retryAttempts.toLong *
                (embeddingSettings.timeoutMs.toLong + embeddingSettings.retryDelayMs.toLong)).millis,
              IO.pure(true),
              diagnostics,
              new DurableRetrySettings(
                embeddingSettings.durableRetryAttempts,
                embeddingSettings.durableRetryBaseMillis.millis,
                embeddingSettings.durableRetryCapMillis.millis
              ),
              embeddingSettings.workerRestartDelayMillis.millis,
              embeddingHealth.set,
              Clock[IO],
              UUIDGen[IO]
            )
            search = new MongoSemanticSearchRepository(
              database,
              indexes.jobs,
              indexes.candidates,
              indexes.lexical,
              indexes.candidateLexical,
              numCandidates,
              branchLimit,
              fusion,
              rerank.enabled,
              rerank.model,
              diagnostics,
              Some(discoveryPolicy)
            )
          } yield (
            publisher: EmbeddingWorkPublisher,
            Some(SemanticSearchService(users, jobs, provider, search, voyage.model)),
            EmbeddingCoverageService.live(
              users,
              new MongoEmbeddingCoverageRepository(database, diagnostics),
              embeddingSettings.durableRetryCapMillis.millis,
              clock = Clock[IO]
            )
          )
      }
      (publisher, semanticSearch, coverage) = embedding
      idempotent = Idempotent(mutationReceipts)
      accountService = UserAccountService(
        users,
        users,
        hasher,
        new JwtAccessTokenIssuer(config.jwtAuth),
        new HmacAuthenticationFingerprint(config.jwtAuth.receiptSecret),
        erasureRequests,
        publisher,
        idempotent,
        diagnostics
      )
      services = HiringGraphQLServices(
        HiringReadService(users, jobs, applications),
        JobService.live(users, jobs, publisher, idempotent, diagnostics),
        ApplicationService.live(users, jobs, applications, idempotent),
        CursorCodec.keyFromSecret(config.jwtAuth.hmacSecret, config.jwtAuth.cursorTtlSeconds),
        accountService,
        OperationalTelemetryService.live(users, jobs, searchSessions, searchSessionWork, idempotent),
        SearchSessionRecording(searchSessionHandoff),
        semanticSearch,
        AnalyticsReportingService(users, analyticsReports),
        Option.when(config.kafka.interview.enabled)(
          new com.example.graphQL.cats.service.application.InterviewSchedulingService(
            users,
            jobs,
            applications,
            interviewRepository,
            config.kafka.interview.preCommitDeadlineSeconds.seconds,
            proposalTtl = proposalTtl
          )
        ),
        coverage
      )
      _ <- OperationalEventKafkaRuntime
        .resource(config.kafka, outbox, receipts, quarantine, diagnostics, Clock[IO], UUIDGen[IO])
      _ <- InterviewSchedulingRuntime.resource(
        config.kafka,
        interviewRepository,
        new MongoInterviewSubjectCleanup(database, diagnostics, config.kafka.interview.topics),
        diagnostics
      )
      metadata = MongoDatabaseProbe.connectionMetadata(config.mongoUri, config.mongoDatabase)
    } yield MongoHiringRuntime(
      probe(database, metadata, diagnostics, embeddingHealth.get),
      services,
      UserAuthenticationService(users),
      embeddingHealth.get.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
    )

  private def voyageEmbeddingService(
      voyage: VoyageConfig,
      embedding: EmbeddingConfig,
      diagnostics: Diagnostics
  ): Resource[IO, EmbeddingService] =
    VoyageEmbeddingService.resource(
      voyage.apiKey,
      voyage.endpoint,
      voyage.model,
      voyage.dimension,
      embedding.timeoutMs.millis,
      diagnostics = diagnostics
    )

  private def probe(
      database: MongoDatabase[IO],
      metadata: Map[LogField, String],
      diagnostics: Diagnostics,
      embeddingHealthy: IO[Boolean]
  ): DatabaseProbe = new DatabaseProbe {
    private val delegate = MongoDatabaseProbe.fromDatabase(database, metadata, diagnostics)

    override def check(requestId: Option[String]): IO[ProbeResult] =
      delegate.check(requestId).flatMap {
        case ProbeResult.Ready => embeddingHealthy.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
        case other             => IO.pure(other)
      }
  }

  private def setupEffect(database: MongoDatabase[IO], config: AppConfig, diagnostics: Diagnostics): IO[Unit] =
    MongoHiringSetup.initialize(
      database,
      config.vectorSearch match {
        case VectorSearchConfig.Disabled         => None
        case enabled: VectorSearchConfig.Enabled =>
          Some(
            AtlasSearchIndexConfig(
              enabled.indexes.jobs,
              enabled.indexes.candidates,
              enabled.indexes.lexical,
              enabled.indexes.candidateLexical,
              enabled.voyage.dimension,
              enabled.indexes.readyTimeoutMs,
              enabled.indexes.pollIntervalMs
            )
          )
      },
      config.resetOnStart,
      diagnostics,
      config.kafka.interview.topics
    )

}
