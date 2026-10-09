package com.example.graphQL.cats.runtime

import cats.effect.{Clock, Deferred, IO, Resource, Ref}
import cats.effect.std.{Semaphore, UUIDGen}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewProposalTtl
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices}
import com.example.graphQL.cats.service.port.EmbeddingService
import com.example.graphQL.cats.service.{
  AnalyticsReportingService,
  BackgroundWorker,
  DatabaseProbe,
  EmbeddingCoverageService,
  EmbeddingCoverageUseCases,
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
import com.example.graphQL.cats.service.protocol.{AccountUseCases, JobUseCases, SearchUseCases, UserAuthenticator}
import com.example.graphQL.cats.service.events.{
  OperationalTelemetryService,
  SearchSessionHandoff,
  SearchSessionHandoffConfig
}
import com.example.graphQL.cats.service.search.{
  EmbeddingPipeline,
  EmbeddingWorkPublisher,
  SearchSessionRecording,
  SemanticSearchService
}
import com.example.graphQL.cats.config.{
  JwtAuthConfig,
  KafkaConfig,
  PasswordHashConfig,
  VectorSearchConfig,
  DiscoveryConfig
}
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
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase

final case class MongoHiringRuntime(
    probe: DatabaseProbe,
    services: HiringGraphQLServices,
    userAuthenticator: UserAuthenticator,
    hiringReadiness: IO[ProbeResult]
)

private[runtime] final class SetupLifecycle private (
    completion: Deferred[IO, Either[Throwable, Unit]]
) {
  def await: IO[Boolean] = completion.get.map(_.isRight)

  def awaitSuccessful: IO[Unit] = completion.get.flatMap(IO.fromEither)

  def ready: IO[Boolean] = completion.tryGet.map(_.exists(_.isRight))
}

private[runtime] object SetupLifecycle {
  def resource(setup: IO[Unit], diagnostics: Diagnostics): Resource[IO, SetupLifecycle] =
    Resource.eval(Deferred[IO, Either[Throwable, Unit]]).flatMap { completion =>
      BackgroundWorker
        .resource("mongo-setup", diagnostics)(
          setup.attempt
            .flatTap(
              _.fold(
                error => diagnostics.emit(LogEvent.MongoSetupFailed, fields = LogFields.failure(error)),
                _ => IO.unit
              )
            )
            .flatMap(completion.complete)
            .void
        )
        .as(new SetupLifecycle(completion))
    }

}

object MongoHiringRuntime {
  private val disabledEmbeddingPublisher = new EmbeddingWorkPublisher {
    override def wake: IO[Unit] = IO.unit
  }

  final case class RuntimeConfig(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig,
      jwtAuth: JwtAuthConfig,
      passwordHash: PasswordHashConfig,
      kafka: KafkaConfig,
      resetOnStart: Boolean,
      embeddingService: (VectorSearchConfig, String, Diagnostics) => Resource[IO, EmbeddingService] =
        voyageEmbeddingService,
      discovery: DiscoveryConfig = DiscoveryConfig(),
      adminSeed: com.example.graphQL.cats.config.AdminSeedConfig = com.example.graphQL.cats.config.AdminSeedConfig()
  )

  def resource(config: RuntimeConfig): Resource[IO, MongoHiringRuntime] =
    for {
      passwordHashPermits <- Resource.eval(Semaphore[IO](Runtime.getRuntime.availableProcessors.toLong))
      embeddingHealth <- Resource.eval(Ref.of[IO, Boolean](!config.vectorSearch.enabled))
      discoveryPolicy <- Resource.eval(
        DiscoveryQueryPolicy.create(config.discovery.maxTimeMillis.millis, config.discovery.permits)
      )
      client <- MongoDatabaseProbe.clientResource(config.uri)
      database <- Resource.eval(client.getDatabase(config.databaseName))
      setup <- SetupLifecycle.resource(
        setupEffect(
          database,
          config.vectorSearch,
          config.resetOnStart,
          config.diagnostics,
          config.kafka.interview.topics
        ),
        config.diagnostics
      )
      _ <- Resource.eval(setup.awaitSuccessful)
      fingerprints = new HmacAuthenticationFingerprint(config.jwtAuth.receiptSecret)
      seedAccounts = MongoUserRepository.transactional(
        database,
        client,
        MongoEmbeddingWorkEnqueuer.disabled,
        config.diagnostics
      )
      _ <- Argon2PasswordHasher
        .resource(
          config.passwordHash.iterations,
          config.passwordHash.memoryKilobytes,
          config.passwordHash.parallelism,
          passwordHashPermits
        )
        .evalMap(hasher => MongoAdminSeed.run(database, seedAccounts, hasher, config.adminSeed, Clock[IO], UUIDGen[IO]))
      capability <- embeddingCapability(database, client, config, setup.await, embeddingHealth.set, discoveryPolicy)
      users = capability.users
      applications = MongoApplicationRepository.transactional(database, client, config.diagnostics)
      searchSessions = MongoSearchSessionRepository.transactional(database, client, config.diagnostics)
      searchSessionWork =
        MongoSearchSessionWorkRepository.transactional(database, client, config.diagnostics, UUIDGen[IO])
      outbox = MongoOperationalEventOutboxRepository.transactional(database, client, config.diagnostics, UUIDGen[IO])
      receipts = new MongoConsumerReceiptRepository(database, config.diagnostics)
      mutationReceipts = MongoMutationReceiptRepository.transactional(database, client, config.diagnostics)
      erasureRequests = MongoAnalyticsErasureRequestRepository.transactional(
        database,
        client,
        config.diagnostics,
        config.kafka.interview.topics
      )
      analyticsReports = MongoAnalyticsReportRepository.transactional(database, client, config.diagnostics)
      quarantine = new MongoEventQuarantineRepository(database, config.diagnostics)
      interviewRepository = MongoInterviewWorkflowRepository.live(
        database,
        client,
        config.diagnostics,
        config.kafka.interview.completedDedupRetentionSeconds.seconds
      )
      services <- hiringServices(
        capability,
        applications,
        searchSessions,
        searchSessionWork,
        mutationReceipts,
        erasureRequests,
        analyticsReports,
        new MongoEmbeddingCoverageRepository(database, config.diagnostics),
        config.vectorSearch.durableRetryCapMillis.millis,
        interviewRepository,
        config.kafka,
        config.jwtAuth,
        fingerprints,
        config.passwordHash,
        passwordHashPermits,
        config.diagnostics
      )
      _ <- OperationalEventKafkaRuntime
        .resource(config.kafka, outbox, receipts, quarantine, config.diagnostics, Clock[IO], UUIDGen[IO])
      _ <- InterviewSchedulingRuntime.resource(
        config.kafka,
        interviewRepository,
        new MongoInterviewSubjectCleanup(database, config.diagnostics, config.kafka.interview.topics),
        config.diagnostics
      )
      metadata = MongoDatabaseProbe.connectionMetadata(config.uri, config.databaseName)
    } yield MongoHiringRuntime(
      probe(database, metadata, config.diagnostics, (setup.ready, embeddingHealth.get).mapN(_ && _)),
      services,
      UserAuthenticationService(users),
      (setup.ready, embeddingHealth.get).mapN((setupReady, healthy) =>
        if (setupReady && healthy) ProbeResult.Ready else ProbeResult.Unavailable
      )
    )

  private type RuntimeEmbeddingCapability = EmbeddingCapability[
    MongoEmbeddingWorkRepository,
    MongoUserRepository,
    MongoJobRepository,
    MongoSemanticSearchRepository
  ]

  private def embeddingCapability(
      database: MongoDatabase[IO],
      client: MongoClient[IO],
      config: RuntimeConfig,
      embeddingWorkerReady: IO[Boolean],
      embeddingHealth: Boolean => IO[Unit],
      discoveryPolicy: DiscoveryQueryPolicy
  ): Resource[IO, RuntimeEmbeddingCapability] =
    EmbeddingCapability.resource(
      config.vectorSearch,
      IO(new MongoEmbeddingWorkRepository(database, config.diagnostics, UUIDGen[IO])),
      embeddingWork =>
        IO(
          MongoUserRepository.transactional(
            database,
            client,
            embeddingWork.fold(MongoEmbeddingWorkEnqueuer.disabled)(identity),
            config.diagnostics
          )
        ),
      embeddingWork =>
        IO(
          MongoJobRepository.transactional(
            database,
            client,
            embeddingWork.fold(MongoEmbeddingWorkEnqueuer.disabled)(identity),
            config.diagnostics,
            Some(discoveryPolicy)
          )
        ),
      IO(
        new MongoSemanticSearchRepository(
          database,
          config.vectorSearch.jobVectorIndex,
          config.vectorSearch.candidateVectorIndex,
          config.vectorSearch.jobLexicalIndex,
          config.vectorSearch.candidateLexicalIndex,
          config.vectorSearch.numCandidates,
          config.vectorSearch.branchResultLimit,
          config.vectorSearch.fusionStrategy,
          config.vectorSearch.rerankEnabled,
          config.vectorSearch.rerankModel,
          config.diagnostics,
          Some(discoveryPolicy)
        )
      ),
      (vectorSearch, apiKey) => config.embeddingService(vectorSearch, apiKey, config.diagnostics),
      (work, users, jobs, embeddings) => {
        val embeddingLease =
          (config.vectorSearch.retryAttempts.toLong *
            (config.vectorSearch.timeoutMillis.toLong + config.vectorSearch.retryDelayMillis.toLong)).millis
        EmbeddingPipeline
          .resource(
            work,
            users,
            jobs,
            embeddings,
            config.vectorSearch.voyageModel,
            config.vectorSearch.queueSize,
            config.vectorSearch.parallelism,
            config.vectorSearch.retryAttempts,
            config.vectorSearch.retryDelayMillis.millis,
            embeddingLease,
            embeddingWorkerReady,
            config.diagnostics,
            config.vectorSearch.durableRetryAttempts,
            config.vectorSearch.durableRetryBaseMillis.millis,
            config.vectorSearch.durableRetryCapMillis.millis,
            config.vectorSearch.workerRestartDelayMillis.millis,
            embeddingHealth,
            Clock[IO],
            UUIDGen[IO]
          )
          .map(publisher => publisher: EmbeddingWorkPublisher)
      }
    )

  private def hiringServices(
      capability: RuntimeEmbeddingCapability,
      applications: MongoApplicationRepository,
      searchSessions: MongoSearchSessionRepository,
      searchSessionWork: MongoSearchSessionWorkRepository,
      mutationReceipts: MongoMutationReceiptRepository,
      erasureRequests: MongoAnalyticsErasureRequestRepository,
      analyticsReports: MongoAnalyticsReportRepository,
      embeddingCoverage: MongoEmbeddingCoverageRepository,
      durableRetryCap: FiniteDuration,
      interviewRepository: MongoInterviewWorkflowRepository,
      kafka: KafkaConfig,
      jwtAuth: JwtAuthConfig,
      fingerprints: HmacAuthenticationFingerprint,
      passwordHash: PasswordHashConfig,
      passwordHashPermits: Semaphore[IO],
      diagnostics: Diagnostics
  ): Resource[IO, HiringGraphQLServices] =
    val users = capability.users
    val jobs = capability.jobs
    val tokenIssuer = new JwtAccessTokenIssuer(jwtAuth)
    val idempotent = Idempotent(mutationReceipts)
    val readModel = HiringReadService(users, jobs, applications)
    val applicationService = ApplicationService.live(users, jobs, applications, idempotent)
    val interactionService =
      OperationalTelemetryService.live(users, jobs, searchSessions, searchSessionWork, idempotent)
    val cursorKey = CursorCodec.keyFromSecret(jwtAuth.hmacSecret, jwtAuth.cursorTtlSeconds)

    def assemble(
        proposalTtl: InterviewProposalTtl,
        jobService: JobUseCases,
        accountService: AccountUseCases,
        coverageService: EmbeddingCoverageUseCases,
        semanticSearch: Option[SearchUseCases] = None,
        searchSessionHandoff: SearchSessionHandoff
    ): HiringGraphQLServices =
      HiringGraphQLServices(
        readModel,
        jobService,
        applicationService,
        cursorKey,
        accountService,
        interactionService,
        SearchSessionRecording(searchSessionHandoff),
        semanticSearch,
        AnalyticsReportingService(users, analyticsReports),
        Option.when(kafka.interview.enabled)(
          new com.example.graphQL.cats.service.application.InterviewSchedulingService(
            users,
            jobs,
            applications,
            interviewRepository,
            kafka.interview.preCommitDeadlineSeconds.seconds,
            proposalTtl = proposalTtl
          )
        ),
        coverageService
      )

    // Fail closed: a lifetime the domain rejects stops startup instead of silently becoming the default.
    Resource
      .eval(
        IO.fromEither(
          InterviewProposalTtl
            .from(kafka.interview.proposalTtl)
            .leftMap(error => new IllegalStateException(s"Invalid interview proposal lifetime: $error"))
        )
      )
      .flatMap(proposalTtl =>
        Argon2PasswordHasher
          .resource(
            passwordHash.iterations,
            passwordHash.memoryKilobytes,
            passwordHash.parallelism,
            passwordHashPermits
          )
          .map(hasher => (proposalTtl, hasher))
      )
      .flatMap { (proposalTtl, hasher) =>
        SearchSessionHandoff.resource(searchSessionWork, SearchSessionHandoffConfig(), diagnostics, Clock[IO]).map {
          searchSessionHandoff =>
            capability match {
              case EmbeddingCapability.Disabled(_, _) =>
                val account =
                  UserAccountService(
                    users,
                    users,
                    hasher,
                    tokenIssuer,
                    fingerprints,
                    erasureRequests,
                    disabledEmbeddingPublisher,
                    idempotent,
                    diagnostics
                  )
                val jobService = JobService.live(users, jobs, disabledEmbeddingPublisher, idempotent, diagnostics)
                assemble(
                  proposalTtl,
                  jobService,
                  account,
                  EmbeddingCoverageService.vectorSearchDisabled(users),
                  searchSessionHandoff = searchSessionHandoff
                )
              case EmbeddingCapability.Enabled(_, _, _, search, embeddings, publisher, model) =>
                val jobService = JobService.live(users, jobs, publisher, idempotent, diagnostics)
                val accountService =
                  UserAccountService(
                    users,
                    users,
                    hasher,
                    tokenIssuer,
                    fingerprints,
                    erasureRequests,
                    publisher,
                    idempotent,
                    diagnostics
                  )
                val semanticSearch = SemanticSearchService(
                  users,
                  jobs,
                  embeddings,
                  search,
                  model
                )
                assemble(
                  proposalTtl,
                  jobService,
                  accountService,
                  EmbeddingCoverageService.live(users, embeddingCoverage, durableRetryCap, clock = Clock[IO]),
                  Some(semanticSearch),
                  searchSessionHandoff
                )
            }
        }
      }

  private def voyageEmbeddingService(
      config: VectorSearchConfig,
      apiKey: String,
      diagnostics: Diagnostics
  ): Resource[IO, EmbeddingService] =
    VoyageEmbeddingService.resource(
      apiKey,
      config.voyageEndpoint,
      config.voyageModel,
      config.voyageDimension,
      config.timeoutMillis.millis,
      diagnostics = diagnostics
    )

  private def probe(
      database: MongoDatabase[IO],
      metadata: Map[LogField, String],
      diagnostics: Diagnostics,
      setupReady: IO[Boolean]
  ): DatabaseProbe = new DatabaseProbe {
    private val delegate = MongoDatabaseProbe.fromDatabase(database, metadata, diagnostics)

    override def check(requestId: Option[String]): IO[ProbeResult] =
      delegate.check(requestId).flatMap {
        case ProbeResult.Ready => setupReady.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
        case other             => IO.pure(other)
      }
  }

  private def setupEffect(
      database: MongoDatabase[IO],
      vectorSearch: VectorSearchConfig,
      resetOnStart: Boolean,
      diagnostics: Diagnostics,
      topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair
  ): IO[Unit] =
    MongoHiringSetup.initialize(
      database,
      Option.when(vectorSearch.enabled)(
        AtlasSearchIndexConfig(
          vectorSearch.jobVectorIndex,
          vectorSearch.candidateVectorIndex,
          vectorSearch.jobLexicalIndex,
          vectorSearch.candidateLexicalIndex,
          vectorSearch.voyageDimension,
          vectorSearch.indexReadyTimeoutMillis,
          vectorSearch.indexPollIntervalMillis
        )
      ),
      resetOnStart,
      diagnostics,
      topics
    )

}
