package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices}
import com.example.graphQL.cats.service.port.EmbeddingService
import com.example.graphQL.cats.service.{
  AnalyticsReportingService,
  DatabaseProbe,
  Diagnostics,
  HiringReadService,
  LogEvent,
  LogField,
  LogFields,
  ProbeResult
}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.auth.{Argon2PasswordHasher, UserAccountService, UserAuthenticationService}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{AccountUseCases, JobUseCases, SearchUseCases, UserAuthenticator}
import com.example.graphQL.cats.service.events.{
  OperationalTelemetryService,
  SearchSessionHandoff,
  SearchSessionHandoffConfig
}
import com.example.graphQL.cats.service.search.{EmbeddingPipeline, EmbeddingWorkPublisher, SemanticSearchService}
import com.example.graphQL.cats.config.{JwtAuthConfig, KafkaConfig, PasswordHashConfig, VectorSearchConfig}
import com.example.graphQL.cats.infrastructure.auth.JwtAccessTokenIssuer
import com.example.graphQL.cats.infrastructure.kafka.OperationalEventKafkaRuntime
import scala.concurrent.duration.*
import com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService
import com.example.graphQL.cats.repository.mongo.{
  MongoApplicationRepository,
  MongoInterviewWorkflowRepository,
  MongoInterviewSubjectCleanup,
  MongoConsumerReceiptRepository,
  MongoDatabaseProbe,
  MongoEventQuarantineRepository,
  MongoHiringSetup,
  MongoJobRepository,
  MongoAnalyticsErasureRequestRepository,
  MongoAnalyticsReportRepository,
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
      Resource
        .make(
          setup.attempt
            .flatTap(
              _.fold(
                error => diagnostics.emit(LogEvent.MongoSetupFailed, fields = LogFields.failure(error)),
                _ => IO.unit
              )
            )
            .flatMap(completion.complete)
            .start
        )(_.cancel)
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
        voyageEmbeddingService
  )

  def resource(config: RuntimeConfig): Resource[IO, MongoHiringRuntime] =
    for {
      passwordHashPermits <- Resource.eval(Semaphore[IO](Runtime.getRuntime.availableProcessors.toLong))
      client <- MongoDatabaseProbe.clientResource(config.uri)
      database <- Resource.eval(client.getDatabase(config.databaseName))
      setup <- SetupLifecycle.resource(
        setupEffect(database, config.vectorSearch, config.resetOnStart, config.diagnostics),
        config.diagnostics
      )
      capability <- embeddingCapability(database, client, config, setup.await)
      users = capability.users
      applications = MongoApplicationRepository.transactional(database, client, config.diagnostics)
      searchSessions = MongoSearchSessionRepository.transactional(database, client, config.diagnostics)
      searchSessionWork = MongoSearchSessionWorkRepository.transactional(database, client, config.diagnostics)
      outbox = MongoOperationalEventOutboxRepository.transactional(database, client, config.diagnostics)
      receipts = new MongoConsumerReceiptRepository(database, config.diagnostics)
      mutationReceipts = MongoMutationReceiptRepository.transactional(database, client, config.diagnostics)
      erasureRequests = MongoAnalyticsErasureRequestRepository.transactional(database, client, config.diagnostics)
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
        interviewRepository,
        config.kafka,
        config.jwtAuth,
        config.passwordHash,
        passwordHashPermits,
        config.diagnostics
      )
      _ <- Resource.eval(setup.awaitSuccessful)
      _ <- OperationalEventKafkaRuntime.resource(config.kafka, outbox, receipts, quarantine, config.diagnostics)
      _ <- InterviewSchedulingRuntime.resource(
        config.kafka,
        interviewRepository,
        new MongoInterviewSubjectCleanup(database),
        config.diagnostics
      )
      metadata = MongoDatabaseProbe.connectionMetadata(config.uri, config.databaseName)
    } yield MongoHiringRuntime(
      probe(database, metadata, config.diagnostics, setup.ready),
      services,
      UserAuthenticationService(users),
      setup.ready.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
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
      embeddingWorkerReady: IO[Boolean]
  ): Resource[IO, RuntimeEmbeddingCapability] =
    EmbeddingCapability.resource(
      config.vectorSearch,
      IO(new MongoEmbeddingWorkRepository(database, config.diagnostics)),
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
            config.diagnostics
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
          config.diagnostics
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
            config.diagnostics
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
      interviewRepository: MongoInterviewWorkflowRepository,
      kafka: KafkaConfig,
      jwtAuth: JwtAuthConfig,
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
        jobService: JobUseCases,
        accountService: AccountUseCases,
        semanticSearch: Option[SearchUseCases] = None,
        searchSessionHandoff: SearchSessionHandoff
    ): HiringGraphQLServices =
      HiringGraphQLServices(
        readModel,
        jobService,
        applicationService,
        cursorKey,
        accountService,
        semanticSearch,
        interactionService,
        searchSessionHandoff,
        AnalyticsReportingService(users, analyticsReports),
        Option.when(kafka.interview.enabled)(
          new com.example.graphQL.cats.service.application.InterviewSchedulingService(
            users,
            jobs,
            applications,
            interviewRepository,
            kafka.interview.preCommitDeadlineSeconds.seconds
          )
        )
      )

    Argon2PasswordHasher
      .resource(
        passwordHash.iterations,
        passwordHash.memoryKilobytes,
        passwordHash.parallelism,
        passwordHashPermits
      )
      .flatMap { hasher =>
        SearchSessionHandoff.resource(searchSessionWork, SearchSessionHandoffConfig(), diagnostics).map {
          searchSessionHandoff =>
            capability match {
              case EmbeddingCapability.Disabled(_, _) =>
                val account =
                  UserAccountService(
                    users,
                    users,
                    hasher,
                    tokenIssuer,
                    erasureRequests,
                    disabledEmbeddingPublisher,
                    idempotent,
                    diagnostics
                  )
                val jobService = JobService.live(users, jobs, disabledEmbeddingPublisher, idempotent, diagnostics)
                assemble(jobService, account, searchSessionHandoff = searchSessionHandoff)
              case EmbeddingCapability.Enabled(_, _, _, search, embeddings, publisher, model) =>
                val jobService = JobService.live(users, jobs, publisher, idempotent, diagnostics)
                val accountService =
                  UserAccountService(
                    users,
                    users,
                    hasher,
                    tokenIssuer,
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
                assemble(jobService, accountService, Some(semanticSearch), searchSessionHandoff)
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

    override def check: IO[ProbeResult] =
      check(None)

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
      diagnostics: Diagnostics
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
      diagnostics
    )

}
