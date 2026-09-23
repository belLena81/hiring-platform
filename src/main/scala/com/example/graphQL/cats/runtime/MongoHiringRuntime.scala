package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices}
import com.example.graphQL.cats.repository.protocol.EmbeddingService
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
  AtlasSearchIndexConfig
}
import com.mongodb.reactivestreams.client.MongoDatabase

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

  def resource(setup: IO[Unit]): Resource[IO, SetupLifecycle] = resource(setup, Diagnostics.noop)
}

object MongoHiringRuntime {
  final case class RuntimeConfig(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig,
      jwtAuth: JwtAuthConfig,
      passwordHash: PasswordHashConfig,
      kafka: KafkaConfig,
      resetOnStart: Boolean,
      embeddingService: (VectorSearchConfig, String) => Resource[IO, EmbeddingService] = voyageEmbeddingService
  )

  def resource(config: RuntimeConfig): Resource[IO, MongoHiringRuntime] =
    for {
      passwordHashPermits <- Resource.eval(Semaphore[IO](Runtime.getRuntime.availableProcessors.toLong))
      client <- MongoDatabaseProbe.clientResource(config.uri)
      database = client.getDatabase(config.databaseName)
      setup <- SetupLifecycle.resource(
        setupEffect(database, config.vectorSearch, config.resetOnStart),
        config.diagnostics
      )
      capability <- embeddingCapability(database, client, config, setup.await)
      users = capability.users
      applications = MongoApplicationRepository.transactional(database, client)
      searchSessions = MongoSearchSessionRepository.transactional(database, client)
      searchSessionWork = MongoSearchSessionWorkRepository.transactional(database, client)
      outbox = new MongoOperationalEventOutboxRepository(database)
      receipts = new MongoConsumerReceiptRepository(database)
      mutationReceipts = MongoMutationReceiptRepository.transactional(database, client)
      erasureRequests = MongoAnalyticsErasureRequestRepository.transactional(database, client)
      analyticsReports = new MongoAnalyticsReportRepository(database)
      quarantine = new MongoEventQuarantineRepository(database)
      services <- hiringServices(
        capability,
        applications,
        searchSessions,
        searchSessionWork,
        mutationReceipts,
        erasureRequests,
        analyticsReports,
        config.jwtAuth,
        config.passwordHash,
        passwordHashPermits,
        config.diagnostics
      )
      _ <- OperationalEventKafkaRuntime.resource(config.kafka, outbox, receipts, quarantine, config.diagnostics)
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
      database: MongoDatabase,
      client: com.mongodb.reactivestreams.client.MongoClient,
      config: RuntimeConfig,
      embeddingWorkerReady: IO[Boolean]
  ): Resource[IO, RuntimeEmbeddingCapability] =
    EmbeddingCapability.resource(
      config.vectorSearch,
      IO(new MongoEmbeddingWorkRepository(database)),
      embeddingWork => IO(MongoUserRepository.transactional(database, client, embeddingWork)),
      embeddingWork => IO(MongoJobRepository.transactional(database, client, embeddingWork)),
      IO(
        new MongoSemanticSearchRepository(
          database,
          config.vectorSearch.jobVectorIndex,
          config.vectorSearch.candidateVectorIndex,
          config.vectorSearch.jobLexicalIndex,
          config.vectorSearch.numCandidates
        )
      ),
      config.embeddingService,
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
            embeddingWorkerReady
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
      jwtAuth: JwtAuthConfig,
      passwordHash: PasswordHashConfig,
      passwordHashPermits: Semaphore[IO],
      diagnostics: Diagnostics
  ): Resource[IO, HiringGraphQLServices] =
    val users = capability.users
    val jobs = capability.jobs
    val hasher = new Argon2PasswordHasher(
      passwordHash.iterations,
      passwordHash.memoryKilobytes,
      passwordHash.parallelism,
      passwordHashPermits
    )
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
        searchSessions,
        searchSessionHandoff,
        AnalyticsReportingService(users, analyticsReports)
      )

    SearchSessionHandoff.resource(searchSessionWork, SearchSessionHandoffConfig(), diagnostics).map {
      searchSessionHandoff =>
        capability match {
          case EmbeddingCapability.Disabled(_, _) =>
            val account =
              UserAccountService(users, users, hasher, tokenIssuer, erasureRequests, idempotent = idempotent)
            val jobService = JobService.live(users, jobs, EmbeddingWorkPublisher.noop, idempotent)
            assemble(jobService, account, searchSessionHandoff = searchSessionHandoff)
          case EmbeddingCapability.Enabled(_, _, _, search, embeddings, publisher, model) =>
            val jobService = JobService.live(users, jobs, publisher, idempotent)
            val accountService =
              UserAccountService(users, users, hasher, tokenIssuer, erasureRequests, publisher, idempotent)
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

  private def voyageEmbeddingService(config: VectorSearchConfig, apiKey: String): Resource[IO, EmbeddingService] =
    VoyageEmbeddingService.resource(
      apiKey,
      config.voyageEndpoint,
      config.voyageModel,
      config.voyageDimension,
      config.timeoutMillis.millis
    )

  private def probe(
      database: MongoDatabase,
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
      database: com.mongodb.reactivestreams.client.MongoDatabase,
      vectorSearch: VectorSearchConfig,
      resetOnStart: Boolean
  ): IO[Unit] =
    MongoHiringSetup.initialize(
      database,
      Option.when(vectorSearch.enabled)(
        AtlasSearchIndexConfig(
          vectorSearch.jobVectorIndex,
          vectorSearch.candidateVectorIndex,
          vectorSearch.jobLexicalIndex,
          vectorSearch.voyageDimension,
          vectorSearch.indexReadyTimeoutMillis,
          vectorSearch.indexPollIntervalMillis
        )
      ),
      resetOnStart
    )

}
