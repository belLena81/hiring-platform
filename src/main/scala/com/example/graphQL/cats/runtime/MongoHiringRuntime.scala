package com.example.graphQL.cats.runtime

import cats.effect.{Clock, IO, Resource, Ref}
import com.example.graphQL.cats.shared.MillisecondClock
import cats.effect.std.{Semaphore, UUIDGen}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewProposalTtl
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices}
import com.example.graphQL.cats.service.{
  AnalyticsReportingService,
  DatabaseProbe,
  EmbeddingCoverageService,
  Diagnostics,
  HiringReadService,
  LogEvent,
  LogField,
  LogFields,
  ProbeResult,
  DegradedComponents,
  ReadinessComponent,
  ReadinessThresholds,
  WorkerHeartbeats
}
import com.example.graphQL.cats.service.application.{ApplicationService, InterviewSchedulingService}
import com.example.graphQL.cats.service.auth.{
  RecruiterJobClosureSchedule,
  RecruiterJobClosureWorker,
  UserAccountService,
  UserAuthenticationService
}
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
  LexicalMatchLimit,
  SearchSessionRecording,
  SemanticSearchService
}
import com.example.graphQL.cats.config.{AppConfig, RecruiterJobClosureConfig, VectorSearchConfig}
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
  MongoRecruiterJobClosure,
  MongoConsumerReceiptRepository,
  MongoDatabaseProbe,
  MongoEventQuarantineRepository,
  MongoHiringSetup,
  MongoJobRepository,
  MongoLexicalJobMatcher,
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
import com.example.graphQL.cats.service.port.{JobClosurePageSize, JobClosureRetry, LexicalJobMatcher}
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import mongo4cats.database.MongoDatabase

final case class MongoHiringRuntime(
    probe: DatabaseProbe[IO],
    services: HiringGraphQLServices,
    userAuthenticator: UserAuthenticator[IO],
    hiringReadiness: IO[ProbeResult],
    degradedComponents: IO[List[ReadinessComponent]]
)

object MongoHiringRuntime {
  private val disabledEmbeddingPublisher = new EmbeddingWorkPublisher[IO] {
    override def wake: IO[Unit] = IO.unit
  }

  def resource(config: AppConfig, diagnostics: Diagnostics[IO]): Resource[IO, MongoHiringRuntime] = {
    // Every service-stamped timestamp is persisted by MongoDB at millisecond precision; one clock keeps them equal.
    val clock: Clock[IO] = MillisecondClock(Clock[IO])
    for {
      passwordHashPermits <- Resource.eval(Semaphore[IO](Runtime.getRuntime.availableProcessors.toLong))
      embeddingHealth <- Resource.eval(Ref.of[IO, Boolean](config.vectorSearch == VectorSearchConfig.Disabled))
      breakers <- Resource.eval(DependencyBreakers.create(config.resilience, diagnostics))
      heartbeats <- Resource.eval(WorkerHeartbeats.create[IO])
      // An idle worker reports at least three times per staleness window.
      alive = (component: ReadinessComponent) =>
        heartbeats.alive(component, (config.readiness.workerStalenessSeconds.seconds / 3).max(1.second))
      discoveryPolicy <- Resource.eval(
        DiscoveryQueryPolicy.create(
          config.discovery.maxTimeMillis.millis,
          config.discovery.permits,
          breakers.atlasSearch
        )
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
          clock,
          UUIDGen[IO]
        )
      )
      embeddingWork = new MongoEmbeddingWorkRepository(database, diagnostics, UUIDGen[IO])
      enqueuer =
        if (config.vectorSearch == VectorSearchConfig.Disabled) MongoEmbeddingWorkEnqueuer.disabled
        else embeddingWork
      closureSettings <- Resource.eval(recruiterJobClosureSettings(config.recruiterJobClosure))
      users = MongoUserRepository.transactional(
        database,
        client,
        enqueuer,
        diagnostics,
        breakers.mongoWrites,
        closureSettings._1
      )
      jobs = MongoJobRepository.transactional(
        database,
        client,
        enqueuer,
        diagnostics,
        Some(discoveryPolicy),
        breakers.mongoWrites
      )
      applications = MongoApplicationRepository.transactional(database, client, diagnostics, breakers.mongoWrites)
      searchSessions = MongoSearchSessionRepository.transactional(database, client, diagnostics, breakers.mongoWrites)
      searchSessionWork = MongoSearchSessionWorkRepository.transactional(
        database,
        client,
        diagnostics,
        UUIDGen[IO],
        breakers.mongoWrites
      )
      outbox = MongoOperationalEventOutboxRepository.transactional(
        database,
        client,
        diagnostics,
        UUIDGen[IO],
        breakers.mongoWrites
      )
      receipts = new MongoConsumerReceiptRepository(database, diagnostics)
      mutationReceipts = MongoMutationReceiptRepository.transactional(
        database,
        client,
        diagnostics,
        UUIDGen[IO],
        breakers.mongoWrites
      )
      erasureRequests = MongoAnalyticsErasureRequestRepository.transactional(
        database,
        client,
        diagnostics,
        config.kafka.interview.topics,
        breakers.mongoWrites
      )
      analyticsReports =
        MongoAnalyticsReportRepository.transactional(database, client, diagnostics, breakers.mongoWrites)
      quarantine = new MongoEventQuarantineRepository(database, diagnostics)
      interviewRepository = MongoInterviewWorkflowRepository.live(
        database,
        client,
        diagnostics,
        config.kafka.interview.completedDedupRetentionSeconds.seconds,
        breakers.mongoWrites
      )
      // Fail closed: a lifetime the domain rejects stops startup instead of silently becoming the default.
      proposalTtl <- Resource.eval(
        IO.fromEither(
          InterviewProposalTtl
            .from(config.kafka.interview.proposalTtl)
            .leftMap(error => new IllegalStateException(s"Invalid interview proposal lifetime: $error"))
        )
      )
      // The refined config range is carried into the typed limit; an impossible value stops startup.
      facetLimit <- Resource.eval(
        IO.fromOption(LexicalMatchLimit.from(config.discovery.lexicalFacetMatchLimit))(
          new IllegalStateException("Invalid lexical facet match limit")
        )
      )
      handoffAlive <- Resource.eval(alive(ReadinessComponent.SearchSessionHandoff))
      searchSessionHandoff <- SearchSessionHandoff
        .resource[IO](
          searchSessionWork,
          SearchSessionHandoffConfig(),
          diagnostics,
          clock,
          alive = handoffAlive.beat
        )
      // One branch decides the embedding capability; everything below is shared by both modes.
      embedding <- config.vectorSearch match {
        case VectorSearchConfig.Disabled =>
          Resource.pure[
            IO,
            (
                EmbeddingWorkPublisher[IO],
                Option[SemanticSearchService[IO]],
                EmbeddingCoverageService[IO],
                Option[LexicalJobMatcher[IO]]
            )
          ](
            (disabledEmbeddingPublisher, None, EmbeddingCoverageService.vectorSearchDisabled(users), None)
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
            provider <- VoyageEmbeddingService.resource(
              voyage.apiKey,
              voyage.endpoint,
              voyage.model,
              voyage.dimension,
              embeddingSettings.timeoutMs.millis,
              diagnostics = diagnostics,
              breaker = breakers.voyage
            )
            publisher <- EmbeddingPipeline.resource[IO](
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
              diagnostics,
              new DurableRetrySettings(
                embeddingSettings.durableRetryAttempts,
                embeddingSettings.durableRetryBaseMillis.millis,
                embeddingSettings.durableRetryCapMillis.millis
              ),
              embeddingSettings.workerRestartDelayMillis.millis,
              embeddingHealth.set,
              clock
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
            publisher: EmbeddingWorkPublisher[IO],
            Some(SemanticSearchService(users, jobs, provider, search, voyage.model)),
            EmbeddingCoverageService.live(
              users,
              new MongoEmbeddingCoverageRepository(database, diagnostics),
              embeddingSettings.durableRetryCapMillis.millis,
              clock = clock
            ),
            Some(new MongoLexicalJobMatcher(database, indexes.lexical, diagnostics, discoveryPolicy))
          )
      }
      (publisher, semanticSearch, coverage, lexicalMatcher) = embedding
      idempotent = Idempotent(mutationReceipts, clock)
      accountService = UserAccountService(
        users,
        users,
        hasher,
        new JwtAccessTokenIssuer(config.jwtAuth),
        new HmacAuthenticationFingerprint(config.jwtAuth.receiptSecret),
        erasureRequests,
        publisher,
        idempotent,
        diagnostics,
        clock,
        UUIDGen[IO]
      )
      readModel = HiringReadService(users, jobs, applications)
      services = HiringGraphQLServices(
        readModel,
        new JobService(
          users,
          jobs,
          publisher,
          idempotent,
          diagnostics,
          clock,
          UUIDGen[IO],
          lexicalMatcher = lexicalMatcher,
          lexicalFacetMatchLimit = facetLimit,
          lexicalFacetDeadline = Some(config.discovery.maxTimeMillis.millis)
        ),
        new ApplicationService(users, jobs, applications, readModel, idempotent, clock, UUIDGen[IO]),
        CursorCodec.keyFromSecret(config.jwtAuth.hmacSecret, config.jwtAuth.cursorTtlSeconds),
        accountService,
        new OperationalTelemetryService(users, jobs, searchSessions, searchSessionWork, idempotent, clock),
        SearchSessionRecording(searchSessionHandoff, searchSessions)(using cats.Monad[IO], clock, UUIDGen[IO]),
        semanticSearch,
        AnalyticsReportingService(users, analyticsReports),
        Option.when(config.kafka.interview.enabled)(
          new InterviewSchedulingService[IO](using cats.Monad[IO], clock, UUIDGen[IO])(
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
        .resource(
          config.kafka,
          outbox,
          receipts,
          quarantine,
          diagnostics,
          clock,
          UUIDGen[IO],
          breakers.kafkaPublisher,
          alive
        )
      _ <- InterviewSchedulingRuntime.resource(
        config.kafka,
        interviewRepository,
        new MongoInterviewSubjectCleanup(database, diagnostics, config.kafka.interview.topics),
        diagnostics,
        alive
      )
      // Open jobs of a deleted recruiter beyond the first deletion page are closed here, one transaction per page.
      closure = MongoRecruiterJobClosure.transactional(
        database,
        client,
        diagnostics,
        closureSettings._1,
        breakers.mongoWrites,
        closureSettings._3
      )
      closureAlive <- Resource.eval(alive(ReadinessComponent.RecruiterJobClosure))
      _ <- new RecruiterJobClosureWorker[IO](closure, diagnostics, closureSettings._2, closureAlive.beat).resource
      degraded <- Resource.eval(
        DegradedComponents.create[IO](
          heartbeats,
          ReadinessThresholds(
            config.readiness.workerStalenessSeconds.seconds,
            config.readiness.maxOutboxAgeSeconds.seconds,
            config.readiness.maxClosureWorkAgeSeconds.seconds
          ),
          embeddingHealth.get,
          now => outbox.oldestDueAge(now),
          now => closure.oldestPendingAge(now),
          diagnostics
        )
      )
      metadata = MongoDatabaseProbe.connectionMetadata(config.mongoUri, config.mongoDatabase)
    } yield MongoHiringRuntime(
      probe(database, metadata, diagnostics, breakers.mongoWrites),
      services,
      UserAuthenticationService(users),
      embeddingHealth.get.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable),
      degraded.current
    )
  }

  /** The validated numbers become the typed values; an impossible combination stops startup. */
  private def recruiterJobClosureSettings(
      config: RecruiterJobClosureConfig
  ): IO[(JobClosurePageSize, RecruiterJobClosureSchedule, JobClosureRetry)] =
    IO.fromOption(
      (
        JobClosurePageSize.from(config.pageSize),
        RecruiterJobClosureSchedule.from(
          config.idlePollMillis.millis,
          config.failureBackoffInitialMillis.millis,
          config.failureBackoffMaxMillis.millis
        ),
        JobClosureRetry.from(config.rowRetryInitialMillis.millis, config.rowRetryMaxMillis.millis)
      ).tupled
    )(new IllegalStateException("Invalid recruiter job closure settings"))

  private def probe(
      database: MongoDatabase[IO],
      metadata: Map[LogField, String],
      diagnostics: Diagnostics[IO],
      mongoWrites: CircuitBreaker[IO]
  ): DatabaseProbe[IO] = new DatabaseProbe[IO] {
    private val delegate = MongoDatabaseProbe.fromDatabase(database, metadata, diagnostics)

    override def check(requestId: Option[String]): IO[ProbeResult] =
      // The ping goes straight to the database; only its result is combined with dependency health.
      delegate.check(requestId).flatMap(readiness(_, mongoWrites))
  }

  /** Only the ping and the Mongo-writes breaker decide the status; every other component is reported as degraded. Once
    * the open period ends the breaker admits again, so readiness recovers and traffic can probe it.
    */
  private[runtime] def readiness(ping: ProbeResult, mongoWrites: CircuitBreaker[IO]): IO[ProbeResult] =
    ping match {
      case ProbeResult.Ready =>
        mongoWrites.admits.map(writable => if (writable) ProbeResult.Ready else ProbeResult.Unavailable)
      case other => IO.pure(other)
    }

  private def setupEffect(database: MongoDatabase[IO], config: AppConfig, diagnostics: Diagnostics[IO]): IO[Unit] =
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
