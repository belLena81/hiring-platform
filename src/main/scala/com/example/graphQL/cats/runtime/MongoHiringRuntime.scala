package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices}
import com.example.graphQL.cats.repository.protocol.EmbeddingService
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HiringReadService, LogEvent, LogField, LogFields, ProbeResult}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.auth.{Argon2PasswordHasher, UserAccountService, UserAuthenticationService}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.protocol.{AccountUseCases, JobUseCases, SearchUseCases, UserAuthenticator}
import com.example.graphQL.cats.service.events.OperationalTelemetryService
import com.example.graphQL.cats.service.search.{EmbeddingPipeline, SemanticSearchService}
import com.example.graphQL.cats.config.{JwtAuthConfig, KafkaConfig, PasswordHashConfig, VectorSearchConfig}
import com.example.graphQL.cats.infrastructure.auth.JwtAccessTokenIssuer
import com.example.graphQL.cats.infrastructure.kafka.OperationalEventKafkaRuntime
import scala.concurrent.duration.*
import com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService
import com.example.graphQL.cats.repository.mongo.{
  MongoApplicationRepository, MongoConsumerReceiptRepository, MongoDatabaseProbe, MongoEventQuarantineRepository, MongoHiringSetup, MongoJobRepository,
  MongoOperationalEventOutboxRepository, MongoSearchSessionRepository, MongoSemanticSearchRepository,
  MongoUserRepository, MongoEmbeddingWorkRepository, AtlasSearchIndexConfig
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
      Resource.make(
        setup.attempt.flatTap(_.fold(
          error => diagnostics.emit(LogEvent.MongoSetupFailed, fields = LogFields.failure(error)),
          _ => IO.unit
        )).flatMap(completion.complete).start
      )(_.cancel).as(new SetupLifecycle(completion))
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
    Resource.eval(Semaphore[IO](Runtime.getRuntime.availableProcessors.toLong)).flatMap { passwordHashPermits =>
      MongoDatabaseProbe.clientResource(config.uri).flatMap { client =>
      val database = client.getDatabase(config.databaseName)
      val embeddingWork = Option.when(config.vectorSearch.enabled)(new MongoEmbeddingWorkRepository(database))
      val users = MongoUserRepository.transactional(database, client, embeddingWork)
      val jobs = MongoJobRepository.transactional(database, client, embeddingWork)
      val applications = MongoApplicationRepository.transactional(database, client)
      val searchSessions = MongoSearchSessionRepository.transactional(database, client)
      val outbox = new MongoOperationalEventOutboxRepository(database)
      val receipts = new MongoConsumerReceiptRepository(database)
      val quarantine = new MongoEventQuarantineRepository(database)
      hiringServices(database, users, jobs, applications, searchSessions, config.vectorSearch, config.embeddingService, config.jwtAuth, config.passwordHash, passwordHashPermits).flatMap { services =>
        SetupLifecycle.resource(setupEffect(database, config.vectorSearch, config.resetOnStart), config.diagnostics).flatMap { setup =>
          OperationalEventKafkaRuntime.resource(config.kafka, outbox, receipts, quarantine, config.diagnostics).as {
          val metadata = MongoDatabaseProbe.connectionMetadata(config.uri, config.databaseName)
          MongoHiringRuntime(
            probe(database, metadata, config.diagnostics, setup.ready),
            services,
            UserAuthenticationService(users),
            setup.ready.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
          )
          }
        }
      }
      }
    }

  private def hiringServices(
      database: MongoDatabase,
      users: MongoUserRepository,
      jobs: MongoJobRepository,
      applications: MongoApplicationRepository,
      searchSessions: MongoSearchSessionRepository,
      vectorSearch: VectorSearchConfig,
      embeddingService: (VectorSearchConfig, String) => Resource[IO, EmbeddingService],
      jwtAuth: JwtAuthConfig,
      passwordHash: PasswordHashConfig,
      passwordHashPermits: Semaphore[IO]
  ): Resource[IO, HiringGraphQLServices] =
    val hasher = new Argon2PasswordHasher(
      passwordHash.iterations,
      passwordHash.memoryKilobytes,
      passwordHash.parallelism,
      passwordHashPermits
    )
    val tokenIssuer = new JwtAccessTokenIssuer(jwtAuth)
    val readModel = HiringReadService(users, jobs, applications)
    val applicationService = ApplicationService(users, jobs, applications)
    val interactionService = OperationalTelemetryService(users, jobs, searchSessions)
    val cursorKey = CursorCodec.keyFromSecret(jwtAuth.hmacSecret)

    def assemble(
        jobService: JobUseCases,
        accountService: AccountUseCases,
        semanticSearch: Option[SearchUseCases] = None
    ): HiringGraphQLServices =
      HiringGraphQLServices(
        readModel,
        jobService,
        applicationService,
        cursorKey,
        accountService,
        semanticSearch,
        Some(interactionService),
        searchSessions
      )

    if (!vectorSearch.enabled) {
      val account = UserAccountService(users, users, hasher, tokenIssuer)
      val jobService = JobService(users, jobs)
      Resource.pure(assemble(jobService, account))
    } else {
      Resource.eval(IO.fromOption(vectorSearch.voyageApiKey)(
        new IllegalArgumentException("VOYAGE_API_KEY is required when vector search is enabled")
      )).flatMap { apiKey =>
        val search = new MongoSemanticSearchRepository(
          database,
          vectorSearch.jobVectorIndex,
          vectorSearch.candidateVectorIndex,
          vectorSearch.jobLexicalIndex,
          vectorSearch.numCandidates
        )
        embeddingService(vectorSearch, apiKey).flatMap { embeddings =>
          EmbeddingPipeline.resource(
            new MongoEmbeddingWorkRepository(database),
            users,
            jobs,
            embeddings,
            vectorSearch.voyageModel,
            vectorSearch.queueSize,
            vectorSearch.parallelism,
            vectorSearch.retryAttempts,
            vectorSearch.retryDelayMillis.millis,
            (vectorSearch.timeoutMillis + vectorSearch.retryDelayMillis).millis
          ).map { embeddingWork =>
            val jobService = JobService(users, jobs, embeddingWork)
            val accountService = UserAccountService(users, users, hasher, tokenIssuer, embeddingWork)
            val semanticSearch = SemanticSearchService(
              users,
              jobs,
              embeddings,
              search,
              vectorSearch.voyageModel
            )
            assemble(jobService, accountService, Some(semanticSearch))
          }
        }
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
        case other => IO.pure(other)
      }
  }

  private def setupEffect(
      database: com.mongodb.reactivestreams.client.MongoDatabase,
      vectorSearch: VectorSearchConfig,
      resetOnStart: Boolean
  ): IO[Unit] =
    MongoHiringSetup.initialize(database, Option.when(vectorSearch.enabled)(AtlasSearchIndexConfig(
      vectorSearch.jobVectorIndex,
      vectorSearch.candidateVectorIndex,
      vectorSearch.jobLexicalIndex,
      vectorSearch.voyageDimension,
      vectorSearch.indexReadyTimeoutMillis,
      vectorSearch.indexPollIntervalMillis
    )), resetOnStart)

}
