package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{CursorCodec, HiringGraphQLServices}
import com.example.graphQL.cats.repository.protocol.EmbeddingService
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HiringReadService, LogField, ProbeResult}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.auth.{Argon2PasswordHasher, UserAccountService, UserAuthenticationService}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.protocol.{AccountUseCases, JobUseCases, SearchUseCases, UserAuthenticator}
import com.example.graphQL.cats.service.events.OperationalTelemetryService
import com.example.graphQL.cats.service.search.{EmbeddingPipeline, SemanticSearchService}
import com.example.graphQL.cats.config.{JwtAuthConfig, KafkaConfig, KafkaConsumerConfig, KafkaPublisherConfig, PasswordHashConfig, VectorSearchConfig}
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
import org.typelevel.otel4s.trace.Tracer

final case class MongoHiringRuntime(
    probe: DatabaseProbe,
    services: HiringGraphQLServices,
    userAuthenticator: UserAuthenticator[IO],
    hiringReadiness: IO[ProbeResult]
)

private[runtime] final class SetupLifecycle private (
    completion: Deferred[IO, Either[Throwable, Unit]]
) {
  def await: IO[Boolean] = completion.get.map(_.isRight)

  def ready: IO[Boolean] = completion.tryGet.map(_.exists(_.isRight))
}

private[runtime] object SetupLifecycle {
  def resource(setup: IO[Unit]): Resource[IO, SetupLifecycle] =
    Resource.eval(Deferred[IO, Either[Throwable, Unit]]).flatMap { completion =>
      Resource.make(
        setup.attempt.flatMap(completion.complete).start
      )(_.cancel).as(new SetupLifecycle(completion))
    }
}

object MongoHiringRuntime {
  def resource(uri: String, databaseName: String, diagnostics: Diagnostics,
      jwtAuth: JwtAuthConfig): Resource[IO, MongoHiringRuntime] =
    resource(uri, databaseName, diagnostics, disabledVectorSearch, voyageEmbeddingService, jwtAuth)

  def resource(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig,
      jwtAuth: JwtAuthConfig
  ): Resource[IO, MongoHiringRuntime] =
    resource(uri, databaseName, diagnostics, vectorSearch, voyageEmbeddingService, jwtAuth)

  def resource(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig,
      jwtAuth: JwtAuthConfig,
      resolverTimeout: FiniteDuration,
      passwordHash: PasswordHashConfig,
      kafka: KafkaConfig,
      tracer: Tracer[IO]
  ): Resource[IO, MongoHiringRuntime] =
    resource(uri, databaseName, diagnostics, vectorSearch, voyageEmbeddingService, jwtAuth,
      resolverTimeout, passwordHash, kafka, tracer)

  def resource(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig,
      embeddingService: (VectorSearchConfig, String) => Resource[IO, EmbeddingService[IO]],
      jwtAuth: JwtAuthConfig
  ): Resource[IO, MongoHiringRuntime] =
    resource(uri, databaseName, diagnostics, vectorSearch, embeddingService, jwtAuth, 4.seconds,
      tracer = Tracer.noop[IO])

  def resource(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig,
      embeddingService: (VectorSearchConfig, String) => Resource[IO, EmbeddingService[IO]],
      jwtAuth: JwtAuthConfig,
      resolverTimeout: FiniteDuration,
      passwordHash: PasswordHashConfig = defaultPasswordHash,
      kafka: KafkaConfig = disabledKafka,
      tracer: Tracer[IO]
  ): Resource[IO, MongoHiringRuntime] =
    MongoDatabaseProbe.clientResource(uri).flatMap { client =>
      val database = client.getDatabase(databaseName)
      val embeddingWork = Option.when(vectorSearch.enabled)(new MongoEmbeddingWorkRepository(database))
      val users = MongoUserRepository.transactional(database, client, embeddingWork)
      val jobs = MongoJobRepository.transactional(database, client, embeddingWork)
      val applications = MongoApplicationRepository.transactional(database, client)
      val searchSessions = MongoSearchSessionRepository.transactional(database, client)
      val outbox = new MongoOperationalEventOutboxRepository(database)
      val receipts = new MongoConsumerReceiptRepository(database)
      val quarantine = new MongoEventQuarantineRepository(database)
      Resource.eval(MongoHiringSetup.initializeCore(database, vectorSearch.enabled)) *>
      hiringServices(database, users, jobs, applications, searchSessions, vectorSearch, embeddingService, diagnostics, jwtAuth, passwordHash, tracer, resolverTimeout).flatMap { services =>
        SetupLifecycle.resource(setupEffect(database, vectorSearch)).flatMap { setup =>
          OperationalEventKafkaRuntime.resource(kafka, outbox, receipts, quarantine, diagnostics).as {
          val metadata = MongoDatabaseProbe.connectionMetadata(uri, databaseName)
          MongoHiringRuntime(
            probe(database, metadata, diagnostics, setup.ready),
            services,
            UserAuthenticationService[IO](users),
            setup.ready.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
          )
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
      embeddingService: (VectorSearchConfig, String) => Resource[IO, EmbeddingService[IO]],
      diagnostics: Diagnostics,
      jwtAuth: JwtAuthConfig,
      passwordHash: PasswordHashConfig,
      tracer: Tracer[IO],
      resolverTimeout: FiniteDuration
  ): Resource[IO, HiringGraphQLServices] =
    val hasher = new Argon2PasswordHasher(passwordHash.iterations, passwordHash.memoryKilobytes, passwordHash.parallelism)
    val tokenIssuer = new JwtAccessTokenIssuer(jwtAuth)
    val readModel = BoundedHiringServices.readModel(HiringReadService[IO](users, jobs, applications), resolverTimeout)
    val applicationService = BoundedHiringServices.applications(ApplicationService[IO](users, jobs, applications), resolverTimeout)
    val interactionService = OperationalTelemetryService[IO](users, jobs, searchSessions)
    val cursorCodec = CursorCodec.fromSecret(jwtAuth.hmacSecret)

    def assemble(
        jobService: JobUseCases[IO],
        accountService: AccountUseCases[IO],
        semanticSearch: Option[SearchUseCases[IO]] = None
    ): HiringGraphQLServices =
      HiringGraphQLServices(
        TracedHiringServices.readModel(readModel, diagnostics, tracer),
        TracedHiringServices.jobs(jobService, diagnostics, tracer),
        TracedHiringServices.applications(applicationService, diagnostics, tracer),
        cursorCodec,
        TracedHiringServices.accounts(accountService, diagnostics, tracer),
        semanticSearch.map(TracedHiringServices.search(_, diagnostics, tracer)),
        interactionService,
        searchSessions
      )

    if (!vectorSearch.enabled) {
      val account = BoundedHiringServices.accounts(UserAccountService(users, users, hasher, tokenIssuer), resolverTimeout)
      val jobService = BoundedHiringServices.jobs(JobService[IO](users, jobs), resolverTimeout)
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
            vectorSearch.embeddingVersion,
            vectorSearch.queueSize,
            vectorSearch.parallelism,
            vectorSearch.retryAttempts,
            vectorSearch.retryDelayMillis.millis,
            (vectorSearch.timeoutMillis + vectorSearch.retryDelayMillis).millis
          ).map { embeddingWork =>
            val jobService = BoundedHiringServices.jobs(JobService[IO](users, jobs, embeddingWork), resolverTimeout)
            val accountService = BoundedHiringServices.accounts(UserAccountService(users, users, hasher, tokenIssuer, embeddingWork), resolverTimeout)
            val semanticSearch = BoundedHiringServices.search(SemanticSearchService[IO](
              users,
              jobs,
              embeddings,
              search,
              vectorSearch.voyageModel,
              vectorSearch.embeddingVersion
            ), resolverTimeout)
            assemble(jobService, accountService, Some(semanticSearch))
          }
        }
      }
    }

  private def voyageEmbeddingService(config: VectorSearchConfig, apiKey: String): Resource[IO, EmbeddingService[IO]] =
    VoyageEmbeddingService.resource(
      apiKey,
      config.voyageEndpoint,
      config.voyageModel,
      config.voyageDimension,
      config.timeoutMillis.millis
    )

  private val disabledVectorSearch: VectorSearchConfig =
    VectorSearchConfig(
      enabled = false,
      voyageApiKey = None,
      voyageEndpoint = "https://api.voyageai.com/v1/embeddings",
      voyageModel = "voyage-4-lite",
      voyageDimension = 1024,
      embeddingVersion = 1,
      queueSize = 128,
      parallelism = 4,
      timeoutMillis = 5000,
      retryAttempts = 3,
      retryDelayMillis = 250,
      jobVectorIndex = "jobs_embedding_vector",
      candidateVectorIndex = "candidates_embedding_vector",
      jobLexicalIndex = "jobs_text_search",
      indexReadyTimeoutMillis = 120000,
      indexPollIntervalMillis = 1000,
      numCandidates = 100
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
      vectorSearch: VectorSearchConfig
  ): IO[Unit] =
    MongoHiringSetup.initialize(database, Option.when(vectorSearch.enabled)(AtlasSearchIndexConfig(
      vectorSearch.jobVectorIndex,
      vectorSearch.candidateVectorIndex,
      vectorSearch.jobLexicalIndex,
      vectorSearch.voyageDimension,
      vectorSearch.indexReadyTimeoutMillis,
      vectorSearch.indexPollIntervalMillis
    )), vectorSearch.enabled)

  private val defaultPasswordHash = PasswordHashConfig(iterations = 2, memoryKilobytes = 19456, parallelism = 1)

  private val disabledKafka: KafkaConfig =
    KafkaConfig(
      enabled = false,
      bootstrapServers = "127.0.0.1:9092",
      topic = "hiring.operational-events.v1",
      consumerGroup = "hiring-phase5-consumer",
      KafkaPublisherConfig("local-publisher", batchSize = 25, leaseSeconds = 30, retryDelaySeconds = 5, maxAttempts = 10, pollIntervalMillis = 500),
      KafkaConsumerConfig(enabled = false, receiptTtlDays = 8, quarantineTtlDays = 7)
    )
}
