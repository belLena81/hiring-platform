package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLServices
import com.example.graphQL.cats.repository.protocol.EmbeddingService
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HiringReadService, LogField, ProbeResult}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.auth.{Argon2PasswordHasher, UserAccountService, UserAuthenticationService}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import com.example.graphQL.cats.service.search.{EmbeddingPipeline, SemanticSearchService}
import com.example.graphQL.cats.config.VectorSearchConfig
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService
import com.example.graphQL.cats.repository.mongo.{
  MongoApplicationRepository, MongoDatabaseProbe, MongoHiringSetup, MongoJobRepository, MongoSemanticSearchRepository,
  MongoUserRepository, AtlasSearchIndexConfig
}
import com.mongodb.reactivestreams.client.MongoDatabase

final case class MongoHiringRuntime(
    probe: DatabaseProbe,
    services: HiringGraphQLServices,
    userAuthenticator: UserAuthenticator[IO],
    ensureSetup: IO[Boolean]
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
  def resource(uri: String, databaseName: String, diagnostics: Diagnostics): Resource[IO, MongoHiringRuntime] =
    resource(uri, databaseName, diagnostics, disabledVectorSearch)

  def resource(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig
  ): Resource[IO, MongoHiringRuntime] =
    resource(uri, databaseName, diagnostics, vectorSearch, voyageEmbeddingService)

  def resource(
      uri: String,
      databaseName: String,
      diagnostics: Diagnostics,
      vectorSearch: VectorSearchConfig,
      embeddingService: (VectorSearchConfig, String) => EmbeddingService[IO],
      jwtAuth: JwtAuthConfig = JwtAuthConfig(None, "hiring-platform-local", "hiring-graphql-api")
  ): Resource[IO, MongoHiringRuntime] =
    MongoDatabaseProbe.clientResource(uri).flatMap { client =>
      val database = client.getDatabase(databaseName)
      val users = MongoUserRepository.transactional(database, client)
      val jobs = new MongoJobRepository(database)
      val applications = MongoApplicationRepository.transactional(database, client)
      hiringServices(database, users, jobs, applications, vectorSearch, embeddingService, diagnostics, jwtAuth).flatMap { services =>
        SetupLifecycle.resource(setupEffect(database, vectorSearch)).map { setup =>
          val metadata = MongoDatabaseProbe.connectionMetadata(uri, databaseName)
          MongoHiringRuntime(
            probe(database, metadata, diagnostics, setup.ready),
            services,
            UserAuthenticationService[IO](users),
            setup.await
          )
        }
      }
    }

  private def hiringServices(
      database: MongoDatabase,
      users: MongoUserRepository,
      jobs: MongoJobRepository,
      applications: MongoApplicationRepository,
      vectorSearch: VectorSearchConfig,
      embeddingService: (VectorSearchConfig, String) => EmbeddingService[IO],
      diagnostics: Diagnostics,
      jwtAuth: JwtAuthConfig
  ): Resource[IO, HiringGraphQLServices] =
    if (!vectorSearch.enabled) {
      val readModel = HiringReadService[IO](users, jobs, applications)
      val account = UserAccountService(users, users, Argon2PasswordHasher(), jwtAuth)
      Resource.pure(HiringGraphQLServices(
        TracedHiringServices.readModel(readModel, diagnostics),
        TracedHiringServices.jobs(JobService[IO](users, jobs), diagnostics),
        TracedHiringServices.applications(ApplicationService[IO](users, jobs, applications), diagnostics)
        , accountService = Some(account)
      ))
    } else {
      Resource.eval(IO.fromOption(vectorSearch.voyageApiKey)(
        new IllegalArgumentException("VOYAGE_API_KEY is required when vector search is enabled")
      )).flatMap { apiKey =>
        val embeddings = embeddingService(vectorSearch, apiKey)
        val search = new MongoSemanticSearchRepository(
          database,
          vectorSearch.jobVectorIndex,
          vectorSearch.candidateVectorIndex,
          vectorSearch.jobLexicalIndex,
          vectorSearch.numCandidates
        )
        EmbeddingPipeline.resource(
          users,
          jobs,
          embeddings,
          vectorSearch.voyageModel,
          vectorSearch.embeddingVersion,
          vectorSearch.queueSize,
          vectorSearch.parallelism
        ).map { queue =>
          val jobService = JobService[IO](users, jobs, queue)
          val applicationService = ApplicationService[IO](users, jobs, applications)
          val readModel = HiringReadService[IO](users, jobs, applications)
          val semanticSearch = SemanticSearchService[IO](
            users,
            jobs,
            embeddings,
            search,
            vectorSearch.voyageModel,
            vectorSearch.embeddingVersion
          )
          val services = HiringGraphQLServices(
            TracedHiringServices.readModel(readModel, diagnostics),
            TracedHiringServices.jobs(jobService, diagnostics),
            TracedHiringServices.applications(applicationService, diagnostics),
            Some(TracedHiringServices.search(semanticSearch, diagnostics)),
            Some(UserAccountService(users, users, Argon2PasswordHasher(), jwtAuth))
          )
          services
        }
      }
    }

  private def voyageEmbeddingService(config: VectorSearchConfig, apiKey: String): EmbeddingService[IO] =
    new VoyageEmbeddingService(
      apiKey,
      config.voyageEndpoint,
      config.voyageModel,
      config.voyageDimension,
      config.timeoutMillis
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
    )))
}
