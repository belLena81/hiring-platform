package com.example.graphQL.cats.runtime

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.example.graphQL.cats.transport.graphql.HiringGraphQLServices
import com.example.graphQL.cats.repository.protocol.EmbeddingService
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HiringReadService, LogField, ProbeResult}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.auth.UserAuthenticationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import com.example.graphQL.cats.service.search.{EmbeddingPipeline, SemanticSearchService}
import com.example.graphQL.cats.config.VectorSearchConfig
import com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService
import com.example.graphQL.cats.repository.mongo.{
  MongoApplicationRepository, MongoDatabaseProbe, MongoHiringSetup, MongoJobRepository, MongoSemanticSearchRepository,
  MongoUserRepository
}
import com.mongodb.reactivestreams.client.MongoDatabase

final case class MongoHiringRuntime(
    probe: DatabaseProbe,
    services: HiringGraphQLServices,
    userAuthenticator: UserAuthenticator[IO],
    ensureSetup: IO[Boolean]
)

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
      embeddingService: (VectorSearchConfig, String) => EmbeddingService[IO]
  ): Resource[IO, MongoHiringRuntime] =
    MongoDatabaseProbe.clientResource(uri).flatMap { client =>
      val database = client.getDatabase(databaseName)
      Resource.eval((Ref.of[IO, Boolean](false), Semaphore[IO](1)).tupled).flatMap { case (setupComplete, setupLock) =>
        val users = new MongoUserRepository(database)
        val jobs = new MongoJobRepository(database)
        val applications = MongoApplicationRepository.transactional(database, client)
        hiringServices(database, users, jobs, applications, vectorSearch, embeddingService).map { services =>
          val setup = ensureSetup(database, setupComplete, setupLock)
          val metadata = MongoDatabaseProbe.connectionMetadata(uri, databaseName)
          MongoHiringRuntime(probe(database, metadata, diagnostics, setup), services, UserAuthenticationService[IO](users), setup)
        }
      }
    }

  private def hiringServices(
      database: MongoDatabase,
      users: MongoUserRepository,
      jobs: MongoJobRepository,
      applications: MongoApplicationRepository,
      vectorSearch: VectorSearchConfig,
      embeddingService: (VectorSearchConfig, String) => EmbeddingService[IO]
  ): Resource[IO, HiringGraphQLServices] =
    if (!vectorSearch.enabled) {
      val readModel = HiringReadService[IO](users, jobs, applications)
      Resource.pure(HiringGraphQLServices(
        readModel,
        JobService[IO](users, jobs),
        ApplicationService[IO](users, jobs, applications)
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
            readModel,
            jobService,
            applicationService,
            Some(semanticSearch)
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
      numCandidates = 100
    )

  private def probe(
      database: MongoDatabase,
      metadata: Map[LogField, String],
      diagnostics: Diagnostics,
      setup: IO[Boolean]
  ): DatabaseProbe = new DatabaseProbe {
    private val delegate = MongoDatabaseProbe.fromDatabase(database, metadata, diagnostics)

    override def check: IO[ProbeResult] =
      check(None)

    override def check(requestId: Option[String]): IO[ProbeResult] =
      delegate.check(requestId).flatMap {
        case ProbeResult.Ready => setup.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable)
        case other => IO.pure(other)
      }
  }

  private def ensureSetup(
      database: com.mongodb.reactivestreams.client.MongoDatabase,
      done: Ref[IO, Boolean],
      lock: Semaphore[IO]
  ): IO[Boolean] =
    done.get.flatMap {
      case true => IO.pure(true)
      case false =>
        lock.permit.use { _ =>
          done.get.flatMap {
            case true => IO.pure(true)
            case false => MongoHiringSetup.initialize(database).attempt.flatMap {
              case Right(()) => done.set(true).as(true)
              case Left(_) => IO.pure(false)
            }
          }
        }
    }
}
