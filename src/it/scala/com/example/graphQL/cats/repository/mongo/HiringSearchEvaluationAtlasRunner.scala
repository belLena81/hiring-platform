package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{Job, User}
import com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.EmbeddingInputType
import com.example.graphQL.cats.service.search.*
import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import com.example.graphQL.cats.infrastructure.search.{SearchEvaluationArtifacts, SearchEvaluationReportJson}
import com.example.graphQL.cats.shared.crypto.SourceHash
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.control.NoStackTrace

/** Explicit opt-in only. Owns a fresh database. It uses synthetic embeddings and never calls a provider, unless the
  * owner also passes `--authorize-provider-embeddings` (with the required budget and allowance options, the provider
  * key and the data-collection attestation in configuration): then the 16 corpus entities and the 12 query texts are
  * embedded through the Voyage adapter. Only the fabricated corpus is sent. It is started by the owner, never by tests.
  */
object HiringSearchEvaluationAtlasRunner extends IOApp {
  private[mongo] val indexes = AtlasSearchIndexConfig(
    "curated_jobs_vector",
    "curated_candidates_vector",
    "curated_jobs_lexical",
    "curated_candidates_lexical",
    HiringSearchEvaluationCorpus.Dimensions,
    120000,
    500
  )
  private[mongo] final case class Settings(
      output: Path,
      revision: String,
      concurrency: Int,
      provider: Option[ProviderEmbeddingOptions] = None
  )

  /** A provider-capture stop whose fixed message is safe to print (no key, text or vector). */
  private final case class ProviderCaptureStopped(reason: String) extends RuntimeException(reason) with NoStackTrace

  private[mongo] def parse(args: List[String]): Either[String, Settings] =
    ProviderEmbeddingOptions.extract(args).leftMap(_.message).flatMap { case (provider, rest) =>
      parseBase(rest).map(_.copy(provider = provider))
    }

  private def parseBase(args: List[String]): Either[String, Settings] = args match {
    case List(
          "--authorize-disposable-atlas",
          "--output",
          output,
          "--source-revision",
          revision,
          "--concurrency",
          count
        ) =>
      for {
        concurrency <- count.toIntOption.filter(Set(1, 8)).toRight("Concurrency must be 1 or 8")
        _ <- Either.cond(revision.trim.nonEmpty, (), "Source revision is required")
        directory <- Either.catchNonFatal(Path.of(output).normalize()).leftMap(_ => "Invalid output path")
        _ <- Either.cond(directory.startsWith(Path.of(".local/data")), (), "Output must be beneath .local/data")
      } yield Settings(directory, revision, concurrency)
    case _ =>
      Left(
        "Use --authorize-disposable-atlas --output .local/data/search-evaluation --source-revision REV " +
          "--concurrency 1|8 " +
          "[--authorize-provider-embeddings --absolute-p95-budget-ms N --latency-noise-allowance-ms N " +
          "[--embedding-pause-ms N]]"
      )
  }
  private[mongo] def ownedDatabaseName(nonce: UUID): String =
    s"search_evaluation_curated_${nonce.toString.replace("-", "")}"

  override def run(args: List[String]): IO[ExitCode] = parse(args) match {
    case Left(message)   => IO.println(message).as(ExitCode.Error)
    case Right(settings) =>
      com.example.hiring.testing.DisposableAtlas.authorizedUri(sys.env) match {
        case Left(message) => IO.println(message).as(ExitCode.Error)
        case Right(uri)    =>
          settings.provider match {
            case None          => execute(uri, settings, None)
            case Some(options) =>
              // Authorization, configuration and every provider call happen before any database is created. Any
              // raised error becomes one fixed message: no stack trace and no exception text is printed.
              providerBranch(uri, settings, options).attempt.flatMap {
                case Right(code) => IO.pure(code)
                case Left(_)     => IO.println(ProviderEmbeddingRefusal.UnexpectedFailure.message).as(ExitCode.Error)
              }
          }
      }
  }

  private final case class ProviderRun(
      config: ProviderEmbeddingConfig,
      options: ProviderEmbeddingOptions,
      result: ProviderEmbeddingResult
  )

  private def providerBranch(uri: String, settings: Settings, options: ProviderEmbeddingOptions): IO[ExitCode] =
    embedCorpus(options).flatMap {
      case Right(provider) =>
        // The completed record is written before any database exists, so every later stop keeps the billed tokens.
        IO.realTimeInstant
          .flatMap { timestamp =>
            ProviderEmbeddingEvidence.recordThen(
              ProviderEmbeddingEvidence.writeCompleted(
                settings.output,
                settings.concurrency,
                timestamp,
                provider.config,
                options,
                provider.result.usage,
                captureBound,
                SearchEvaluationFixtures.K
              ),
              execute(uri, settings, Some(provider))
            )
          }
          .flatMap {
            case Right(code)   => IO.pure(code)
            case Left(refusal) => IO.println(refusal.message).as(ExitCode.Error)
          }
      case Left(failure) => reportFailure(settings, options, failure)
    }

  /** A stop after at least one provider request keeps a counts-only record, so the tokens already billed are not lost
    * (a fully answered run is recorded by `providerBranch` before any Atlas work; this method covers the embedding
    * stage). No database exists at this point. The record holds no key, vector or input text.
    */
  private def reportFailure(
      settings: Settings,
      options: ProviderEmbeddingOptions,
      failure: ProviderEmbeddingFailure
  ): IO[ExitCode] =
    (failure.config match {
      case Some(config) if failure.requestsMade > 0 =>
        IO.realTimeInstant
          .flatMap(timestamp =>
            ProviderEmbeddingEvidence.writeFailure(
              settings.output,
              settings.concurrency,
              timestamp,
              config,
              options,
              failure,
              captureBound,
              SearchEvaluationFixtures.K
            )
          )
          .attempt
          .map(written =>
            if (written.isRight) " A counts-only failure record was written."
            else " The failure record could not be written."
          )
      case _ => IO.pure("")
    }).flatMap(note => IO.println(failure.refusal.message + note)).as(ExitCode.Error)

  /** Local overlay: ONE explicit file relative to the working directory (never every classpath `local.conf`). */
  private val LocalOverlay = new File("src/main/resources/local.conf")

  /** Read inside `IO` when the owner-started provider branch runs. A parse or resolve error is a typed failure with a
    * fixed message; the error text is dropped.
    */
  private def providerConfiguration: IO[Either[ProviderEmbeddingRefusal, ProviderEmbeddingConfig]] =
    IO.blocking(
      Try(
        ConfigFactory
          .parseFile(LocalOverlay, ConfigParseOptions.defaults.setAllowMissing(true))
          .withFallback(ConfigFactory.load())
          .resolve()
      ).toEither
    ).map(_.leftMap(_ => ProviderEmbeddingRefusal.InvalidConfiguration).flatMap(ProviderEmbeddingConfig.read))

  private def embedCorpus(options: ProviderEmbeddingOptions): IO[Either[ProviderEmbeddingFailure, ProviderRun]] =
    ProviderEmbeddingCapture
      .embedAll(
        Some(options),
        providerConfiguration,
        config =>
          VoyageEmbeddingService.meteredResource(
            config.apiKey,
            config.endpoint,
            config.model,
            config.dimension,
            config.timeout,
            diagnostics = Diagnostics.noop[IO]
          ),
        HiringSearchEvaluationCorpus.documentTexts.map((id, text) =>
          ProviderEmbeddingRequest(id, text, EmbeddingInputType.Document)
        ) ++ SearchEvaluationFixtures.corpus.queries.map(query =>
          ProviderEmbeddingRequest(query.queryId, HiringSearchEvaluationCorpus.text(query), EmbeddingInputType.Query)
        )
      )
      .map(_.map { case (config, result) => ProviderRun(config, options, result) })

  private def execute(uri: String, settings: Settings, provider: Option[ProviderRun]): IO[ExitCode] =
    MongoDatabaseProbe
      .clientResource(uri)
      .use { client =>
        Resource
          .make(
            IO(UUID.randomUUID()).flatMap(nonce => client.getDatabase(ownedDatabaseName(nonce)))
          )(db => MongoAccessEvaluationSupport.command(db, new Document("dropDatabase", 1)).attempt.void)
          .use(database =>
            evaluate(database, settings, com.example.hiring.testing.DisposableAtlas.deployment(uri), provider)
          )
      }
      .attempt
      .flatMap {
        case Right(_) =>
          IO.println(
            "Curated execution capture written; semantic quality and privacy acceptance remain unobserved."
          ).as(ExitCode.Success)
        case Left(stopped: ProviderCaptureStopped) => IO.println(stopped.reason).as(ExitCode.Error)
        case Left(_)                               =>
          IO.println("Curated Atlas evaluation failed; connection diagnostics were suppressed.")
            .as(ExitCode.Error)
      }

  /** The bound the capture is taken with: the K of the report. The retrieval page size derives from this same value, so
    * the truncation guard and the page cannot drift apart.
    */
  private[mongo] val captureBound: Int = SearchEvaluationFixtures.K

  private def seed(
      database: MongoDatabase[IO],
      corpusJobs: List[Job],
      corpusCandidates: List[User],
      indexes: AtlasSearchIndexConfig
  ): IO[Unit] = for {
    _ <- database.createCollection(MongoCollections.Jobs)
    _ <- database.createCollection(MongoCollections.Users)
    jobs <- Mongo4catsCollections.documents(database, MongoCollections.Jobs)
    users <- Mongo4catsCollections.documents(database, MongoCollections.Users)
    // Physically absent job 7 is the deleted-job fixture; stale job 8 carries an obsolete hash.
    _ <- jobs.insertMany(
      corpusJobs
        .filterNot(_.id.value.toString == SearchEvaluationFixtures.jobId(7))
        .map(HiringSearchEvaluationCorpus.jobDocument)
    )
    _ <- users.insertMany(
      corpusCandidates.map(HiringSearchEvaluationCorpus.candidateDocument) :+
        MongoHiringCodecs.user(HiringSearchEvaluationCorpus.recruiter)
    )
    _ <- MongoAtlasSearchSetup.provision(MongoHiringSetup.SetupDatabase(database, Map.empty), indexes)
  } yield ()

  private def evaluate(
      database: MongoDatabase[IO],
      settings: Settings,
      deployment: String,
      provider: Option[ProviderRun]
  ): IO[Unit] = for {
    model <- IO.pure(provider.fold(HiringSearchEvaluationCorpus.Model)(_.config.model))
    dimension = provider.fold(HiringSearchEvaluationCorpus.Dimensions)(_.config.dimension)
    runIndexes = indexes.copy(dimension = dimension)
    embedding = provider.fold(RetrievalEmbeddings.synthetic) { run =>
      RetrievalEmbeddings(model, dimension, query => run.result.vectors((EmbeddingInputType.Query, query.queryId)))
    }
    _ <- seed(
      database,
      provider.fold(HiringSearchEvaluationCorpus.jobs) { run =>
        HiringSearchEvaluationCorpus.jobsWith(
          model,
          entity => run.result.vectors((EmbeddingInputType.Document, entity.id))
        )
      },
      provider.fold(HiringSearchEvaluationCorpus.candidates) { run =>
        HiringSearchEvaluationCorpus.candidatesWith(
          model,
          entity => run.result.vectors((EmbeddingInputType.Document, entity.id))
        )
      },
      runIndexes
    )
    fingerprint <- SearchEvaluationArtifacts.sourceFingerprint
    timestamp <- IO.realTimeInstant
    version <- MongoAccessEvaluationSupport
      .command(database, new Document("buildInfo", 1))
      .map(value =>
        Option(value.getString("version"))
          .filter(text => text.length <= 128 && text.matches("[0-9]+(\\.[0-9]+){1,3}[-a-zA-Z0-9.]*"))
      )
      .handleError(_ => None)
    definitions <- List(MongoCollections.Jobs, MongoCollections.Users).traverse { collection =>
      MongoAtlasSearchAdmin
        .listIndexes(database, collection, 4)
        .map(_.sortBy(_.getString("name")).map { index =>
          val definition = Option(index.get("latestDefinition", classOf[Document]))
            .orElse(Option(index.get("definition", classOf[Document])))
          s"$collection:${index.getString("name")}:${definition.fold("unavailable")(_.toJson)}"
        })
    }
    indexIdentity = SourceHash.sha256(definitions.flatten.mkString("\n"))
    environment = SearchEvaluationFixtures.environment.copy(
      identity = s"curated-disposable-$deployment-${database.underlying.getName}",
      atlasVersion = version,
      telemetryUnavailable = ((SearchEvaluationFixtures.environment.telemetryUnavailable -
        SearchEvaluationTelemetry.Latency - SearchEvaluationTelemetry.AtlasVersion)
        .map { case (field, _) => field -> SearchEvaluationTelemetryReason.NotCaptured }) ++
        Option.when(version.isEmpty)(
          SearchEvaluationTelemetry.AtlasVersion -> SearchEvaluationTelemetryReason.NotCaptured
        )
    )
    reports <- List(
      SearchEvaluationStrategy.Lexical,
      SearchEvaluationStrategy.Vector,
      SearchEvaluationStrategy.ApplicationRrf
    ).traverse { strategy =>
      for {
        started <- IO.monotonic
        captured <- SearchEvaluationCapture.capture(
          SearchEvaluationFixtures.corpus,
          strategy,
          settings.concurrency,
          30.seconds,
          Resource
            .eval(DiscoveryQueryPolicy.create(2.seconds, 4))
            .map[SearchEvaluationCapture.Retrieval](policy =>
              new HiringSearchEvaluationRetrieval(database, policy, embedding, runIndexes)
            ),
          maximumResults = captureBound
        )
        ended <- IO.monotonic
        observations <- IO.fromEither(captured.leftMap(_ => new IllegalStateException("Invalid capture workload")))
        run = SearchEvaluationFixtures
          .emptyRun(strategy, settings.revision, fingerprint, timestamp)
          .copy(
            coordinates = SearchEvaluationFixtures
              .emptyRun(strategy, settings.revision, fingerprint, timestamp)
              .coordinates
              .copy(
                rankingOrigin = SearchEvaluationRankingOrigin.ObservedAtlas,
                embeddingModel = model,
                embeddingDimensions = dimension,
                embeddingProvenance = provider.fold(SearchEvaluationEmbeddingProvenance.SyntheticFixture)(_ =>
                  SearchEvaluationEmbeddingProvenance.ProviderGenerated
                ),
                indexIdentity = indexIdentity
              ),
            concurrency = settings.concurrency,
            durationMillis = Some((ended - started).toMillis.max(1L)),
            environment = environment
              .copy(providerRequests = provider.map(_.result.usage.size.toLong).orElse(environment.providerRequests)),
            queries = observations
          )
        // Truncation guard: the capture declares the K it was taken with, never a larger bound. A list that reaches
        // that bound could hide a hit the ranker would promote, so a provider capture stops there.
        _ <-
          if (provider.isEmpty || strategy == SearchEvaluationStrategy.Lexical) IO.unit
          else
            IO.raiseWhen(
              SkillEvidenceRankedEvaluation
                .truncationErrors(run, SearchEvaluationFixtures.queries, captureBound)
                .nonEmpty
            )(ProviderCaptureStopped("Possibly truncated capture; no report was written."))
        report <- IO.fromEither(
          SearchEvaluationHarness
            .report(SearchEvaluationFixtures.corpus, run, SearchEvaluationFixtures.K)
            .leftMap(_ => new IllegalStateException("Invalid curated capture report"))
        )
      } yield report
    }
    baseline <- IO.fromOption(reports.find(_.run.strategy == SearchEvaluationStrategy.Vector))(
      new IllegalStateException("Missing vector baseline")
    )
    policy = SearchEvaluationAssessmentPolicy(
      "conservative-no-regression-20261006",
      SearchEvaluationPolicyAgreement.Agreed,
      SearchEvaluationUseCase.values.toList.map(_ -> SearchEvaluationUseCasePolicy()).toMap,
      latencyNoiseAllowanceMillis = provider.fold(0.0)(_.options.latencyNoiseAllowanceMillis),
      absoluteP95Budget = provider.map(run => java.time.Duration.ofMillis(run.options.absoluteP95Budget.toMillis))
    )
    _ <- reports.traverse_ { report =>
      for {
        assessment <- IO.fromEither(
          SearchEvaluationAssessment
            .assess(baseline, report, policy, None, None, None)
            .leftMap(_ => new IllegalStateException("Invalid curated paired assessment"))
        )
        _ <- IO.blocking {
          Files.createDirectories(settings.output)
          Files.writeString(
            settings.output.resolve(
              s"curated-${report.run.strategy.toString}-${settings.concurrency}-${timestamp.toEpochMilli}.json"
            ),
            SearchEvaluationReportJson.render(report.copy(assessment = Some(assessment))).spaces2,
            StandardCharsets.UTF_8
          )
          ()
        }
      } yield ()
    }
    _ <- IO.blocking {
      val jobs = SearchEvaluationFixtures.jobs
      val candidates = SearchEvaluationFixtures.candidates
      val tags = Map(
        "closedJob" -> jobs.count(value => !value.active && !value.deleted),
        "deletedJob" -> jobs.count(_.deleted),
        "staleSourceJob" -> jobs.count(_.stale),
        "inactiveCandidate" -> candidates.count(value => !value.active && !value.deleted),
        "deletedCandidate" -> candidates.count(_.deleted),
        "staleSourceCandidate" -> candidates.count(_.stale),
        "consentOptedIn" -> candidates.count(_.optIn.contains(true)),
        "consentOptedOut" -> candidates.count(_.optIn.contains(false)),
        "consentAbsent" -> candidates.count(_.optIn.isEmpty)
      )
      val record = AtlasLocalCriterionEvidence.curatedRecord(
        deployment,
        version,
        settings.revision,
        settings.concurrency,
        reports.map(report =>
          s"curated-${report.run.strategy.toString}-${settings.concurrency}-${timestamp.toEpochMilli}.json"
        ),
        tags
      )
      Files.createDirectories(settings.output)
      Files.writeString(
        settings.output.resolve(s"criterion-evidence-curated-${settings.concurrency}-${timestamp.toEpochMilli}.json"),
        record.spaces2,
        StandardCharsets.UTF_8
      )
      ()
    }
  } yield ()
}
