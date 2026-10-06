package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.infrastructure.search.{SearchEvaluationArtifacts, SearchEvaluationReportJson}
import com.example.graphQL.cats.shared.crypto.SourceHash
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.duration.*

/** Explicit opt-in only. Owns a fresh database; uses synthetic embeddings and never calls a provider. */
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
  private[mongo] final case class Settings(output: Path, revision: String, concurrency: Int)
  private[mongo] def parse(args: List[String]): Either[String, Settings] = args match {
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
        "Use --authorize-disposable-atlas --output .local/data/search-evaluation --source-revision REV --concurrency 1|8"
      )
  }
  private[mongo] def ownedDatabaseName(nonce: UUID): String =
    s"search_evaluation_curated_${nonce.toString.replace("-", "")}"

  override def run(args: List[String]): IO[ExitCode] = parse(args) match {
    case Left(message)   => IO.println(message).as(ExitCode.Error)
    case Right(settings) =>
      sys.env.get("ATLAS_TEST_URI") match {
        case None => IO.println("ATLAS_TEST_URI is required after explicit disposable Atlas opt-in").as(ExitCode.Error)
        case Some(uri) =>
          MongoDatabaseProbe
            .clientResource(uri)
            .use { client =>
              Resource
                .make(
                  IO(UUID.randomUUID()).flatMap(nonce => client.getDatabase(ownedDatabaseName(nonce)))
                )(db => db.runCommand(mongo4cats.bson.Document.fromJava(new Document("dropDatabase", 1))).void)
                .use(database => evaluate(database, settings))
            }
            .attempt
            .flatMap {
              case Right(_) =>
                IO.println(
                  "Curated execution capture written; semantic quality and privacy acceptance remain unobserved."
                ).as(ExitCode.Success)
              case Left(_) =>
                IO.println("Curated Atlas evaluation failed; connection diagnostics were suppressed.")
                  .as(ExitCode.Error)
            }
      }
  }

  private def seed(database: MongoDatabase[IO]): IO[Unit] = for {
    _ <- database.createCollection(MongoCollections.Jobs)
    _ <- database.createCollection(MongoCollections.Users)
    jobs <- Mongo4catsCollections.documents(database, MongoCollections.Jobs)
    users <- Mongo4catsCollections.documents(database, MongoCollections.Users)
    // Physically absent job 7 is the deleted-job fixture; stale job 8 carries an obsolete hash.
    _ <- jobs.insertMany(
      HiringSearchEvaluationCorpus.jobs
        .filterNot(_.id.value.toString == SearchEvaluationFixtures.jobId(7))
        .map(HiringSearchEvaluationCorpus.jobDocument)
    )
    _ <- users.insertMany(
      HiringSearchEvaluationCorpus.candidates.map(HiringSearchEvaluationCorpus.candidateDocument) :+
        MongoHiringCodecs.user(HiringSearchEvaluationCorpus.recruiter)
    )
    _ <- MongoAtlasSearchSetup.provision(MongoHiringSetup.SetupDatabase(database, Map.empty), indexes)
  } yield ()

  private def evaluate(database: MongoDatabase[IO], settings: Settings): IO[Unit] = for {
    _ <- seed(database)
    fingerprint <- SearchEvaluationArtifacts.sourceFingerprint
    timestamp <- IO.realTimeInstant
    version <- database
      .runCommand(mongo4cats.bson.Document.fromJava(new Document("buildInfo", 1)))
      .map(
        _.getString("version")
          .filter(value => value.length <= 128 && value.matches("[0-9]+(\\.[0-9]+){1,3}[-a-zA-Z0-9.]*"))
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
      identity = s"curated-disposable-Atlas-${database.underlying.getName}",
      atlasVersion = version,
      telemetryUnavailable =
        ((SearchEvaluationFixtures.environment.telemetryUnavailable - SearchEvaluationTelemetry.Latency - SearchEvaluationTelemetry.AtlasVersion)
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
          Resource.pure[IO, SearchEvaluationCapture.Retrieval](new HiringSearchEvaluationRetrieval(database)),
          maximumResults = SearchEvaluationFixtures.K
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
                embeddingModel = HiringSearchEvaluationCorpus.Model,
                embeddingProvenance = SearchEvaluationEmbeddingProvenance.SyntheticFixture,
                indexIdentity = indexIdentity
              ),
            concurrency = settings.concurrency,
            durationMillis = Some((ended - started).toMillis.max(1L)),
            environment = environment,
            queries = observations
          )
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
      SearchEvaluationUseCase.values.toList.map(_ -> SearchEvaluationUseCasePolicy()).toMap
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
  } yield ()
}
