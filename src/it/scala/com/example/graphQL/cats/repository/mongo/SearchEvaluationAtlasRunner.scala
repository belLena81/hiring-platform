package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.infrastructure.search.{SearchEvaluationArtifacts, SearchEvaluationReportJson}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.domain.pagination.PageSize
import mongo4cats.client.MongoClient
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.codecs.CodecRegistry
import mongo4cats.bson.Document as CatsDocument
import org.bson.Document

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Random

/** Creates a synthetic disposable Atlas collection, captures paired ANN/ENN runs, and drops it. */
object SearchEvaluationAtlasRunner extends IOApp {
  private val MaxNumCandidates = 10000
  private val ExplainTimeoutMillis = 30000
  private final case class Settings(
      uri: String,
      database: String,
      outputDirectory: Path,
      datasetDocuments: Int,
      queryCount: Int,
      dimensions: Int,
      numCandidates: Int,
      branchLimit: Int,
      storedSource: Boolean,
      quantization: Option[String],
      pageSize: Int,
      concurrency: Int,
      temperature: String,
      seed: Long,
      sourceRevision: String
  )

  private final case class TimedRanking(
      ids: List[String],
      latencyMillis: Double,
      error: Option[SearchEvaluationFailure]
  )
  private final case class PairedRanking(queryId: String, ann: TimedRanking, enn: TimedRanking)

  override def run(args: List[String]): IO[ExitCode] =
    parse(args) match {
      case Left(message)   => IO.println(message).as(ExitCode.Error)
      case Right(settings) =>
        MongoDatabaseProbe.clientResource(settings.uri).use { client =>
          runEvaluation(client, settings).attempt.flatMap {
            case Right(_) =>
              IO.println("Synthetic Atlas search evaluation completed; JSON records were written.").as(ExitCode.Success)
            case Left(error) =>
              IO.println(s"Atlas search evaluation failed: ${error.getClass.getSimpleName}").as(ExitCode.Error)
          }
        }
    }

  private def parse(args: List[String]): Either[String, Settings] = {
    val values = args.grouped(2).collect { case List(key, value) if key.startsWith("--") => key.drop(2) -> value }.toMap
    def int(name: String, default: Int): Either[String, Int] =
      values.get(name).fold[Either[String, Int]](Right(default))(value => value.toIntOption.toRight(s"Invalid --$name"))
    for {
      uri <- sys.env.get("ATLAS_TEST_URI").toRight("Set ATLAS_TEST_URI for an authorized disposable Atlas deployment.")
      database <- values.get("database").toRight("Supply --database for the disposable evaluation collection.")
      output <- values.get("output").toRight("Supply --output directory for JSON run records.")
      sourceRevision <- values
        .get("source-revision")
        .filter(_.trim.nonEmpty)
        .toRight("Supply --source-revision for this working tree; its Scala fingerprint is captured separately.")
      documents <- int("documents", 128)
      queries <- int("queries", 20)
      dimensions <- int("dimensions", 1024)
      candidates <- int("num-candidates", 100)
      branchLimit <- int("branch-limit", 100)
      storedSource <- values
        .get("stored-source")
        .fold[Either[String, Boolean]](Right(false))(_.toBooleanOption.toRight("Invalid --stored-source"))
      quantization <- values
        .get("quantization")
        .filter(_ != "none")
        .traverse(value => Either.cond(value == "scalar", value, "--quantization must be none or scalar"))
      pageSize <- int("page-size", 7)
      concurrency <- int("concurrency", 1)
      temperature = values.getOrElse("temperature", "cold")
      seed <- values.get("seed").fold[Either[String, Long]](Right(20261005L))(_.toLongOption.toRight("Invalid --seed"))
      _ <- Either.cond(documents > 0 && documents <= 10000, (), "--documents must be between 1 and 10000")
      _ <- Either.cond(queries > 0 && queries <= 100, (), "--queries must be between 1 and 100")
      _ <- Either.cond(dimensions >= 17 && dimensions <= 4096, (), "--dimensions must be between 17 and 4096")
      _ <- Either.cond(
        pageSize >= PageSize.Min && pageSize <= PageSize.Max,
        (),
        s"--page-size must be between ${PageSize.Min} and ${PageSize.Max}"
      )
      _ <- Either.cond(
        branchLimit >= pageSize && branchLimit <= PageSize.Max && candidates >= branchLimit && candidates <= MaxNumCandidates,
        (),
        s"--num-candidates must cover branch limit (page size <= branch limit <= 100) and be at most $MaxNumCandidates"
      )
      _ <- Either.cond(Set(1, 8).contains(concurrency), (), "--concurrency must be 1 or 8")
      _ <- Either.cond(Set("cold", "warm").contains(temperature), (), "--temperature must be cold or warm")
      _ <- Either.cond(
        database.startsWith("search_evaluation_"),
        (),
        "--database must use the isolated search_evaluation_ prefix"
      )
      _ <- Either.cond(
        Path.of(output).normalize().startsWith(Path.of(".local/data")),
        (),
        "--output must be beneath .local/data"
      )
    } yield Settings(
      uri,
      s"${database}_${UUID.randomUUID().toString.replace("-", "")}",
      Path.of(output),
      documents,
      queries,
      dimensions,
      candidates,
      branchLimit,
      storedSource,
      quantization,
      pageSize,
      concurrency,
      temperature,
      seed,
      sourceRevision
    )
  }

  private def runEvaluation(client: MongoClient[IO], settings: Settings): IO[Unit] =
    client.getDatabase(settings.database).flatMap { database =>
      Resource
        .make(IO.pure(database))(_ =>
          database
            .runCommand(CatsDocument.fromJava(new Document("dropDatabase", 1)))
            .attempt
            .void
        )
        .use { _ =>
          val collectionName = s"search_evaluation_${UUID.randomUUID().toString.replace("-", "")}"
          database.getCollection[CatsDocument](collectionName, CodecRegistry.Default).map(_.as[Document]).flatMap {
            collection =>
              val indexName = "synthetic_vector"
              val vectorField = new Document("type", "vector")
                .append("path", "embedding")
                .append("numDimensions", Int.box(settings.dimensions))
                .append("similarity", "cosine")
              settings.quantization.foreach(value => vectorField.append("quantization", value))
              val indexDefinition = new Document(
                "fields",
                List(
                  vectorField,
                  new Document("type", "filter").append("path", "category")
                ).asJava
              )

              if (settings.storedSource) {
                val _ = indexDefinition.append("storedSource", new Document("include", List("_id", "category").asJava))
              }
              Resource
                .make(
                  database.createCollection(collectionName).as(collection)
                )(ownedCollection => ownedCollection.drop)
                .use { _ =>
                  for {
                    _ <- seedCollection(collection, settings)
                    _ <- createSearchIndex(database, collectionName, indexName, indexDefinition)
                    _ <- awaitIndex(collection, indexName, 120.seconds)
                    version <- database
                      .runCommand(CatsDocument.fromJava(new Document("buildInfo", 1)))
                      .map(_.getString("version").getOrElse("unknown"))
                    _ <- Option
                      .when(settings.temperature == "warm")(
                        (1 to settings.queryCount).toList.traverse_(query =>
                          retrieve(
                            collection,
                            settings,
                            syntheticVector(settings.seed ^ query.toLong, settings.dimensions, query % 10, query % 7),
                            Some(query % 10),
                            exact = false
                          ).void
                        )
                      )
                      .getOrElse(IO.unit)
                    started <- IO.monotonic
                    queryResults <- (1 to settings.queryCount).toList
                      .grouped(settings.concurrency)
                      .toList
                      .traverse(batch => batch.parTraverse(query => evaluateQuery(collection, settings, query)))
                      .map(_.flatten)
                    ended <- IO.monotonic
                    indexBytes <- database
                      .runCommand(CatsDocument.fromJava(new Document("collStats", collectionName)))
                      .map(_.getAs[Long]("totalIndexSize"))
                      .handleError(_ => None)
                    _ <- writeReports(settings, version, queryResults, (ended - started).toMillis, indexBytes)
                    _ <- captureExplain(database, collectionName, settings)
                  } yield ()
                }
          }
        }
    }

  private def createSearchIndex(
      database: MongoDatabase[IO],
      collectionName: String,
      indexName: String,
      definition: Document
  ): IO[Unit] =
    database
      .runCommand(
        CatsDocument.fromJava(
          new Document("createSearchIndexes", collectionName)
            .append(
              "indexes",
              List(
                new Document("name", indexName)
                  .append("type", "vectorSearch")
                  .append("definition", definition)
              ).asJava
            )
        )
      )
      .void

  private def seedCollection(collection: MongoCollection[IO, Document], settings: Settings): IO[Unit] = {
    (0 until settings.datasetDocuments).grouped(500).toList.traverse_ { ids =>
      val documents = ids.map { id =>
        val vector = syntheticVector(settings.seed + id.toLong, settings.dimensions, id % 10, id % 7)
        new Document("_id", f"doc-$id%05d")
          .append("category", Int.box(id % 10))
          .append("judgedTopic", Int.box(id % 7))
          .append("embedding", vector.map(java.lang.Double.valueOf).asJava)
      }.toList
      collection.insertMany(documents)
    }
  }

  private def evaluateQuery(
      collection: MongoCollection[IO, Document],
      settings: Settings,
      queryNumber: Int
  ): IO[PairedRanking] = {
    val queryId = f"q-$queryNumber%03d"
    val category = queryNumber % 10
    val topic = queryNumber % 7
    val queryVector = syntheticVector(settings.seed ^ queryNumber.toLong, settings.dimensions, category, topic)
    val filterCategory = queryNumber % 3 match {
      case 0 => None
      case 1 => Some(category)
      case _ => Some(10)
    }
    val ann = timedRanking(collection, settings, queryVector, filterCategory, exact = false)
    val enn = timedRanking(collection, settings, queryVector, filterCategory, exact = true)
    if (queryNumber % 2 == 0) (ann, enn).tupled.map { case (annResult, ennResult) =>
      PairedRanking(queryId, annResult, ennResult)
    }
    else (enn, ann).tupled.map { case (ennResult, annResult) => PairedRanking(queryId, annResult, ennResult) }
  }

  private def timedRanking(
      collection: MongoCollection[IO, Document],
      settings: Settings,
      vector: List[Double],
      category: Option[Int],
      exact: Boolean
  ): IO[TimedRanking] =
    for {
      started <- IO.monotonic
      result <- retrieve(collection, settings, vector, category, exact).attempt
      ended <- IO.monotonic
    } yield result.fold(
      _ =>
        TimedRanking(
          Nil,
          (ended - started).toNanos.toDouble / 1000000.0,
          Some(if (exact) SearchEvaluationFailure.ReferenceFailed else SearchEvaluationFailure.RetrievalFailed)
        ),
      ids => TimedRanking(ids, (ended - started).toNanos.toDouble / 1000000.0, None)
    )

  private def captureExplain(database: MongoDatabase[IO], collectionName: String, settings: Settings): IO[Unit] = {
    val vector = syntheticVector(settings.seed ^ 1L, settings.dimensions, 1, 1)
    List(false, true).traverse_ { exact =>
      val pipeline = retrievalPipeline(settings, vector, Some(1), exact)
      val aggregate = new Document("aggregate", collectionName)
        .append("pipeline", pipeline.asJava)
        .append("cursor", new Document())
        .append("maxTimeMS", Int.box(ExplainTimeoutMillis))
      MongoAccessEvaluationSupport
        .command(database, new Document("explain", aggregate).append("verbosity", "executionStats"))
        .attempt
        .flatMap { result =>
          val json = result.fold(
            error =>
              io.circe.Json.obj(
                "status" -> io.circe.Json.fromString("Unavailable"),
                "errorType" -> io.circe.Json.fromString(error.getClass.getSimpleName)
              ),
            document => io.circe.parser.parse(document.toJson).getOrElse(io.circe.Json.Null)
          )
          IO.blocking {
            val _ = Files.createDirectories(settings.outputDirectory)
            val _ = Files.writeString(
              settings.outputDirectory.resolve(s"vector-explain-${if (exact) "enn" else "ann"}.json"),
              json.spaces2,
              StandardCharsets.UTF_8
            )
          }
        }
    }
  }

  private def retrievalPipeline(
      settings: Settings,
      vector: List[Double],
      category: Option[Int],
      exact: Boolean
  ): List[Document] = {
    val vectorSearch = new Document("index", "synthetic_vector")
      .append("path", "embedding")
      .append("queryVector", vector.map(java.lang.Double.valueOf).asJava)
      .append("limit", Int.box(settings.branchLimit))
    category.foreach(value => vectorSearch.append("filter", new Document("category", Int.box(value))))
    if (settings.storedSource) { val _ = vectorSearch.append("returnStoredSource", java.lang.Boolean.TRUE) }
    if (exact) vectorSearch.append("exact", java.lang.Boolean.TRUE)
    else vectorSearch.append("numCandidates", Int.box(settings.numCandidates))
    List(new Document("$vectorSearch", vectorSearch), new Document("$project", new Document("_id", 1)))
  }

  private def retrieve(
      collection: MongoCollection[IO, Document],
      settings: Settings,
      vector: List[Double],
      category: Option[Int],
      exact: Boolean
  ): IO[List[String]] = {
    val pipeline = retrievalPipeline(settings, vector, category, exact)
    collection.aggregate[Document](pipeline).stream.take(settings.branchLimit.toLong + 1L).compile.toList.flatMap {
      results =>
        if (results.size > settings.branchLimit)
          IO.raiseError(new IllegalStateException(s"Atlas search result exceeded branch limit ${settings.branchLimit}"))
        else IO.pure(results.map(_.getString("_id")).distinct.take(settings.pageSize))
    }
  }

  private def writeReports(
      settings: Settings,
      atlasVersion: String,
      results: List[PairedRanking],
      durationMillis: Long,
      indexBytes: Option[Long]
  ): IO[Unit] = {
    val entityIds = (0 until settings.datasetDocuments).map(id => f"doc-$id%05d").toList
    val corpusIdentity =
      s"synthetic-vector-${settings.seed}-${settings.datasetDocuments}-${settings.dimensions}-${settings.queryCount}"
    val corpus = SearchEvaluationCorpus(
      corpusIdentity,
      SourceHash.sha256(corpusIdentity),
      settings.seed,
      entityIds,
      SearchEvaluationRubric(
        "independent-synthetic-topic",
        SearchEvaluationJudgmentOrigin.Synthetic,
        false,
        "synthetic topic generator"
      ),
      results.zipWithIndex.map { case (result, index) =>
        val number = index + 1
        val eligible = entityIds.zipWithIndex.collect {
          case (id, entityIndex) if number % 3 == 0 || (number % 3 == 1 && entityIndex % 10 == number % 10) => id
        }
        val group = number % 3 match {
          case 0 => SearchEvaluationFilterGroup.Broad
          case 1 => SearchEvaluationFilterGroup.Selective
          case _ => SearchEvaluationFilterGroup.Empty
        }
        SearchEvaluationFixtureQuery(
          result.queryId,
          SearchEvaluationUseCase.JobSearch,
          group,
          SearchEvaluationSplit.HeldOut,
          s"category-bucket-${number % 3}",
          eligible,
          judgedRelevantIds(settings, result.queryId),
          SearchEvaluationLabelReview.Pending
        )
      }
    )
    def observation(value: TimedRanking): SearchEvaluationRanking = value.error match {
      case Some(category) => SearchEvaluationRanking.Failed(category, Some(value.latencyMillis))
      case None           => SearchEvaluationRanking.Succeeded(value.ids, Some(value.latencyMillis))
    }
    for {
      fingerprint <- SearchEvaluationArtifacts.sourceFingerprint
      timestamp <- IO.realTimeInstant
      run = SearchEvaluationRun(
        SearchEvaluationStrategy.AtlasAnn,
        SearchEvaluationCoordinates(
          settings.sourceRevision,
          fingerprint,
          corpus.identity,
          corpus.digest,
          corpus.rubric.identity,
          SearchEvaluationRankingOrigin.ObservedAtlas,
          "deterministic-synthetic",
          settings.dimensions,
          s"synthetic-vector-cosine-dimensions-${settings.dimensions}-category-storedSource-${settings.storedSource}-quantization-${settings.quantization.getOrElse("none")}",
          settings.numCandidates,
          settings.branchLimit,
          settings.pageSize,
          "captured Atlas branch order"
        ),
        settings.concurrency,
        if (settings.temperature == "warm") settings.queryCount else 0,
        settings.temperature == "warm",
        timestamp,
        Option.when(durationMillis > 0L)(durationMillis),
        SearchEvaluationEnvironment(
          "disposable synthetic Atlas",
          Some(atlasVersion),
          indexBytes,
          None,
          None,
          None,
          Some(0L),
          Map(
            SearchEvaluationTelemetry.Cpu -> SearchEvaluationTelemetryReason.NotCaptured,
            SearchEvaluationTelemetry.Memory -> SearchEvaluationTelemetryReason.NotCaptured,
            SearchEvaluationTelemetry.VectorIndex -> SearchEvaluationTelemetryReason.NotCaptured,
            SearchEvaluationTelemetry.Billing -> SearchEvaluationTelemetryReason.NoProviderUsed,
            SearchEvaluationTelemetry.AtlasQueryMetrics -> SearchEvaluationTelemetryReason.NotCaptured
          ) ++
            Option.when(indexBytes.isEmpty)(
              SearchEvaluationTelemetry.CollectionIndex -> SearchEvaluationTelemetryReason.NotCaptured
            )
        ),
        results.map(result => SearchEvaluationQuery(result.queryId, observation(result.ann), observation(result.enn)))
      )
      report <- IO.fromEither(
        SearchEvaluationHarness
          .report(corpus, run, settings.pageSize)
          .left
          .map(_ => new IllegalArgumentException("Invalid synthetic evaluation observations"))
      )
      json = SearchEvaluationReportJson.render(report).spaces2
      filename = s"search-evaluation-${settings.concurrency}-${timestamp.toEpochMilli}.json"
      _ <- IO.blocking {
        Files.createDirectories(settings.outputDirectory)
        Files.writeString(settings.outputDirectory.resolve(filename), json, StandardCharsets.UTF_8)
        ()
      }
    } yield ()
  }

  private def judgedRelevantIds(settings: Settings, queryId: String): Set[String] = {
    val queryNumber = queryId.drop(2).toInt
    val topic = queryNumber % 7
    (0 until settings.datasetDocuments).iterator
      .filter { id =>
        queryNumber % 3 match {
          case 0 => id % 7 == topic
          case 1 => id % 10 == queryNumber % 10 && id % 7 == topic
          case _ => false
        }
      }
      .map(id => f"doc-$id%05d")
      .toSet
  }

  private def awaitIndex(collection: MongoCollection[IO, Document], name: String, timeout: FiniteDuration): IO[Unit] = {
    val deadline = IO.monotonic.map(_ + timeout)
    def poll(until: FiniteDuration): IO[Unit] =
      collection
        .aggregate[Document](List(new Document("$listSearchIndexes", new Document())))
        .stream
        .take(101L)
        .compile
        .toList
        .flatMap { indexes =>
          indexes.find(_.getString("name") == name) match {
            case Some(index) if index.getBoolean("queryable", java.lang.Boolean.FALSE).booleanValue() => IO.unit
            case _                                                                                    =>
              IO.monotonic.flatMap(now =>
                if (now >= until) IO.raiseError(new IllegalStateException("Synthetic Atlas vector index timed out"))
                else IO.sleep(1.second) *> poll(until)
              )
          }
        }
    deadline.flatMap(poll)
  }

  private def syntheticVector(seed: Long, dimensions: Int, category: Int, topic: Int): List[Double] = {
    val random = new Random(seed)
    val values = List.fill(dimensions)(random.nextGaussian() * 0.1).toArray
    values(category) = values(category) + 1.0
    values(10 + topic) = values(10 + topic) + 1.0
    val norm = math.sqrt(values.map(value => value * value).sum)
    values.toList.map(_ / norm)
  }
}
