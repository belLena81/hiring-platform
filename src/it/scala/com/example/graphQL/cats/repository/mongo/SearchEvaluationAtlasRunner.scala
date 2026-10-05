package com.example.graphQL.cats.repository.mongo

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.search.{SearchEvaluationHarness, SearchEvaluationQuery, SearchEvaluationRun}
import com.example.graphQL.cats.domain.pagination.PageSize
import mongo4cats.client.MongoClient
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.codecs.CodecRegistry
import mongo4cats.bson.Document as CatsDocument
import io.circe.Json
import org.bson.Document

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Random

/** Creates a synthetic disposable Atlas collection, captures paired ANN/ENN runs, and drops it. */
object SearchEvaluationAtlasRunner extends IOApp {
  private val MaxNumCandidates = 1000
  private final case class Settings(
      uri: String,
      database: String,
      outputDirectory: Path,
      datasetDocuments: Int,
      queryCount: Int,
      dimensions: Int,
      numCandidates: Int,
      pageSize: Int,
      concurrency: Int,
      temperature: String,
      seed: Long
  )

  private final case class TimedRanking(ids: List[String], latencyMillis: Double, error: Option[String])
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
      documents <- int("documents", 128)
      queries <- int("queries", 20)
      dimensions <- int("dimensions", 1024)
      candidates <- int("num-candidates", 100)
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
        candidates >= pageSize && candidates <= MaxNumCandidates,
        (),
        s"--num-candidates must cover page size and be at most $MaxNumCandidates"
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
      pageSize,
      concurrency,
      temperature,
      seed
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
              val indexDefinition = new Document(
                "fields",
                List(
                  new Document("type", "vector")
                    .append("path", "embedding")
                    .append("numDimensions", Int.box(settings.dimensions))
                    .append("similarity", "cosine"),
                  new Document("type", "filter").append("path", "category")
                ).asJava
              )

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
      error => TimedRanking(Nil, (ended - started).toNanos.toDouble / 1000000.0, Some(error.getClass.getSimpleName)),
      ids => TimedRanking(ids, (ended - started).toNanos.toDouble / 1000000.0, None)
    )

  private def retrieve(
      collection: MongoCollection[IO, Document],
      settings: Settings,
      vector: List[Double],
      category: Option[Int],
      exact: Boolean
  ): IO[List[String]] = {
    val vectorSearch = new Document("index", "synthetic_vector")
      .append("path", "embedding")
      .append("queryVector", vector.map(java.lang.Double.valueOf).asJava)
      .append("limit", Int.box(settings.pageSize))
    category.foreach(value => vectorSearch.append("filter", new Document("category", Int.box(value))))
    if (exact) vectorSearch.append("exact", java.lang.Boolean.TRUE)
    else vectorSearch.append("numCandidates", Int.box(settings.numCandidates))
    val pipeline = List(
      new Document("$vectorSearch", vectorSearch),
      new Document("$project", new Document("_id", 1))
    )
    collection.aggregate[Document](pipeline).stream.take(settings.pageSize.toLong + 1L).compile.toList.flatMap {
      results =>
        if (results.size > settings.pageSize)
          IO.raiseError(new IllegalStateException(s"Atlas search result exceeded page size ${settings.pageSize}"))
        else IO.pure(results.map(_.getString("_id")))
    }
  }

  private def writeReports(
      settings: Settings,
      atlasVersion: String,
      results: List[PairedRanking],
      durationMillis: Long,
      indexBytes: Option[Long]
  ): IO[Unit] = {
    val filters = Json.obj(
      "categories" -> Json.fromInt(10),
      "queryBuckets" -> Json.fromString("broad, selective, and empty filters in round-robin order")
    )
    val queryMetrics =
      Json.obj("source" -> Json.fromString("runner local observations; Atlas query metrics unavailable"))
    val report = SearchEvaluationRun(
      strategy = "atlasAnnComparedWithEnn",
      datasetDocuments = settings.datasetDocuments,
      filters = filters,
      embeddingModel = "deterministic-synthetic",
      embeddingDimensions = settings.dimensions,
      quantization = "index-default",
      numCandidates = settings.numCandidates,
      branchResultLimit = settings.numCandidates,
      pageSize = settings.pageSize,
      concurrency = settings.concurrency,
      durationMillis = durationMillis,
      warmupQueries = if (settings.temperature == "warm") settings.queryCount else 0,
      temperature = settings.temperature,
      timestampUtc = Instant.now().toString,
      environment = s"${settings.database} disposable synthetic Atlas workload",
      atlasVersion = atlasVersion,
      collectionIndexBytes = indexBytes,
      vectorSearchIndexBytes = None,
      cpuMillis = None,
      peakMemoryBytes = None,
      providerRequests = Some(0L),
      queryMetrics = queryMetrics,
      queries = results.map { result =>
        SearchEvaluationQuery(
          result.queryId,
          judgedRelevantIds(settings, result.queryId),
          result.ann.ids,
          result.enn.ids,
          result.ann.latencyMillis,
          Some(result.enn.latencyMillis),
          result.ann.error.orElse(result.enn.error)
        )
      }
    )
    val json = SearchEvaluationHarness.report(report, settings.pageSize).spaces2
    val filename = s"search-evaluation-${settings.concurrency}-${Instant.now().toEpochMilli}.json"
    IO.blocking {
      Files.createDirectories(settings.outputDirectory)
      Files.writeString(settings.outputDirectory.resolve(filename), json, StandardCharsets.UTF_8)
      ()
    }
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
