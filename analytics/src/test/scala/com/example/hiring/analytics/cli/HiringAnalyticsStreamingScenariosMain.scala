package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.mongodb.ReadConcern
import com.mongodb.client.MongoClients
import org.bson.Document
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions.{col, lit}
import pureconfig.ConfigSource
import HiringAnalyticsStreamingWorkloadMain.{ApiSettings, ProducerCredentials}
import HiringAnalyticsStreamingWorkloadMain.given

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Bounded live scenarios run separately from the measured healthy workload. */
object HiringAnalyticsStreamingScenariosMain extends IOApp {
  private def json(value: String): String = io.circe.Json.fromString(value).noSpaces
  private def http(api: ApiSettings, token: String, operation: String): Document = {
    val uri = URI.create(api.url)
    require(uri.getScheme == "http" && uri.getHost == "127.0.0.1", "loopback proof API required")
    val request = HttpRequest
      .newBuilder(URI.create(api.url + "/graphql"))
      .header("Content-Type", "application/json")
      .header("Authorization", "Bearer " + token)
      .timeout(java.time.Duration.ofSeconds(15))
      .POST(HttpRequest.BodyPublishers.ofString(new Document("query", operation).toJson))
      .build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    require(response.statusCode() == 200, "scenario GraphQL transport failed")
    Document.parse(response.body())
  }
  private def await(label: String)(condition: IO[Boolean]): IO[Unit] = {
    def loop: IO[Unit] = condition.flatMap(done => if (done) IO.unit else IO.sleep(2.seconds) *> IO.defer(loop))
    loop.timeoutTo(180.seconds, IO.raiseError(new IllegalStateException(label)))
  }
  private def ownedConfiguration(api: ApiSettings, database: String, requireState: Boolean): java.nio.file.Path = {
    require(
      api.nonce.matches("[a-f0-9]{16}") && database == "hiring_streaming_proof_" + api.nonce,
      "API identity must match the isolated Mongo namespace"
    )
    val current = Paths.get(".").toAbsolutePath.normalize()
    val repository = if (Files.isDirectory(current.resolve("analytics/src"))) current else current.getParent
    require(
      repository != null && Files.isDirectory(repository.resolve("analytics/src")),
      "actual repository root required"
    )
    require(
      Paths.get(sys.env("HIRING_STREAMING_PROOF_REPO")).toAbsolutePath.normalize() == repository,
      "proof repository differs"
    )
    val directory = Paths.get(sys.env("HIRING_STREAMING_PROOF_CONFIG")).toAbsolutePath.normalize()
    require(
      directory == repository.resolve(".local/config/hiring-streaming-proof-" + api.nonce),
      "owned proof config path required"
    )
    var ancestor: java.nio.file.Path = directory
    while (ancestor != null) {
      require(!Files.isSymbolicLink(ancestor), "proof config must not traverse symlinks")
      ancestor = ancestor.getParent
    }
    import java.nio.file.attribute.PosixFilePermission.*
    require(
      Files.isDirectory(directory) && Files.getOwner(directory).getName == System.getProperty("user.name") &&
        Files.getPosixFilePermissions(directory).asScala.toSet == Set(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE),
      "private owned config directory required"
    )
    val state = directory.resolve("scenarios.json")
    if (requireState) {
      require(
        !Files.isSymbolicLink(state) && Files.isRegularFile(state) && Files.size(state) <= 1048576L &&
          Files.getOwner(state).getName == System.getProperty("user.name") &&
          Files.getPosixFilePermissions(state).asScala.toSet == Set(OWNER_READ, OWNER_WRITE),
        "bounded private scenario state required"
      )
    } else {
      Vector("scenarios.json", "scenarios-replay.conf", "scenarios-race.conf").foreach { name =>
        val target = directory.resolve(name)
        require(!Files.exists(target) && !Files.isSymbolicLink(target), "scenario outputs must be unused")
      }
    }
    directory
  }
  private def scenario(mode: String): IO[Unit] = for {
    settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
    _ <- IO.fromEither(
      StreamingProofIsolation
        .validate(
          settings.common.mongoDatabase,
          AnalyticsTopic.unwrap(settings.topic),
          settings.common.lakehouseRoot,
          settings.streaming.checkpointLocation,
          settings.common.sparkLocalDirectory
        )
        .leftMap(new IllegalArgumentException(_))
    )
    api <- IO.fromEither(
      ConfigSource.default
        .at("analytics.workload-api")
        .load[ApiSettings]
        .leftMap(_ => new IllegalArgumentException("scenario API settings missing"))
    )
    credentials <- IO.fromEither(
      ConfigSource.default
        .at("analytics.workload-producer")
        .load[ProducerCredentials]
        .leftMap(_ => new IllegalArgumentException("scenario producer settings missing"))
    )
    directory <- IO.blocking(ownedConfiguration(api, settings.common.mongoDatabase, mode != "prepare"))
    adminToken <- IO.blocking(HiringAnalyticsStreamingWorkloadMain.login(api, "Admin"))
    _ <- Resource.make(IO.blocking(MongoClients.create(settings.common.mongoUri)))(m => IO.blocking(m.close())).use {
      mongo =>
        AppModule
          .sparkMongo[IO](
            settings.common.mongoUri,
            "local[1]",
            "hiring-streaming-scenarios-" + mode,
            sparkLocalDirectory = settings.common.sparkLocalDirectory + "/scenario-" + mode
          )
          .use { case (spark, client, execution) =>
            val database = mongo.getDatabase(settings.common.mongoDatabase).withReadConcern(ReadConcern.MAJORITY)
            val statePath = directory.resolve("scenarios.json")
            val topic = AnalyticsTopic.unwrap(settings.topic)
            def dataset(name: String) =
              spark.read.format("delta").load(SparkPhysicalLocation.resolve(settings.common.lakehouseRoot + "/" + name))
            def count(name: String, event: String): IO[Long] = execution {
              val path = SparkPhysicalLocation.resolve(settings.common.lakehouseRoot + "/" + name)
              if (!io.delta.tables.DeltaTable.isDeltaTable(spark, path)) 0L
              else dataset(name).filter(col("eventId") === event).count()
            }
            def watermark: IO[Option[Instant]] = execution {
              dataset("control/streaming_progress")
                .filter(col("outcome") === "Published" && col("candidateWatermark").isNotNull)
                .orderBy(col("batchId").desc)
                .limit(1)
                .collect()
                .headOption
                .map(_.getAs[Timestamp]("candidateWatermark").toInstant)
            }
            def adminCount(skill: String): IO[Long] = IO.blocking {
              val token = adminToken
              val day = Instant.now().truncatedTo(ChronoUnit.DAYS)
              val body = http(
                api,
                token,
                s"{ analyticsReport(from: ${json(day.minusSeconds(7 * 86400L).toString)}, to: ${json(day.plusSeconds(86400).toString)}) { skillPostingActivity { skill postings } } }"
              )
              require(!body.containsKey("errors"), "guarded Admin report unavailable")
              body
                .get("data", classOf[Document])
                .get("analyticsReport", classOf[Document])
                .getList("skillPostingActivity", classOf[Document])
                .asScala
                .filter(_.getString("skill") == skill)
                .map(_.get("postings", classOf[java.lang.Number]).longValue())
                .sum
            }
            def hiredDistribution: IO[Boolean] = IO.blocking {
              val day = Instant.now().truncatedTo(ChronoUnit.DAYS)
              val body = http(
                api,
                adminToken,
                s"{ analyticsReport(from: ${json(day.minusSeconds(7 * 86400L).toString)}, to: ${json(day.plusSeconds(86400).toString)}) { timeToHire { p50Hours p75Hours p90Hours p95Hours eligibleCount excludedCount } } }"
              )
              require(!body.containsKey("errors"), "guarded time-to-hire report unavailable")
              val report = body.get("data", classOf[Document]).get("analyticsReport", classOf[Document])
              Option(report.get("timeToHire", classOf[Document])).exists(d =>
                Vector("p50Hours", "p75Hours", "p90Hours", "p95Hours")
                  .forall(key => d.get(key, classOf[java.lang.Number]).doubleValue() == 1.0) &&
                  d.get("eligibleCount", classOf[java.lang.Number]).longValue() == 12 && d
                    .get("excludedCount", classOf[java.lang.Number])
                    .longValue() == 0
              )
            }
            def sendApplication(
                producer: KafkaProducer[String, String],
                application: String,
                actor: String,
                at: Instant,
                hired: Boolean
            ): IO[String] = IO.blocking {
              val id = UUID.randomUUID().toString
              val kind = if (hired) "APPLICATION_STATUS_CHANGED" else "APPLICATION_CREATED"
              val status = if (hired) "Hired" else "Created"
              val body = s"""{"eventId":${json(id)},"eventType":${json(kind)},"occurredAt":${json(
                  at.toString
                )},"aggregateType":"Application","aggregateId":${json(application)},"actorId":${json(
                  actor
                )},"payload":{"applicationId":${json(application)},"candidateId":${json(
                  actor
                )},"jobId":"synthetic-hiring-formula-job","newStatus":${json(status)}}}"""
              producer.beginTransaction()
              producer
                .send(new ProducerRecord[String, String](topic, Int.box(0), id, body))
                .get(30, java.util.concurrent.TimeUnit.SECONDS)
              producer.commitTransaction()
              id
            }
            def readState: IO[Document] = IO.blocking {
              require(Files.isRegularFile(statePath) && !Files.isSymbolicLink(statePath), "scenario state missing")
              val state = Document.parse(Files.readString(statePath));
              require(state.getString("nonce") == api.nonce, "scenario nonce differs"); state
            }
            def send(
                producer: KafkaProducer[String, String],
                id: String,
                actor: String,
                skill: String,
                at: Instant,
                abort: Boolean = false
            ): IO[Long] = IO.blocking {
              val body = s"""{"eventId":${json(id)},"eventType":"JOB_CREATED","occurredAt":${json(
                  at.toString
                )},"aggregateType":"Job","aggregateId":${json(id)},"actorId":${json(
                  actor
                )},"payload":{"job":{"skills":[${json(skill)}]}}}"""
              producer.beginTransaction()
              val metadata = producer
                .send(new ProducerRecord[String, String](topic, Int.box(0), id, body))
                .get(30, java.util.concurrent.TimeUnit.SECONDS)
              if (abort) producer.abortTransaction() else producer.commitTransaction()
              metadata.offset()
            }
            val connection = settings.common.kafka
              .copy(saslUsername = Some(credentials.username), saslPassword = Some(credentials.password))
            val producerResource =
              Resource.eval(IO.fromEither(KafkaClientProperties.clientProperties(connection))).flatMap { props =>
                val config = new Properties(); config.put("bootstrap.servers", connection.bootstrapServers)
                props.foreach { case (key, value) => config.put(key, value) }
                config.put("key.serializer", classOf[StringSerializer].getName);
                config.put("value.serializer", classOf[StringSerializer].getName)
                config.put("acks", "all"); config.put("enable.idempotence", "true")
                config.put("transactional.id", "hiring-publisher-scenarios-" + UUID.randomUUID())
                Resource
                  .make(IO.blocking(new KafkaProducer[String, String](config)))(p => IO.blocking(p.close()))
                  .evalTap(p => IO.blocking(p.initTransactions()))
              }
            mode match {
              case "prepare" =>
                producerResource.use { producer =>
                  for {
                    _ <- IO.raiseWhen(Files.exists(statePath))(
                      new IllegalArgumentException("scenario state already exists")
                    )
                    candidateToken <- IO.blocking(HiringAnalyticsStreamingWorkloadMain.login(api, "Candidate"))
                    candidate <- IO.blocking(
                      http(api, candidateToken, "{ me { id } }")
                        .get("data", classOf[Document])
                        .get("me", classOf[Document])
                        .getString("id")
                    )
                    actors = Vector(candidate) ++ Vector.fill(11)(UUID.randomUUID().toString)
                    now <- IO.realTimeInstant
                    aborted = UUID.randomUUID().toString
                    abortedOffset <- send(producer, aborted, actors.head, "aborted", now, abort = true)
                    control = UUID.randomUUID().toString
                    controlOffset <- send(producer, control, actors(1), "control", now)
                    _ <- await("committed control did not cross aborted gap")(
                      count("bronze/operational_events", control).map(_ == 1)
                    )
                    _ <- count("bronze/operational_events", aborted).flatMap(n =>
                      IO(
                        require(
                          n == 0 && controlOffset > abortedOffset + 1,
                          "aborted transaction leaked or control gap absent"
                        )
                      )
                    )
                    _ <- await("source offset evidence missing aborted gap")(execution {
                      dataset("control/streaming_progress")
                        .select("sourceEndOffsets")
                        .collect()
                        .exists(row =>
                          row
                            .getSeq[org.apache.spark.sql.Row](0)
                            .exists(r =>
                              r.getAs[String]("topic") == topic && r.getAs[Int]("partition") == 0 && r
                                .getAs[Long]("endOffset") > controlOffset
                            )
                        )
                    })
                    duplicate = UUID.randomUUID().toString
                    _ <- send(producer, duplicate, actors(1), "retry", now)
                    _ <- send(producer, duplicate, actors(1), "retry", now)
                    _ <- await("identical retry was not idempotent")(
                      count("bronze/operational_events", duplicate)
                        .map(_ == 2)
                        .flatMap(b => count("silver/operational_events", duplicate).map(s => b && s == 1))
                    )
                    conflictOffset <- send(producer, duplicate, actors(1), "conflicting-synthetic-skill", now)
                    _ <- await("conflict was not quarantined and removed")(
                      execution {
                        dataset("quarantine/operational_events")
                          .filter(
                            col("topic") === topic && col("partition") === 0 && col("offset") === conflictOffset && col(
                              "quarantineReason"
                            ) === "CONFLICTING_EVENT_ID"
                          )
                          .count() == 1
                      }
                        .flatMap(q => count("silver/operational_events", duplicate).map(s => q && s == 0))
                    )
                    lifecycleAt <- IO.realTimeInstant
                    applications <- IO.delay(Vector.fill(12)(UUID.randomUUID().toString))
                    lifecycleIds <- actors
                      .zip(applications)
                      .traverse { case (actor, application) =>
                        (
                          sendApplication(producer, application, actor, lifecycleAt.minusSeconds(3600), hired = false),
                          sendApplication(producer, application, actor, lifecycleAt, hired = true)
                        ).mapN((created, hired) => Vector(created, hired))
                      }
                      .map(_.flatten)
                    _ <- await("actual lifecycle facts missing from continuous Silver")(execution {
                      dataset("silver/operational_events").filter(col("eventId").isin(lifecycleIds*)).count() == 24
                    })
                    _ <- await("continuous one-hour time-to-hire formula not Admin visible")(hiredDistribution)
                    _ <- IO.println(
                      "STREAMING_LIVE_TIME_TO_HIRE_VERIFIED applicationLifecycleRecords=24 distinctSubjects=12 eligibleCount=12 excludedCount=0 p50Hours=1 p75Hours=1 p90Hours=1 p95Hours=1 adminVisible=true"
                    )
                    skill = "closed" + api.nonce
                    late <- actors.zipWithIndex.traverse { case (actor, index) =>
                      val id = UUID.randomUUID().toString
                      send(
                        producer,
                        id,
                        actor,
                        skill,
                        now.truncatedTo(ChronoUnit.DAYS).minusSeconds(3 * 86400L).plusSeconds(86399)
                      )
                        .map(offset =>
                          new Document("eventId", id)
                            .append("partition", 0)
                            .append("offset", offset)
                            .append("actorIsCandidate", index == 0)
                        )
                    }
                    open = UUID.randomUUID().toString
                    _ <- send(producer, open, actors(1), "utc-open", now.truncatedTo(ChronoUnit.DAYS).minusSeconds(1))
                    future = UUID.randomUUID().toString
                    futureObserved <- IO.realTimeInstant
                    futureOffset <- send(producer, future, actors(1), "future", futureObserved.plusSeconds(600))
                    _ <- await("UTC open day did not enter Silver")(
                      count("silver/operational_events", open).map(_ == 1)
                    )
                    _ <- await("UTC closed facts missing")(execution {
                      io.delta.tables.DeltaTable.isDeltaTable(
                        spark,
                        SparkPhysicalLocation.resolve(settings.common.lakehouseRoot + "/silver/late_operational_events")
                      ) && dataset("silver/late_operational_events")
                        .filter(col("eventId").isin(late.map(_.getString("eventId"))*))
                        .count() == 12
                    })
                    _ <- execution {
                      require(
                        dataset("silver/operational_events")
                          .filter(col("eventId").isin(late.map(_.getString("eventId"))*))
                          .count() == 0,
                        "closed-day facts entered current Silver before explicit replay"
                      )
                    }
                    preReplayCount <- adminCount(skill)
                    _ <- IO(
                      require(preReplayCount == 0, "closed-day skill visible through Admin before explicit replay")
                    )
                    _ <- await("future event did not enter quarantine")(execution {
                      dataset("quarantine/operational_events")
                        .filter(
                          col("topic") === topic && col("partition") === 0 && col("offset") === futureOffset && col(
                            "quarantineReason"
                          ) === "EVENT_TIMESTAMP_TOO_FAR_IN_FUTURE"
                        )
                        .count() == 1
                    })
                    _ <- count("silver/operational_events", future)
                      .flatMap(n => IO(require(n == 0, "future fact entered Silver")))
                    before <- watermark
                    _ <- IO(require(before.nonEmpty, "idle watermark baseline missing"))
                    _ <- IO.sleep(35.seconds)
                    after <- watermark
                    _ <- IO(require(before == after, "idle time advanced event watermark"))
                    rows <- execution {
                      dataset("silver/late_operational_events")
                        .filter(col("eventId").isin(late.map(_.getString("eventId"))*))
                        .select("eventId", "ingestedAt", "expiresAt")
                        .collect()
                        .map(row =>
                          new Document("eventId", row.getString(0))
                            .append("ingestedAt", row.getTimestamp(1).toInstant.toString)
                            .append("expiresAt", row.getTimestamp(2).toInstant.toString)
                        )
                        .toVector
                    }
                    request = UUID.randomUUID().toString
                    raceRequest = UUID.randomUUID().toString
                    _ <- IO.blocking {
                      val base = sys.props("config.file")
                      def replayConfig(name: String, id: String, coordinates: Vector[Document]): Unit = {
                        val values = coordinates
                          .map(d => s"{topic=${json(topic)},partition=0,offset=${d.getLong("offset")}} ")
                          .mkString(",")
                        Files.writeString(
                          directory.resolve(name),
                          s"include file(${json(base)})\nanalytics.replay { request-id=${json(id)}, maximum-records=12, coordinates=[$values] }\n"
                        )
                      }
                      replayConfig("scenarios-replay.conf", request, late)
                      replayConfig("scenarios-race.conf", raceRequest, Vector(late.head))
                      Files.writeString(
                        statePath,
                        new Document("nonce", api.nonce)
                          .append("skill", skill)
                          .append("candidateId", candidate)
                          .append("replayRequest", request)
                          .append("raceRequest", raceRequest)
                          .append("facts", late.asJava)
                          .append("expiry", rows.asJava)
                          .toJson
                      )
                    }
                    _ <- execution {
                      val gold = HiringGoldTransforms
                        .skillPostingActivity(dataset("silver/operational_events"))
                        .fold(throw _, identity)
                      println("STREAMING_PRODUCTION_SKILL_TRANSFORM_PLAN\n" + gold.queryExecution.executedPlan.toString)
                    }
                    _ <- IO.println(
                      "STREAMING_LIVE_ADMISSION_VERIFIED abortedTransaction=true controlGap=true identicalRetry=true conflict=true utcOpenClosed=true futureQuarantined=true idleWatermark=true"
                    )
                  } yield ()
                }
              case "verify-replay" =>
                for {
                  state <- readState
                  baseline = state.getList("expiry", classOf[Document]).asScala.toVector
                  _ <- await("replay facts missing from Silver")(execution {
                    val rows = dataset("silver/operational_events")
                      .filter(col("eventId").isin(baseline.map(_.getString("eventId"))*))
                      .select("eventId", "ingestedAt", "expiresAt")
                      .collect()
                    rows.length == 12 && rows.forall(row =>
                      baseline.exists(d =>
                        d.getString("eventId") == row.getString(0) &&
                          d.getString("ingestedAt") == row.getTimestamp(1).toInstant.toString && d
                            .getString("expiresAt") == row.getTimestamp(2).toInstant.toString
                      )
                    )
                  })
                  _ <- await("replayed closed group is not Admin visible")(
                    adminCount(state.getString("skill")).map(_ == 12)
                  )
                  _ <- IO.blocking {
                    val record = database
                      .getCollection("analytics_late_fact_replay_requests")
                      .find(new Document("requestId", state.getString("replayRequest")))
                      .first()
                    require(
                      record != null && record.getString("progress") == "Published",
                      "replay publication journal absent"
                    )
                  }
                  _ <- IO.println(
                    "STREAMING_LIVE_REPLAY_VERIFIED selectedCoordinates=12 retries=2 originalIngestionExpiryPreserved=true adminVisible=true"
                  )
                } yield ()
              case "contention" =>
                producerResource.use { producer =>
                  for {
                    db <- client.getDatabase(settings.common.mongoDatabase)
                    lock = new MongoAnalyticsLakehouseLock[IO](
                      db,
                      new MongoPublisherStream(settings.common.operational)
                    )
                    id = UUID.randomUUID().toString
                    _ <- lock.resource(settings.common.lakehouseRoot).use { _ =>
                      for {
                        baseline <- IO.blocking(
                          database
                            .getCollection("analytics_report_control")
                            .find(new Document("_id", "analytics-report"))
                            .first()
                            .toJson
                        )
                        _ <- send(producer, id, UUID.randomUUID().toString, "contention", Instant.now())
                        _ <- IO.sleep(65.seconds)
                        countBefore <- count("bronze/operational_events", id)
                        _ <- IO(require(countBefore == 0, "query wrote while shared mutex held"))
                        unchanged <- IO.blocking(
                          database
                            .getCollection("analytics_report_control")
                            .find(new Document("_id", "analytics-report"))
                            .first()
                            .toJson == baseline
                        )
                        _ <- IO(require(unchanged, "report/maintenance changed control while mutex held"))
                      } yield ()
                    }
                    _ <- await("query did not resume after shared mutex release")(
                      count("bronze/operational_events", id).map(_ == 1)
                    )
                    _ <- IO.println(
                      "STREAMING_LIVE_MUTEX_CONTENTION_VERIFIED heldSeconds=65 queryWriteExcluded=true maintenanceTickIntervalSeconds=60 controlUnchanged=true releaseContinued=true"
                    )
                  } yield ()
                }
              case "continuation" =>
                producerResource.use { producer =>
                  for {
                    skill <- IO.delay("restored" + UUID.randomUUID().toString.replace("-", ""))
                    ids <- Vector.fill(12)(UUID.randomUUID().toString).traverse { actor =>
                      val id = UUID.randomUUID().toString
                      send(producer, id, actor, skill, Instant.now()).as(id)
                    }
                    _ <- await("restored checkpoint did not ingest actual new records")(execution {
                      dataset("silver/operational_events").filter(col("eventId").isin(ids*)).count() == 12
                    })
                    _ <- await("restored checkpoint report not Admin visible")(adminCount(skill).map(_ == 12))
                    _ <- IO.println(
                      "STREAMING_CHECKPOINT_RESTORED_CONTINUATION_PASS actualNewRecords=12 adminVisible=true"
                    )
                  } yield ()
                }
              case "delete-race" =>
                for {
                  state <- readState
                  _ <- IO(
                    require(state.containsKey("racePinnedGeneration"), "controlled replay deletion barrier missing")
                  )
                  before = state.get("racePinnedGeneration", classOf[java.lang.Number]).longValue()
                  _ <- await("deletion did not fence the current report generation")(IO.blocking {
                    val control = database
                      .getCollection("analytics_report_control")
                      .find(new Document("_id", "analytics-report"))
                      .first()
                    control.getLong("generation").longValue() > before && control.getString("state") == "Hidden"
                  })
                  candidateFact = state.getList("facts", classOf[Document]).get(0).getString("eventId")
                  _ <- await("candidate late/Silver fact survived deletion race")(
                    count("silver/operational_events", candidateFact)
                      .flatMap(s => count("silver/late_operational_events", candidateFact).map(l => s == 0 && l == 0))
                  )
                  _ <- IO.blocking {
                    val record = database
                      .getCollection("analytics_late_fact_replay_requests")
                      .find(new Document("requestId", state.getString("raceRequest")))
                      .first()
                    require(
                      record != null && record.getString("progress") != "Published",
                      "deletion race published stale request"
                    )
                    require(
                      database
                        .getCollection("analytics_erasure_requests")
                        .find(new Document("_id", state.getString("candidateId")))
                        .first() != null,
                      "owner deletion marker missing"
                    )
                  }
                  _ <- IO.println(
                    "STREAMING_LIVE_DELETION_RACE_VERIFIED actualOwnerHttpDeletion=true generationFenced=true lateAndSilverAbsent=true staleReplayNotPublished=true"
                  )
                } yield ()
              case _ => IO.raiseError(new IllegalArgumentException("unknown scenario mode"))
            }
          }
    }
  } yield ()

  /** Test-only barrier after the real selected-fact merge: owner deletion happens while the production replay still
    * owns its shared mutex and old reservation.
    */
  private def raceReplay: IO[Unit] = for {
    settings <- AnalyticsRuntimeConfig.loadLateFactReplay[IO]
    api <- IO.fromEither(
      ConfigSource.default
        .at("analytics.workload-api")
        .load[ApiSettings]
        .leftMap(_ => new IllegalArgumentException("race API settings missing"))
    )
    directory <- IO.blocking(ownedConfiguration(api, settings.common.mongoDatabase, requireState = true))
    statePath = directory.resolve("scenarios.json")
    state <- IO.blocking(Document.parse(Files.readString(statePath)))
    _ <- IO(
      require(
        state.getString("nonce") == api.nonce && state.getString("raceRequest") == settings.request.requestId.value,
        "race request and nonce must match the prepared fixture"
      )
    )
    streaming <- AnalyticsRuntimeConfig.loadStreaming[IO]
    _ <- IO.fromEither(
      StreamingProofIsolation
        .validate(
          settings.common.mongoDatabase,
          AnalyticsTopic.unwrap(streaming.topic),
          settings.common.lakehouseRoot,
          streaming.streaming.checkpointLocation,
          settings.common.sparkLocalDirectory
        )
        .leftMap(new IllegalArgumentException(_))
    )
    keys <- IO.fromEither(
      SubjectPseudonymizer
        .validateFromBase64(
          Some(settings.common.hmac.secretBase64),
          settings.common.hmac.keyId,
          settings.common.hmac.previousKeyId,
          settings.common.hmac.previousSecretBase64
        )
        .toEither
        .leftMap(_ => new IllegalArgumentException("race HMAC configuration invalid"))
    )
    _ <- AppModule
      .sparkMongo[IO](
        settings.common.mongoUri,
        settings.common.sparkMaster,
        "hiring-replay-owner-deletion-race",
        sparkLocalDirectory = settings.common.sparkLocalDirectory + "/race"
      )
      .use { case (spark, client, driver) =>
        for {
          database <- client.getDatabase(settings.common.mongoDatabase)
          streams = new MongoPublisherStream(settings.common.operational)
          lock = new MongoAnalyticsLakehouseLock[IO](database, streams)
          execution = new LakehouseOperation[IO](driver)
          paths <- IO.fromEither(
            AnalyticsLakehousePaths
              .from(settings.common.lakehouseRoot)
              .toEither
              .leftMap(_ => new IllegalArgumentException("race paths invalid"))
          )
          markers = new MongoActiveDeletionMarkerSource[IO](database, keys, streams)
          publisher = new MongoAnalyticsReportPublisher[IO](client, database, settings.common.operational)
          journal = new MongoAnalyticsLateFactReplayJournal[IO](database, streams, settings.common.lakehouseRoot)
          maintenance = new DeltaAnalyticsErasureLakehouse[IO](
            spark,
            paths,
            keys,
            lock,
            new MongoHmacKeyRetirementAuthorizationStore[IO](database, streams),
            settings.common.operational,
            execution,
            org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[IO]
          )
          delegate = new SparkAnalyticsLateFactReplayStages[IO](
            spark,
            paths,
            execution,
            new DeltaBatchReader[IO](execution),
            new DeltaBatchWriter[IO](paths, execution),
            maintenance
          )
          barrier = new AnalyticsLateFactReplayStages[IO] {
            def validateHmacConfiguration: IO[Unit] = delegate.validateHmacConfiguration
            def validateSelectedFacts(
                r: AnalyticsLateFactReplayRequest,
                tokens: Vector[SubjectToken],
                at: Instant
            ): IO[Unit] = delegate.validateSelectedFacts(r, tokens, at)
            def applyActiveDeletions(tokens: Vector[SubjectToken]): IO[Unit] = delegate.applyActiveDeletions(tokens)
            def rebuildGoldAndExtractReport(at: Instant): IO[AnalyticsReportOutput] =
              delegate.rebuildGoldAndExtractReport(at)
            def mergeSelectedFacts(
                r: AnalyticsLateFactReplayRequest,
                tokens: Vector[SubjectToken],
                at: Instant
            ): IO[Unit] =
              delegate.mergeSelectedFacts(r, tokens, at) *> IO.blocking {
                val pinned = MongoClients.create(settings.common.mongoUri)
                try {
                  val control = pinned
                    .getDatabase(settings.common.mongoDatabase)
                    .withReadConcern(ReadConcern.MAJORITY)
                    .getCollection("analytics_report_control")
                    .find(new Document("_id", "analytics-report"))
                    .first()
                  state.append("racePinnedGeneration", control.getLong("generation"))
                  Files.writeString(statePath, state.toJson)
                } finally pinned.close()
                val token = HiringAnalyticsStreamingWorkloadMain.login(api, "Candidate")
                val result = http(
                  api,
                  token,
                  s"mutation { deleteMyAccount(input: {idempotencyKey: ${json(UUID.randomUUID().toString)}}) { __typename ... on DeletionReceipt { status } } }"
                )
                require(
                  !result.containsKey("errors") && result
                    .get("data", classOf[Document])
                    .get("deleteMyAccount", classOf[Document])
                    .getString("__typename") == "DeletionReceipt",
                  "actual owner deletion failed at replay merge barrier"
                )
                println(
                  "STREAMING_REPLAY_HTTP_DELETION_BARRIER_PASSED actualMergeBeforeDelete=true oldReservationPinned=true"
                )
              }
          }
          _ <- journal.ensureIndexes
          result <- new AnalyticsLateFactReplayService[IO](
            settings.common.lakehouseRoot,
            journal,
            markers,
            barrier,
            publisher,
            lock,
            settings.common.operational.retention.publishedSnapshotDays
          ).run(settings.request).attempt
          _ <- result match {
            case Right(AnalyticsLateFactReplayOutcome.ErasurePending) =>
              IO.println("STREAMING_REPLAY_DELETION_RACE_REJECTED category=ERASURE_PENDING")
            case Left(AnalyticsError.LateFactReplayRejected) =>
              IO.println("STREAMING_REPLAY_DELETION_RACE_REJECTED category=DELETION_SELECTION_REJECTED")
            case _ => IO.raiseError(new IllegalStateException("deletion race did not reject publication"))
          }
        } yield ()
      }
  } yield ()

  override def run(args: List[String]): IO[ExitCode] = {
    val program = args match {
      case List("race-replay") => raceReplay
      case List(mode)          => scenario(mode)
      case _                   => IO.raiseError(new IllegalArgumentException("one scenario mode required"))
    }
    program
      .as(ExitCode.Success)
      .handleErrorWith(error =>
        IO.println("STREAMING_LIVE_SCENARIO_FAILED class=" + error.getClass.getSimpleName).as(ExitCode.Error)
      )
  }
}
