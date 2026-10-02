package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp, Ref, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.mongodb.client.MongoClients
import io.delta.tables.DeltaTable
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions.{col, lit}
import pureconfig.{ConfigReader, ConfigSource}

import java.time.Instant
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.lang.management.ManagementFactory
import java.util.{Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Synthetic Kafka-to-publication workload. Availability is measured when readers observe durable output. */
object HiringAnalyticsStreamingWorkloadMain extends IOApp {
  private[cli] case class ProducerCredentials(username: String, password: String) {
    override def toString: String = "ProducerCredentials([REDACTED])"
  }
  private[cli] given ConfigReader[ProducerCredentials] =
    ConfigReader.forProduct2("username", "password")(ProducerCredentials.apply)
  private[cli] case class ApiSettings(url: String, nonce: String, password: String) {
    override def toString: String = "ApiSettings([REDACTED])"
  }
  private[cli] given ConfigReader[ApiSettings] =
    ConfigReader.forProduct3("url", "nonce", "password")(ApiSettings.apply)
  private case class Sample(
      eventId: String,
      partition: Int,
      offset: Long,
      committedAt: Long,
      bronzeAt: Option[Long] = None,
      reportAt: Option[Long] = None
  )
  private case class Observations(maxBacklog: Int = 0, maxObserverHeapBytes: Long = 0L)
  private val pollInterval = 2.seconds
  private val drainTimeout = 180.seconds

  private[cli] def storage(
      settings: com.example.hiring.analytics.config.AnalyticsStreamingRuntimeSettings
  ): (Long, Long) = {
    val roots = Vector(
      Paths.get(URI.create(settings.common.lakehouseRoot)),
      Paths.get(URI.create(settings.streaming.checkpointLocation)),
      Paths.get(settings.common.sparkLocalDirectory)
    )
    roots
      .filter(Files.exists(_))
      .map { root =>
        val entries = Files.walk(root)
        try
          entries.iterator().asScala.filter(Files.isRegularFile(_)).foldLeft((0L, 0L)) { case ((count, bytes), path) =>
            require(count < 100000L, "proof storage inventory must remain bounded")
            val size = try Files.size(path)
            catch { case _: java.nio.file.NoSuchFileException => 0L }
            (count + 1L, bytes + size)
          }
        finally entries.close()
      }
      .foldLeft((0L, 0L)) { case ((files, bytes), (moreFiles, moreBytes)) => (files + moreFiles, bytes + moreBytes) }
  }

  private def query(api: ApiSettings, token: String): org.bson.Document = {
    val uri = URI.create(api.url)
    require(uri.getScheme == "http" && Set("127.0.0.1", "localhost").contains(uri.getHost), "local proof API required")
    val day = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.DAYS)
    val operation = s"""{ analyticsReport(from: "${day.minusSeconds(86400)}", to: "${day.plusSeconds(
        86400
      )}") { __typename asOf skillPostingActivity { skill postings } } }"""
    val body = new org.bson.Document("query", operation).toJson
    val request = HttpRequest
      .newBuilder(URI.create(api.url.stripSuffix("/") + "/graphql"))
      .header("Content-Type", "application/json")
      .header("Authorization", "Bearer " + token)
      .timeout(java.time.Duration.ofSeconds(15))
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    require(response.statusCode() == 200, "proof GraphQL endpoint must respond")
    org.bson.Document.parse(response.body())
  }

  private def forbidden(body: org.bson.Document): Boolean = {
    val errors = Option(body.getList("errors", classOf[org.bson.Document])).toVector.flatMap(_.asScala)
    val result = Option(body.get("data", classOf[org.bson.Document])).flatMap(data =>
      Option(data.get("analyticsReport", classOf[org.bson.Document]))
    )
    result.exists(_.getString("code") == "UNAUTHORIZED") || errors.exists(error =>
      Option(error.get("extensions", classOf[org.bson.Document])).exists(_.getString("code") == "UNAUTHORIZED")
    )
  }

  private[cli] def login(api: ApiSettings, role: String): String = {
    val uri = URI.create(api.url)
    require(uri.getScheme == "http" && Set("127.0.0.1", "localhost").contains(uri.getHost), "local proof API required")
    require(api.nonce.matches("[a-f0-9]{16}"), "isolated proof account nonce required")
    val input = new org.bson.Document("idempotencyKey", UUID.randomUUID().toString)
      .append("name", s"Proof $role ${api.nonce}")
      .append("password", api.password)
    val body = new org.bson.Document(
      "query",
      "mutation($input: LoginInput!) { login(input: $input) { __typename ... on AuthSuccess { accessToken } } }"
    )
      .append("variables", new org.bson.Document("input", input))
      .toJson
    val request = HttpRequest
      .newBuilder(URI.create(api.url.stripSuffix("/") + "/graphql"))
      .header("Content-Type", "application/json")
      .timeout(java.time.Duration.ofSeconds(15))
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    require(response.statusCode() == 200, "proof login endpoint must respond")
    val json = org.bson.Document.parse(response.body())
    val result = Option(json.get("data", classOf[org.bson.Document]))
      .flatMap(data => Option(data.get("login", classOf[org.bson.Document])))
      .getOrElse(throw new IllegalStateException("proof login failed"))
    require(result.getString("__typename") == "AuthSuccess", "proof login must succeed")
    Option(result.getString("accessToken"))
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalStateException("proof login token is absent"))
  }

  private def percentile(values: Vector[Long], quantile: Double): Long = {
    require(values.nonEmpty, "latency evidence is empty")
    val sorted = values.sorted
    sorted(math.ceil(values.size * quantile).toInt - 1)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val burst = args == List("burst")
    val records = if (burst) 1000 else 7500
    val cadence = if (burst) Duration.Zero else 120.millis
    if (args.nonEmpty && !burst)
      IO.println("workload accepts optional burst mode and reads validated HOCON").as(ExitCode.Error)
    else
      (for {
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
        _ <- IO.raiseUnless(
          settings.streaming.triggerInterval == 10.seconds && settings.common.sparkMaster == "local[2]"
        )(new IllegalArgumentException("workload requires normal ten-second trigger and local[2] stream"))
        credentials <- IO
          .blocking(ConfigSource.default.at("analytics.workload-producer").load[ProducerCredentials])
          .flatMap(value =>
            IO.fromEither(value.leftMap(_ => new IllegalArgumentException("producer credentials are required")))
          )
        api <- IO
          .blocking(ConfigSource.default.at("analytics.workload-api").load[ApiSettings])
          .flatMap(value =>
            IO.fromEither(value.leftMap(_ => new IllegalArgumentException("authenticated proof API settings required")))
          )
        _ <- IO.raiseUnless(
          api.nonce.matches("[a-f0-9]{16}") && settings.common.mongoDatabase == "hiring_streaming_proof_" + api.nonce
        )(new IllegalArgumentException("workload API nonce must match the isolated Mongo namespace"))
        _ <- IO.blocking {
          require(forbidden(query(api, login(api, "Candidate"))), "Candidate report access must be denied")
          require(forbidden(query(api, login(api, "Recruiter"))), "Recruiter report access must be denied")
        }
        initialAdminToken <- IO.blocking(login(api, "Admin"))
        adminToken <- Ref.of[IO, (String, Long)]((initialAdminToken, System.currentTimeMillis()))
        topic = AnalyticsTopic.unwrap(settings.topic)
        connection = settings.common.kafka.copy(
          saslUsername = Some(credentials.username),
          saslPassword = Some(credentials.password)
        )
        properties <- IO.fromEither(KafkaClientProperties.clientProperties(connection))
        configuration = new Properties()
        _ <- IO {
          configuration.put("bootstrap.servers", connection.bootstrapServers)
          properties.foreach { case (key, value) => configuration.put(key, value) }
          configuration.put("key.serializer", classOf[StringSerializer].getName)
          configuration.put("value.serializer", classOf[StringSerializer].getName)
          configuration.put("acks", "all")
          configuration.put("enable.idempotence", "true")
          configuration.put("transactional.id", "hiring-publisher-workload-" + UUID.randomUUID().toString)
        }
        _ <- Resource.make(IO.blocking(Admin.create(configuration)))(admin => IO.blocking(admin.close())).use { admin =>
          IO.blocking {
            val partitions = admin
              .describeTopics(List(topic).asJava)
              .allTopicNames()
              .get(30, java.util.concurrent.TimeUnit.SECONDS)
              .get(topic)
              .partitions()
              .size()
            require(partitions == 3, "workload requires three partitions")
          }
        }
        samples <- Ref.of[IO, Vector[Sample]](Vector.empty)
        observations <- Ref.of[IO, Observations](Observations())
        storageBefore <- IO.blocking(storage(settings))
        productionDuration <- Ref.of[IO, FiniteDuration](Duration.Zero)
        nonce = UUID.randomUUID().toString.replace("-", "")
        skill = "workload" + nonce
        subjects = Vector.fill(12)(UUID.randomUUID().toString)
        started <- IO.monotonic
        _ <- Resource
          .make(IO.blocking(new KafkaProducer[String, String](configuration)))(producer =>
            IO.blocking(producer.close())
          )
          .use { producer =>
            Resource
              .make(IO.blocking(MongoClients.create(settings.common.mongoUri)))(client => IO.blocking(client.close()))
              .use { mongo =>
                AppModule
                  .sparkMongo[IO](
                    settings.common.mongoUri,
                    "local[1]",
                    "hiring-streaming-workload-observer",
                    sparkLocalDirectory = settings.common.sparkLocalDirectory + "/observer"
                  )
                  .use { case (spark, _, execution) =>
                    val bronze = settings.common.lakehouseRoot.stripSuffix("/") + "/bronze/operational_events"
                    val silver = settings.common.lakehouseRoot.stripSuffix("/") + "/silver/operational_events"
                    val decisions = settings.common.lakehouseRoot.stripSuffix("/") + "/control/streaming_decisions"
                    val progress = settings.common.lakehouseRoot.stripSuffix("/") + "/control/streaming_progress"
                    def observe: IO[Unit] = for {
                      existing <- samples.get
                      coordinates <- execution {
                        if (!DeltaTable.isDeltaTable(spark, bronze)) Set.empty[(Int, Long)]
                        else {
                          val minimum = existing.groupBy(_.partition).toVector.map { case (partition, values) =>
                            partition -> values.map(_.offset).min
                          }
                          val predicate = minimum.foldLeft(lit(false)) { case (condition, (partition, offset)) =>
                            condition || (col("partition") === partition && col("offset") >= offset)
                          }
                          spark.read
                            .format("delta")
                            .load(bronze)
                            .filter(col("topic") === topic && predicate)
                            .select("partition", "offset")
                            .limit(records * 2)
                            .collect()
                            .toVector
                            .map(row => (row.getInt(0), row.getLong(1)))
                            .toSet
                        }
                      }
                      snapshot <- IO.blocking {
                        val report = mongo
                          .getDatabase(settings.common.mongoDatabase)
                          .getCollection("analytics_report_snapshots")
                          .find(new org.bson.Document("_id", "current").append("state", "Published"))
                          .first()
                        Option(report)
                      }
                      currentToken <- adminToken.get.flatMap { case (token, issuedAt) =>
                        if (System.currentTimeMillis() - issuedAt < 300000L) IO.pure(token)
                        else
                          IO.blocking(login(api, "Admin")).flatTap { refreshed =>
                            adminToken.set((refreshed, System.currentTimeMillis()))
                          }
                      }
                      publicSnapshot <- IO
                        .blocking(query(api, currentToken))
                        .map(body =>
                          Option(body.get("data", classOf[org.bson.Document]))
                            .flatMap(data => Option(data.get("analyticsReport", classOf[org.bson.Document])))
                            .filter(_.getString("__typename") == "AnalyticsReport")
                        )
                      publishedIds <- execution {
                        val matched = for {
                          stored <- snapshot
                          visible <- publicSnapshot
                          if Instant.parse(visible.getString("asOf")) == stored.getDate("asOf").toInstant
                          if DeltaTable.isDeltaTable(spark, decisions) && DeltaTable.isDeltaTable(
                            spark,
                            progress
                          ) && DeltaTable.isDeltaTable(spark, silver)
                        } yield {
                          val rows = spark.read
                            .format("delta")
                            .load(decisions)
                            .filter(
                              col("publicationRunId") === stored.getString("runId") &&
                                col("publicationGeneration") === stored.getLong("generation").longValue() &&
                                col("publicationRevision") === stored.getLong("revision").longValue()
                            )
                            .join(
                              spark.read.format("delta").load(progress).filter(col("outcome") === "Published"),
                              Seq("lineage", "batchId")
                            )
                            .select("sourceEndOffsets")
                            .limit(2)
                            .collect()
                          require(rows.length <= 1, "published report must bind exactly one stream decision")
                          val ends = rows.headOption.toVector
                            .flatMap(_.getSeq[org.apache.spark.sql.Row](0))
                            .filter(_.getAs[String]("topic") == topic)
                            .map(row => row.getAs[Int]("partition") -> row.getAs[Long]("endOffset"))
                            .toMap
                          val present = spark.read
                            .format("delta")
                            .load(silver)
                            .filter(org.apache.spark.sql.functions.array_contains(col("jobSkills"), skill))
                            .select("eventId")
                            .limit(records + 1)
                            .collect()
                            .map(_.getString(0))
                            .toSet
                          require(present.size <= records, "workload Silver selection must stay bounded")
                          val covered = existing.filter(sample =>
                            present(sample.eventId) && ends.get(sample.partition).exists(_ > sample.offset)
                          )
                          val count =
                            Option(visible.getList("skillPostingActivity", classOf[org.bson.Document])).toVector
                              .flatMap(_.asScala)
                              .filter(_.getString("skill") == skill)
                              .map(_.get("postings", classOf[java.lang.Number]).longValue())
                              .sum
                          if (count >= covered.size.toLong) covered.map(_.eventId).toSet else Set.empty[String]
                        }
                        matched.getOrElse(Set.empty[String])
                      }
                      observed <- IO.realTime.map(_.toMillis)
                      _ <- samples.update(_.map { sample =>
                        sample.copy(
                          bronzeAt = sample.bronzeAt
                            .orElse(Option.when(coordinates((sample.partition, sample.offset)))(observed)),
                          reportAt = sample.reportAt.orElse(Option.when(publishedIds(sample.eventId))(observed))
                        )
                      })
                      current <- samples.get
                      heap <- IO(ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed)
                      _ <- observations.update(value =>
                        Observations(
                          math.max(value.maxBacklog, current.count(_.bronzeAt.isEmpty)),
                          math.max(value.maxObserverHeapBytes, heap)
                        )
                      )
                    } yield ()
                    def drained: IO[Unit] = samples.get.flatMap { values =>
                      if (values.forall(value => value.bronzeAt.nonEmpty && value.reportAt.nonEmpty)) IO.unit
                      else IO.sleep(pollInterval) *> IO.defer(drained)
                    }
                    val observer: IO[Unit] = (observe *> IO.sleep(pollInterval)).foreverM
                    observer.background
                      .use { joined =>
                        IO.race(
                          joined.flatMap(_.embedNever),
                          for {
                            _ <- IO.blocking(producer.initTransactions())
                            productionStarted <- IO.monotonic
                            _ <- (0 until records).toVector.traverse_ { index =>
                              for {
                                current <- IO.monotonic
                                due = productionStarted + cadence * index.toLong
                                _ <- IO.sleep((due - current).max(Duration.Zero))
                                committed <- IO.blocking {
                                  val eventId = UUID.randomUUID().toString
                                  val at = Instant.now().toString
                                  val actor = subjects(index % subjects.size)
                                  val body =
                                    s"""{"eventId":"$eventId","eventType":"JOB_CREATED","occurredAt":"$at","aggregateType":"Job","aggregateId":"$eventId","actorId":"$actor","payload":{"job":{"skills":["$skill"]}}}"""
                                  producer.beginTransaction()
                                  val metadata = producer
                                    .send(new ProducerRecord[String, String](topic, Int.box(index % 3), eventId, body))
                                    .get(30, java.util.concurrent.TimeUnit.SECONDS)
                                  producer.commitTransaction()
                                  Sample(eventId, metadata.partition(), metadata.offset(), System.currentTimeMillis())
                                }
                                _ <- samples.update(_ :+ committed)
                              } yield ()
                            }
                            _ <- IO.sleep(cadence)
                            productionEnded <- IO.monotonic
                            _ <- productionDuration.set(productionEnded - productionStarted)
                            _ <- IO.blocking {
                              // An untimed control forces a final stream publication after any coincident maintenance tick.
                              val eventId = UUID.randomUUID().toString
                              val body = s"""{"eventId":"$eventId","eventType":"JOB_CREATED","occurredAt":"${Instant
                                  .now()}","aggregateType":"Job","aggregateId":"$eventId","actorId":"${subjects.head}","payload":{"job":{"skills":["control"]}}}"""
                              producer.beginTransaction()
                              producer
                                .send(new ProducerRecord[String, String](topic, eventId, body))
                                .get(30, java.util.concurrent.TimeUnit.SECONDS)
                              producer.commitTransaction()
                            }
                            _ <- drained.timeoutTo(drainTimeout, IO.unit)
                          } yield ()
                        ).void
                      }
                      .flatTap(_ =>
                        execution {
                          if (DeltaTable.isDeltaTable(spark, bronze))
                            println(
                              "STREAMING_WORKLOAD_BRONZE_OBSERVER_PLAN\n" + spark.read
                                .format("delta")
                                .load(bronze)
                                .filter(col("topic") === topic)
                                .select("partition", "offset")
                                .queryExecution
                                .executedPlan
                                .toString
                            )
                        }
                      )
                  }
              }
          }
        elapsed <- IO.monotonic.map(_ - started)
        productionElapsed <- productionDuration.get
        measured <- samples.get
        observation <- observations.get
        storageAfter <- IO.blocking(storage(settings))
        bronzeValues = measured.flatMap(sample => sample.bronzeAt.map(_ - sample.committedAt))
        reportValues = measured.flatMap(sample => sample.reportAt.map(_ - sample.committedAt))
        bronzeP95 <- IO(percentile(bronzeValues, 0.95))
        reportP95 <- IO(percentile(reportValues, 0.95))
        _ <- IO.println(
          s"STREAMING_WORKLOAD mode=${
              if (burst) "burst" else "healthy"
            } records=$records untimedControlRecords=1 partitions=3 targetRecordsPerMinute=${
              if (burst) "unpaced" else "500"
            } productionSeconds=${productionElapsed.toSeconds} elapsedSeconds=${elapsed.toSeconds} bronzeSamples=${bronzeValues.size} reportSamples=${reportValues.size} bronzeP95Millis=$bronzeP95 reportP95Millis=$reportP95 observerSpark=local[1] streamSpark=local[2] clock=real"
        )
        _ <- IO.println(
          s"STREAMING_WORKLOAD_OBSERVATIONS maxObservedUnmaterializedRecords=${observation.maxBacklog} maxObserverHeapBytes=${observation.maxObserverHeapBytes} storageFilesBefore=${storageBefore._1} storageFilesAfter=${storageAfter._1} storageBytesBefore=${storageBefore._2} storageBytesAfter=${storageAfter._2} streamProcessResources=externalMeasurementRequired"
        )
        _ <- IO.raiseUnless(
          observation.maxBacklog <= (if (burst) 1000 else 500) &&
            storageAfter._1 - storageBefore._1 <= 10000L &&
            storageAfter._2 - storageBefore._2 <= 1073741824L
        )(new IllegalStateException("streaming lag or storage growth bound exceeded"))
        _ <- IO.println(
          "STREAMING_WORKLOAD_BOUNDS unmaterializedRecordsMax=" + (if (burst) 1000
                                                                   else 500) + " storageGrowthBytesMax=1073741824 storageGrowthFilesMax=10000"
        )
        _ <- IO.raiseUnless(
          bronzeValues.size == records && reportValues.size == records &&
            (burst || (productionElapsed >= 900.seconds && productionElapsed <= 905.seconds && bronzeP95 < 30000 && reportP95 <= 120000))
        )(new IllegalStateException("streaming freshness acceptance failed"))
      } yield ExitCode.Success).handleErrorWith(error =>
        IO.println(s"streaming workload failed (${error.getClass.getSimpleName})").as(ExitCode.Error)
      )
  }
}
