package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp, Outcome, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.adapter.mongo.{MongoPublisherStream, MongoStreamingActivationGate}
import com.example.hiring.analytics.adapter.spark.SparkPhysicalLocation
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.{AnalyticsRuntimeConfig, AnalyticsStreamingRuntimeSettings}
import com.example.hiring.analytics.domain.{AnalyticsTopic, StreamingActivationIdentity}
import com.example.hiring.analytics.errors.AnalyticsError
import com.mongodb.ReadConcern
import com.mongodb.client.{MongoClients, MongoDatabase}
import io.delta.tables.DeltaTable
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions.col
import org.bson.Document
import pureconfig.{ConfigReader, ConfigSource}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, Paths}
import java.time.Instant
import java.util.{Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Fresh synthetic namespace only. Real production stream/gate and real short immutable grant; no clock overrides. */
object StreamingGrantExpiryProofMain extends IOApp {
  private case class ProducerCredentials(username: String, password: String) {
    override def toString: String = "ProducerCredentials([REDACTED])"
  }
  private given ConfigReader[ProducerCredentials] =
    ConfigReader.forProduct2("username", "password")(ProducerCredentials.apply)
  private case class ApiSettings(url: String, nonce: String, password: String) {
    override def toString: String = "ApiSettings([REDACTED])"
  }
  private given ConfigReader[ApiSettings] = ConfigReader.forProduct3("url", "nonce", "password")(ApiSettings.apply)
  private case class Control(eventId: String, partition: Int, offset: Long)
  private case class Publication(runId: String, generation: Long, revision: Long, asOf: Instant)

  private[cli] def isExpiryFailure(error: Throwable): Boolean = error match {
    case AnalyticsError.InvalidConfiguration("analytics streaming activation grant expired") => true
    case AnalyticsError.InvalidConfiguration(
          "analytics streaming activation is absent, malformed, or does not match this runtime"
        ) =>
      true
    case _ => false
  }

  private def http(api: ApiSettings, body: Document, token: Option[String]): Document = {
    val endpoint = URI.create(api.url.stripSuffix("/") + "/graphql")
    require(
      endpoint.getScheme == "http" && Set("127.0.0.1", "localhost").contains(endpoint.getHost),
      "local proof API required"
    )
    val request = HttpRequest
      .newBuilder(endpoint)
      .timeout(java.time.Duration.ofSeconds(10))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body.toJson))
    token.foreach(value => request.header("Authorization", "Bearer " + value))
    val response =
      HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    require(response.statusCode() == 200, "proof API unavailable")
    Document.parse(response.body())
  }

  private def login(api: ApiSettings): String = {
    val body = new Document(
      "query",
      "mutation($input: LoginInput!) { login(input: $input) { __typename ... on AuthSuccess { accessToken } } }"
    )
      .append(
        "variables",
        new Document(
          "input",
          new Document("idempotencyKey", UUID.randomUUID().toString)
            .append("name", "Proof Admin " + api.nonce)
            .append("password", api.password)
        )
      )
    val result = http(api, body, None).get("data", classOf[Document]).get("login", classOf[Document])
    require(result.getString("__typename") == "AuthSuccess", "proof Admin login required")
    result.getString("accessToken")
  }

  private def visible(api: ApiSettings, token: String, skill: String): Option[Instant] = {
    val day = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.DAYS)
    val operation = s"""{ analyticsReport(from: "${day.minusSeconds(86400)}", to: "${day.plusSeconds(
        86400
      )}") { __typename asOf skillPostingActivity { skill postings } } }"""
    val response = http(api, new Document("query", operation), Some(token))
    Option(response.get("data", classOf[Document]))
      .flatMap(data => Option(data.get("analyticsReport", classOf[Document])))
      .filter(_.getString("__typename") == "AnalyticsReport")
      .filter { report =>
        Option(report.getList("skillPostingActivity", classOf[Document])).toVector
          .flatMap(_.asScala)
          .filter(_.getString("skill") == skill)
          .map(_.get("postings", classOf[java.lang.Number]).longValue())
          .sum == 12L
      }
      .map(report => Instant.parse(report.getString("asOf")))
  }

  private def publication(database: MongoDatabase): Option[Publication] = {
    val row = database
      .getCollection("analytics_report_snapshots")
      .find(new Document("_id", "current").append("state", "Published"))
      .projection(
        new Document("runId", 1).append("generation", 1).append("revision", 1).append("asOf", 1).append("expiresAt", 1)
      )
      .first()
    Option(row)
      .filter(_.getDate("expiresAt").toInstant.isAfter(Instant.now()))
      .map(value =>
        Publication(
          value.getString("runId"),
          value.getLong("generation").longValue(),
          value.getLong("revision").longValue(),
          value.getDate("asOf").toInstant
        )
      )
  }

  private def controlMetadata(database: MongoDatabase): Document =
    database
      .getCollection("analytics_report_control")
      .find(new Document("_id", "analytics-report"))
      .projection(
        new Document("generation", 1)
          .append("nextRevision", 1)
          .append("lastPublishedRevision", 1)
          .append("lastRunId", 1)
          .append("state", 1)
      )
      .first()

  private def publish(producer: KafkaProducer[String, String], topic: String, skill: String): Vector[Control] = {
    producer.beginTransaction()
    val controls = Vector.tabulate(12) { index =>
      val id = UUID.randomUUID().toString
      val actor = UUID.randomUUID().toString
      val body = s"""{"eventId":"$id","eventType":"JOB_CREATED","occurredAt":"${Instant
          .now()}","aggregateType":"Job","aggregateId":"$id","actorId":"$actor","payload":{"job":{"skills":["$skill"]}}}"""
      val result = producer
        .send(new ProducerRecord[String, String](topic, Int.box(index % 3), id, body))
        .get(20, java.util.concurrent.TimeUnit.SECONDS)
      Control(id, result.partition(), result.offset())
    }
    producer.commitTransaction()
    controls
  }

  private def gate(settings: AnalyticsStreamingRuntimeSettings, identity: StreamingActivationIdentity): IO[Instant] =
    AppModule.mongoClient[IO](settings.common.mongoUri).use { mongo =>
      mongo
        .getDatabase(settings.common.mongoDatabase)
        .flatMap(database =>
          new MongoStreamingActivationGate[IO](database, new MongoPublisherStream(settings.common.operational))
            .requireAuthorized(identity, settings.streaming.activationGrantId)
        )
    }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      _ <- IO.raiseUnless(args.isEmpty)(new IllegalArgumentException("expiry proof does not accept arguments"))
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
      _ <- IO.blocking {
        Vector(
          Paths.get(new URI(settings.common.lakehouseRoot)),
          Paths.get(new URI(settings.streaming.checkpointLocation)),
          Paths.get(settings.common.sparkLocalDirectory)
        ).foreach { target =>
          require(!Files.exists(target, LinkOption.NOFOLLOW_LINKS), "expiry proof requires unused storage")
          var ancestor = target
          while (ancestor != null) {
            require(!Files.isSymbolicLink(ancestor), "proof ancestors must not be links"); ancestor = ancestor.getParent
          }
        }
      }
      credentials <- IO
        .blocking(ConfigSource.default.at("analytics.workload-producer").load[ProducerCredentials])
        .flatMap(value => IO.fromEither(value.leftMap(_ => new IllegalArgumentException("producer settings required"))))
      api <- IO
        .blocking(ConfigSource.default.at("analytics.workload-api").load[ApiSettings])
        .flatMap(value => IO.fromEither(value.leftMap(_ => new IllegalArgumentException("API settings required"))))
      _ <- IO.raiseUnless(
        settings.common.mongoDatabase == "hiring_streaming_proof_" + api.nonce && api.nonce.matches("[a-f0-9]{16}")
      )(new IllegalArgumentException("proof API must bind the same nonce"))
      connection = settings.common.kafka.copy(
        saslUsername = Some(credentials.username),
        saslPassword = Some(credentials.password)
      )
      properties <- IO.fromEither(KafkaClientProperties.clientProperties(connection)).map { values =>
        val result = new Properties()
        result.setProperty("bootstrap.servers", connection.bootstrapServers)
        values.foreach { case (key, value) => result.setProperty(key, value) }
        result.setProperty("key.serializer", classOf[StringSerializer].getName)
        result.setProperty("value.serializer", classOf[StringSerializer].getName)
        result.setProperty("acks", "all")
        result.setProperty("enable.idempotence", "true")
        result.setProperty("transactional.id", "hiring-publisher-expiry-" + UUID.randomUUID().toString)
        result
      }
      topic = AnalyticsTopic.unwrap(settings.topic)
      identity <- Resource.make(IO.blocking(Admin.create(properties)))(admin => IO.blocking(admin.close())).use {
        admin =>
          IO.blocking {
            val description = admin
              .describeTopics(List(topic).asJava)
              .allTopicNames()
              .get(20, java.util.concurrent.TimeUnit.SECONDS)
              .get(topic)
            require(description.partitions().size() == 3, "three partitions required")
            settings.streaming
              .activationIdentity(
                admin.describeCluster().clusterId().get(20, java.util.concurrent.TimeUnit.SECONDS),
                description.topicId().toString,
                topic,
                settings.common.lakehouseRoot
              )
              .fold(error => throw error, value => value)
          }
      }
      expiresAt <- gate(settings, identity)
      _ <- (
        Resource.make(IO.blocking(new KafkaProducer[String, String](properties)))(producer =>
          IO.blocking(producer.close())
        ),
        Resource.make(IO.blocking(MongoClients.create(settings.common.mongoUri)))(mongo => IO.blocking(mongo.close()))
      ).tupled.use { case (producer, mongo) =>
        val database = mongo.getDatabase(settings.common.mongoDatabase).withReadConcern(ReadConcern.MAJORITY)
        val skill = "expirybefore" + UUID.randomUUID().toString.replace("-", "")
        def waitVisible(token: String): IO[Unit] = IO.realTimeInstant.flatMap { now =>
          if (!expiresAt.isAfter(now))
            IO.raiseError(new IllegalStateException("positive pre-expiry publication was not observed"))
          else
            IO.blocking(visible(api, token, skill) -> publication(database)).flatMap {
              case (Some(asOf), Some(stored)) if asOf == stored.asOf => gate(settings, identity).void
              case _ => IO.sleep(1.second) *> IO.defer(waitVisible(token))
            }
        }
        for {
          authorization <- IO.blocking(
            database
              .getCollection("analytics_streaming_activation")
              .find(new Document("_id", settings.streaming.activationGrantId))
              .first()
          )
          _ <- IO.blocking {
            require(authorization != null, "immutable grant required")
            val duration = java.time.Duration
              .between(authorization.getDate("validFrom").toInstant, authorization.getDate("expiresAt").toInstant)
              .getSeconds
            require(
              duration >= 60L && duration <= 120L && authorization.getDate("expiresAt").toInstant == expiresAt,
              "short immutable grant required"
            )
          }
          _ <- IO.blocking(producer.initTransactions())
          before <- IO.blocking(publish(producer, topic, skill))
          token <- IO.blocking(login(api))
          _ <- Resource.make(AppModule.streaming[IO](settings).use(_.run).start)(_.cancel).use { running =>
            for {
              _ <- waitVisible(token)
              original <- IO
                .blocking[(org.apache.spark.sql.SparkSession, org.apache.spark.sql.streaming.StreamingQuery)] {
                  val session = org.apache.spark.sql.SparkSession.getDefaultSession
                    .filterNot(_.sparkContext.isStopped)
                    .getOrElse(throw new IllegalStateException("original streaming session absent"))
                  val queries = session.streams.active
                  require(queries.length == 1, "one original active query required")
                  val originalQuery: org.apache.spark.sql.streaming.StreamingQuery = queries.head
                  (session, originalQuery)
                }
              _ <- gate(settings, identity).void
              _ <- IO.println(
                "STREAMING_EXPIRY_PRE_EXPIRY_PUBLISHED realAdminQuery=true records=12 partitions=3 grantLive=true"
              )
              remaining <- IO.realTimeInstant
                .map(now => math.max(1L, java.time.Duration.between(now, expiresAt).toMillis))
              outcome <- running.join.timeout((remaining.millis + 30.seconds).min(150.seconds))
              failure <- outcome match {
                case Outcome.Errored(error) => IO.pure(error)
                case _                      =>
                  IO.raiseError[Throwable](new IllegalStateException("production stream did not terminate with expiry"))
              }
              _ <- IO.raiseUnless(isExpiryFailure(failure))(
                new IllegalStateException("specific expiry failure required")
              )
              _ <- IO.blocking {
                require(
                  original._1.sparkContext.isStopped && !original._2.isActive && original._1.streams.active.isEmpty,
                  "original streaming driver and query survived expiry finalization"
                )
              }
            } yield ()
          }
          _ <- IO.realTimeInstant.flatMap(now =>
            IO.raiseUnless(!expiresAt.isAfter(now))(new IllegalStateException("real expiry has not elapsed"))
          )
          denied <- gate(settings, identity).attempt
          _ <- IO.raiseUnless(denied.left.exists(isExpiryFailure))(
            new IllegalStateException("production callback authorization gate did not deny expiry")
          )
          baseline <- IO.blocking(publication(database) -> controlMetadata(database))
          runCount <- IO.blocking(database.getCollection("analytics_report_runs").countDocuments())
          _ <- IO.raiseUnless(baseline._1.nonEmpty && baseline._2 != null)(
            new IllegalStateException("published baseline required")
          )
          after <- IO.blocking(publish(producer, topic, "expiryafter" + UUID.randomUUID().toString.replace("-", "")))
          restart <- AppModule.streaming[IO](settings).use(_.run).timeout(60.seconds).attempt
          _ <- IO.raiseUnless(restart.left.exists(isExpiryFailure))(
            new IllegalStateException("expired immutable grant restart must be denied")
          )
          _ <- AppModule
            .sparkMongo[IO](
              settings.common.mongoUri,
              "local[1]",
              "hiring-streaming-expiry-observer",
              sparkUiEnabled = Some(false),
              sparkLocalDirectory = settings.common.sparkLocalDirectory + "/expiry-observer"
            )
            .use { case (spark, _, execution) =>
              execution {
                val root = settings.common.lakehouseRoot.stripSuffix("/")
                def countIds(path: String, ids: Vector[String]): Long =
                  if (!DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) 0L
                  else
                    spark.read
                      .format("delta")
                      .load(SparkPhysicalLocation.resolve(path))
                      .filter(col("eventId").isin(ids*))
                      .count()
                require(
                  countIds(root + "/bronze/operational_events", before.map(_.eventId)) == 12L,
                  "pre-expiry Bronze control absent"
                )
                require(
                  countIds(root + "/silver/operational_events", before.map(_.eventId)) == 12L,
                  "pre-expiry Silver control absent"
                )
                require(
                  countIds(root + "/bronze/operational_events", after.map(_.eventId)) == 0L,
                  "post-expiry Bronze write occurred"
                )
                require(
                  countIds(root + "/silver/operational_events", after.map(_.eventId)) == 0L,
                  "post-expiry Silver write occurred"
                )
                val progress =
                  spark.read.format("delta").load(SparkPhysicalLocation.resolve(root + "/control/streaming_progress"))
                val rows = progress.select("sourceEndOffsets").limit(33).collect().toVector
                require(rows.size <= 32, "fresh expiry progress must remain bounded")
                val ends = rows.flatMap(_.getSeq[org.apache.spark.sql.Row](0))
                require(
                  after.forall(control =>
                    !ends.exists(row =>
                      row.getAs[String]("topic") == topic &&
                        row.getAs[Int]("partition") == control.partition && row
                          .getAs[Long]("endOffset") > control.offset
                    )
                  ),
                  "post-expiry source was prepared"
                )
                require(spark.streams.active.isEmpty, "read-only observer must not own queries")
              }
            }
          current <- IO.blocking(publication(database) -> controlMetadata(database))
          unchanged <- IO.blocking(
            database
              .getCollection("analytics_streaming_activation")
              .find(new Document("_id", settings.streaming.activationGrantId))
              .first() == authorization
          )
          locks <- IO.blocking(database.getCollection("analytics_lakehouse_mutexes").countDocuments())
          currentRunCount <- IO.blocking(database.getCollection("analytics_report_runs").countDocuments())
          _ <- IO.raiseUnless(current == baseline && unchanged && locks == 0L && currentRunCount == runCount)(
            new IllegalStateException("post-expiry publication, grant or ownership changed")
          )
          _ <- IO.println(
            "STREAMING_EXPIRY_VERIFIED realClock=true immutableGrantUnchanged=true productionExpiryTerminated=true originalDriverStopped=true originalQueryInactive=true callbackGateDenied=true expiredRestartDenied=true postExpirySourceAbsent=true reportUnchanged=true remainingMutexes=0"
          )
        } yield ()
      }
    } yield ExitCode.Success)
      .timeout(5.minutes)
      .handleErrorWith(error =>
        IO.println(s"streaming expiry proof failed (${error.getClass.getSimpleName})").as(ExitCode.Error)
      )
}
