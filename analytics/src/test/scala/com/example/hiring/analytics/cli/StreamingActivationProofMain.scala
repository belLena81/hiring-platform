package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.config.{AnalyticsRuntimeConfig, AnalyticsStreamingRuntimeSettings}
import com.example.hiring.analytics.domain.{
  AnalyticsDigest,
  AnalyticsTopic,
  StreamingActivationAuthorization,
  StreamingActivationIdentity
}
import com.mongodb.client.MongoClients
import io.circe.Json
import io.circe.parser.parse
import org.apache.kafka.clients.admin.Admin
import org.bson.Document

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, Properties}
import scala.jdk.CollectionConverters.*

/** Operator-only test entrypoint: requires current independently produced PASS artifacts before insertion. */
object StreamingActivationProofMain extends IOApp {
  private def sha(bytes: Array[Byte]): String = AnalyticsDigest.sha256Hex(bytes)

  private def repositoryRoot(): Path = {
    val current = Paths.get(".").toAbsolutePath.normalize()
    if (Files.isDirectory(current.resolve("analytics/src"))) current else current.getParent
  }

  private def sourcePaths(): Vector[Path] = {
    val root = repositoryRoot()
    val sources = Vector("src", "analytics/src", "project", "analytics/project", "scripts").flatMap { directory =>
      val path = root.resolve(directory)
      if (!Files.isDirectory(path)) Vector.empty
      else {
        val entries = Files.walk(path)
        try
          entries
            .iterator()
            .asScala
            .filter(file =>
              Files.isRegularFile(file) &&
                !root
                  .relativize(file)
                  .iterator()
                  .asScala
                  .exists(part => Set("target", "__pycache__").contains(part.toString))
            )
            .toVector
        finally entries.close()
      }
    }
    val entries = Files.list(root)
    val compose = try
      entries
        .iterator()
        .asScala
        .filter(path =>
          path.getFileName.toString.startsWith("compose") &&
            path.toString.endsWith(".yaml") && Files.isRegularFile(path)
        )
        .toVector
    finally entries.close()
    (sources ++ compose ++ Vector(root.resolve("build.sbt"), root.resolve("analytics/build.sbt"))).distinct
  }

  private def sourceDigest(): String = {
    val root = repositoryRoot()
    sha(
      sourcePaths()
        .sortBy(_.toString)
        .map(path => root.relativize(path).toString + ":" + sha(Files.readAllBytes(path)))
        .mkString("\n")
        .getBytes(StandardCharsets.UTF_8)
    )
  }

  private def readEvidence(path: Path): (Json, String) = {
    require(!Files.isSymbolicLink(path) && Files.isRegularFile(path), "evidence must be a regular file")
    require(Files.size(path) <= 1024 * 1024, "evidence must be bounded")
    val bytes = Files.readAllBytes(path)
    (
      parse(new String(bytes, StandardCharsets.UTF_8)).fold(problem => throw problem, identity),
      path.toString + "#sha256=" + sha(bytes)
    )
  }

  private def string(json: Json, field: String): String =
    json.hcursor.get[String](field).fold(problem => throw problem, identity)

  private def runtimeIdentity(settings: AnalyticsStreamingRuntimeSettings): IO[StreamingActivationIdentity] =
    IO.fromEither(KafkaClientProperties.clientProperties(settings.common.kafka)).flatMap { properties =>
      IO.blocking {
        val config = new Properties()
        config.put("bootstrap.servers", settings.common.kafka.bootstrapServers)
        properties.foreach { case (key, value) => config.put(key, value) }
        val admin = Admin.create(config)
        try {
          val topic = AnalyticsTopic.unwrap(settings.topic)
          val clusterId = admin.describeCluster().clusterId().get(30, java.util.concurrent.TimeUnit.SECONDS)
          val topicId = admin
            .describeTopics(List(topic).asJava)
            .allTopicNames()
            .get(30, java.util.concurrent.TimeUnit.SECONDS)
            .get(topic)
            .topicId()
            .toString
          settings.streaming
            .activationIdentity(clusterId, topicId, topic, settings.common.lakehouseRoot)
            .fold(problem => throw problem, identity)
        } finally admin.close()
      }
    }

  override def run(args: List[String]): IO[ExitCode] = args match {
    case List("fingerprint") => IO.blocking(sourceDigest()).flatMap(IO.println).as(ExitCode.Success)
    case List("identity")    =>
      AnalyticsRuntimeConfig
        .loadStreaming[IO]
        .flatMap(runtimeIdentity)
        .flatMap(identity => IO.println("IDENTITY_DIGEST=" + sha(identity.canonical.getBytes(StandardCharsets.UTF_8))))
        .as(ExitCode.Success)
    case List("provision", acceptancePath, reviewsDirectory, operatorId, duration) =>
      (for {
        _ <- IO.raiseUnless(operatorId.trim.nonEmpty)(new IllegalArgumentException("operator identity is required"))
        settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
        namespace <- IO.fromEither(
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
          val targets = Vector(
            namespace.resolve("lakehouse"),
            Paths.get(java.net.URI.create(settings.streaming.checkpointLocation)),
            Paths.get(settings.common.sparkLocalDirectory)
          )
          targets.foreach { target =>
            var current: Path = target
            while (current != null) {
              require(!Files.isSymbolicLink(current), "proof namespace must not traverse symlinks")
              current = current.getParent
            }
          }
          require(
            targets.forall(target => !Files.exists(target)),
            "new activation must target unused lakehouse, checkpoint and spill paths"
          )
        }
        seconds <- IO.fromOption(duration.toLongOption.filter(value => value >= 60L && value <= 3600L))(
          new IllegalArgumentException("proof grant must last 60 through 3600 seconds")
        )
        identity <- runtimeIdentity(settings)
        references <- IO.blocking {
          val digest = sourceDigest()
          val expectedIdentityDigest = sha(identity.canonical.getBytes(StandardCharsets.UTF_8))
          val (acceptance, acceptanceReference) = readEvidence(Paths.get(acceptancePath))
          require(string(acceptance, "sourceDigest") == digest, "acceptance source is stale")
          require(string(acceptance, "identityDigest") == expectedIdentityDigest, "acceptance runtime identity differs")
          val scopeFiles =
            acceptance.hcursor.get[Vector[String]]("scopeFiles").fold(problem => throw problem, value => value)
          val sourceFiles = sourcePaths().map(path => repositoryRoot().relativize(path).toString).toSet
          require(
            scopeFiles.nonEmpty && scopeFiles.forall(sourceFiles),
            "acceptance must name current repository source coverage"
          )
          val criteria = acceptance.hcursor.downField("criteria")
          val evidenceFiles = Vector.newBuilder[String]
          (1 to 14).foreach { id =>
            val criterion = criteria.downField(f"HAL-$id%02d")
            require(criterion.get[String]("verdict").contains("PASS"), "all lakehouse criteria must pass")
            val evidence = criterion.get[Vector[Json]]("evidenceFiles").fold(problem => throw problem, value => value)
            require(evidence.nonEmpty, "each acceptance criterion requires concrete evidence files")
            evidence.foreach { record =>
              val path = Paths.get(string(record, "path"))
              require(
                !Files.isSymbolicLink(path) && Files.isRegularFile(path) && Files.size(path) <= 64L * 1024 * 1024,
                "criterion evidence must be a bounded regular file"
              )
              val digest = sha(Files.readAllBytes(path))
              require(string(record, "sha256") == digest, "criterion evidence digest differs")
              evidenceFiles += path.toString + "#sha256=" + digest
            }
          }
          val roles = Vector("CodeReviewer", "SecurityEngineer", "QAEngineer")
          val reviews = roles.flatMap { role =>
            val directory = Paths.get(reviewsDirectory).resolve(role)
            val artifacts = if (Files.isDirectory(directory) && !Files.isSymbolicLink(directory)) {
              val files = Files.list(directory)
              try files.iterator().asScala.filter(_.toString.endsWith(".json")).toVector.sortBy(_.toString)
              finally files.close()
            } else Vector(Paths.get(reviewsDirectory).resolve(role + ".json"))
            require(artifacts.nonEmpty && artifacts.size <= 16, "role review artifacts must be bounded and nonempty")
            val coverages = Vector.newBuilder[(Vector[String], Vector[String], Vector[String])]
            val selected = artifacts.map { artifact =>
              val (review, reference) = readEvidence(artifact)
              require(
                string(review, "role") == role && string(review, "verdict") == "PASS",
                "independent verdict must pass"
              )
              require(string(review, "sourceDigest") == digest, "review source is stale")
              require(string(review, "identityDigest") == expectedIdentityDigest, "review runtime identity differs")
              val coverage =
                review.hcursor.get[Vector[String]]("coveredFiles").fold(problem => throw problem, value => value)
              val excluded =
                review.hcursor.get[Vector[String]]("excludedFiles").fold(problem => throw problem, value => value)
              val authored =
                review.hcursor.get[Vector[String]]("authoredFiles").fold(problem => throw problem, value => value)
              coverages += ((coverage, excluded, authored))
              val reviewer = string(review, "reviewerId")
              require(reviewer.trim.nonEmpty, "independent reviewer identity is required")
              val completed = Instant.parse(string(review, "completedAt"))
              require(
                !completed.isAfter(Instant.now()) && completed.isAfter(Instant.now().minusSeconds(86400)),
                "review must be current"
              )
              (reviewer, reference)
            }
            require(
              StreamingProofIsolation.independentCoverageUnion(scopeFiles, coverages.result()),
              "independent reviews must collectively cover every scoped source without approving authored files"
            )
            selected
          }
          require(reviews.map(_._1).distinct.size >= 2, "activation requires at least two independent reviewers")
          (Vector(acceptanceReference) ++ evidenceFiles.result().distinct, reviews.map(_._2))
        }
        now <- IO.realTimeInstant.map(_.truncatedTo(ChronoUnit.MILLIS))
        authorization <- IO.fromEither(
          StreamingActivationAuthorization
            .fromEvidence(
              identity,
              settings.streaming.activationGrantId,
              now,
              now.plusSeconds(seconds),
              references._1,
              references._2
            )
            .leftMap(new IllegalArgumentException(_))
        )
        _ <- IO.blocking {
          val client = MongoClients.create(settings.common.mongoUri)
          try
            client
              .getDatabase(settings.common.mongoDatabase)
              .getCollection("analytics_streaming_activation")
              .insertOne(
                new Document("_id", authorization.grantId)
                  .append("grantId", authorization.grantId)
                  .append("streamId", identity.streamId)
                  .append("sourceIdentity", identity.sourceIdentity)
                  .append("lakehouseId", identity.lakehouseId)
                  .append("contractFingerprint", identity.contractFingerprint)
                  .append("settingsFingerprint", identity.settingsFingerprint)
                  .append("validFrom", Date.from(authorization.validFrom))
                  .append("expiresAt", Date.from(authorization.expiresAt))
                  .append("evidenceReferences", authorization.evidenceReferences.asJava)
                  .append("independentReviewerReferences", authorization.independentReviewerReferences.asJava)
                  .append("evidenceDigest", authorization.evidenceDigest)
              )
          finally client.close()
        }
        _ <- IO.println("SYNTHETIC_STREAMING_ACTIVATION_PROVISIONED immutableInsert=true")
      } yield ExitCode.Success).handleErrorWith(error =>
        IO.println(s"synthetic activation provisioning failed (${error.getClass.getSimpleName})").as(ExitCode.Error)
      )
    case _ =>
      IO.println(
        "expected fingerprint, identity or provision <acceptance.json> <review-directory> <operator-id> <seconds>"
      ).as(ExitCode.Error)
  }
}
