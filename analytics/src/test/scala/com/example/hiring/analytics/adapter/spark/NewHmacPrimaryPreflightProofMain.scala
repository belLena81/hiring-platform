package com.example.hiring.analytics.adapter.spark

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.mongo.{
  AnalyticsMongoRecords,
  MongoAnalyticsLakehouseLock,
  MongoHmacKeyRetirementAuthorizationStore,
  MongoPublisherStream
}
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.cli.HmacRetirementProofCalendar
import com.example.hiring.analytics.config.{AnalyticsBatchSettings, AnalyticsRuntimeConfig}
import com.example.hiring.analytics.domain.{AnalyticsDigest, AnalyticsTopic, RunId, SubjectPseudonymizer}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
import com.mongodb.ReadConcern
import com.mongodb.client.model.Projections
import io.delta.tables.DeltaTable
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lit}
import org.bson.Document

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, LinkOption, Path}
import java.sql.Timestamp

/** Read-only key preflight for a nonce-bound, already-authorized fixture. Never invokes batch publication or mutation.
  */
object NewHmacPrimaryPreflightProofMain extends IOApp {
  private def category(error: Throwable): String = error match {
    case AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is malformed") =>
      "AUTHORIZATION_MALFORMED"
    case AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is invalid or its key is primary") =>
      "AUTHORIZATION_INVALID_OR_PRIMARY"
    case AnalyticsError.InvalidConfiguration("stored analytical rows require an HMAC key that is not configured") =>
      "STORED_TOKEN_KEY_MISMATCH"
    case AnalyticsError.InvalidConfiguration("retained analytical data has no versioned subject tokens") =>
      "MISSING_SUBJECT_TOKEN_COLUMN"
    case AnalyticsError.InvalidConfiguration(
          "unexpired analytical rows use a different HMAC key; retain the old primary until their retention expires"
        ) =>
      "PRIMARY_TOKEN_MISMATCH"
    case AnalyticsError.InvalidConfiguration(message)
        if message.startsWith("HMAC key material changed without a new key ID:") =>
      "MATERIAL_MISMATCH"
    case AnalyticsError.InvalidConfiguration(message)
        if message.startsWith("HMAC key '") && message.endsWith(
          "cannot be removed without durable cleanup and writer-exclusion authorization"
        ) =>
      "OMISSION_AUTHORIZATION_MISSING"
    case _: AnalyticsError.InvalidConfiguration => "INVALID_CONFIGURATION"
    case _: AnalyticsError                      => "ANALYTICS_ERROR"
    case _                                      => "UNEXPECTED_ERROR"
  }

  private def observed[A](stage: String)(action: IO[A]): IO[A] =
    action.onError { case error =>
      IO.println(
        s"NEW_PRIMARY_PREFLIGHT_FAILED stage=$stage category=${category(error)} class=${error.getClass.getSimpleName}"
      )
    }

  private def completedProof(
      settings: AnalyticsBatchSettings,
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      spark: SparkSession,
      database: MongoDatabase[IO],
      execution: SparkExecution[IO],
      streams: MongoPublisherStream,
      authorizations: Vector[HmacKeyRetirementAuthorization],
      store: MongoHmacKeyRetirementAuthorizationStore[IO]
  ): IO[Unit] = {
    val invalid = AnalyticsError.InvalidConfiguration("isolated completed retirement evidence is invalid")
    for {
      runId <- IO.fromEither(
        sys.env
          .get("HIRING_HMAC_ROTATION_COMPLETED_RUN_ID")
          .toRight(invalid)
          .flatMap(value => RunId.from(value).leftMap(_ => invalid))
      )
      range <- IO.fromOption(
        settings.manifest.offsetRanges.headOption.filter(_ => settings.manifest.offsetRanges.size == 1)
      )(invalid)
      _ <- IO.raiseUnless(
        range.endOffsetExclusive > range.startOffset && range.endOffsetExclusive - range.startOffset == 1L
      )(invalid)
      controlToken <- IO.fromEither(
        pseudonymizer.typedToken("084c58fe-787b-410b-b1bb-7193931f06e3").leftMap(_ => invalid)
      )
      capturedCount <- observed("COMPLETED_DELTA_FILES")(execution.either {
        val fixture = spark.read
          .format("delta")
          .load(SparkPhysicalLocation.resolve(paths.root.stripSuffix("/") + "/control/hmac_retirement_fixture"))
        val references = fixture
          .filter(col("stage") === "old-primary-physical-path")
          .select("reference")
          .limit(1001)
          .collect()
          .toVector
          .map(row => if (row.isNullAt(0)) "" else row.getString(0))
        val silverRoot = Path.of(URI.create(paths.silver)).toAbsolutePath.normalize()
        for {
          _ <- Either.cond(
            references.nonEmpty && references.size <= 1000 && references.distinct.size == references.size,
            (),
            invalid
          )
          captured <- references.traverse { reference =>
            val uri = URI.create(reference)
            if (uri.getScheme != "file" || uri.getAuthority != null || uri.normalize() != uri) Left(invalid)
            else {
              val path = Path.of(uri).toAbsolutePath.normalize()
              Either.cond(
                path.startsWith(silverRoot) && path != silverRoot &&
                  (path.getFileName.toString.endsWith(".parquet") ||
                    (path.getParent == silverRoot.resolve("_delta_log") && path.getFileName.toString
                      .endsWith(".json"))),
                path,
                invalid
              )
            }
          }
          _ <- Either.cond(
            captured.exists(_.getFileName.toString.endsWith(".parquet")) &&
              captured.exists(_.getFileName.toString.endsWith(".json")) && !captured.exists(
                Files.exists(_, LinkOption.NOFOLLOW_LINKS)
              ),
            (),
            invalid
          )
          staged = fixture
            .filter(col("stage") === "new-primary-control-staged")
            .select("observedAt")
            .limit(2)
            .collect()
            .toVector
          stagedAt <- staged match {
            case Vector(row) if !row.isNullAt(0) => Right(row.getTimestamp(0))
            case _                               => Left(invalid)
          }
          silver = spark.read.format("delta").load(SparkPhysicalLocation.resolve(paths.silver))
          _ <- Either.cond(
            silver
              .filter(
                col("eventType") === "APPLICATION_CREATED" && col("subjectToken") === controlToken.value &&
                  col("occurredAt") === stagedAt && col("ingestedAt") === stagedAt
              )
              .limit(2)
              .count() == 1L,
            (),
            invalid
          )
          bronze = spark.read
            .format("delta")
            .load(SparkPhysicalLocation.resolve(paths.bronze))
            .filter(
              col("topic") === AnalyticsTopic.unwrap(range.topic) &&
                col("partition") === range.partition && col("offset") === range.startOffset
            )
            .select("eventId", "eventType")
            .limit(2)
            .collect()
            .toVector
          eventId <- bronze match {
            case Vector(row) if !row.isNullAt(0) && row.getString(1) == "JOB_CREATED" => Right(row.getString(0))
            case _                                                                    => Left(invalid)
          }
          _ <- Either.cond(
            silver
              .filter(
                col("eventId") === eventId && col("eventType") === "JOB_CREATED" &&
                  col("subjectToken") === controlToken.value
              )
              .limit(2)
              .count() == 1L,
            (),
            invalid
          )
          manifests = spark.read
            .format("delta")
            .load(SparkPhysicalLocation.resolve(paths.manifests))
            .filter(
              col("runId") === runId.value &&
                col("topic") === AnalyticsTopic.unwrap(range.topic) && col("partition") === range.partition &&
                col("startOffset") === range.startOffset && col(
                  "endOffsetExclusive"
                ) === range.endOffsetExclusive && col("status") === "PUBLISHED"
            )
          _ <- Either.cond(manifests.limit(2).count() == 1L, (), invalid)
        } yield captured.size
      })
      _ <- IO.println(
        s"NEW_PRIMARY_COMPLETED_PROOF capturedPaths=$capturedCount absent=true originalControlPresent=true sourceRangeSilverMatched=true manifestPublished=true"
      )
      runs <- database
        .withReadConcern(ReadConcern.MAJORITY)
        .getCollection[AnalyticsMongoRecords.ReportRun](
          "analytics_report_runs",
          AnalyticsMongoRecords.reportRunRegistry
        )
      controls <- database
        .withReadConcern(ReadConcern.MAJORITY)
        .getCollection[AnalyticsMongoRecords.ReportControl](
          "analytics_report_control",
          AnalyticsMongoRecords.reportControlRegistry
        )
      snapshots <- database
        .withReadConcern(ReadConcern.MAJORITY)
        .getCollection[AnalyticsMongoRecords.ReportSnapshotMetadata](
          "analytics_report_snapshots",
          AnalyticsMongoRecords.reportSnapshotRegistry
        )
      run <- observed("COMPLETED_MONGO_RUN")(
        streams
          .optional[IO, AnalyticsMongoRecords.ReportRun](
            runs.underlying
              .find(new Document("_id", runId.value))
              .projection(
                Projections
                  .include("_id", "rangeFingerprint", "generation", "revision", "state", "createdAt", "expiresAt")
              )
              .first
          )
          .flatMap(value => IO.fromOption(value)(invalid))
      )
      control <- observed("COMPLETED_MONGO_CONTROL")(
        streams
          .optional[IO, AnalyticsMongoRecords.ReportControl](
            controls.underlying
              .find(new Document("_id", "analytics-report"))
              .projection(
                Projections.include("_id", "generation", "nextRevision", "lastPublishedRevision", "lastRunId", "state")
              )
              .first
          )
          .flatMap(value => IO.fromOption(value)(invalid))
      )
      snapshot <- observed("COMPLETED_MONGO_SNAPSHOT")(
        streams
          .optional[IO, AnalyticsMongoRecords.ReportSnapshotMetadata](
            snapshots.underlying
              .find(new Document("_id", "current").append("state", "Published"))
              .projection(Projections.include("_id", "generation", "revision", "runId", "expiresAt"))
              .first
          )
          .flatMap(value => IO.fromOption(value)(invalid))
      )
      now <- IO.realTimeInstant
      fingerprint = AnalyticsDigest.sha256Hex(
        settings.manifest.offsetRanges
          .sortBy(value => (AnalyticsTopic.unwrap(value.topic), value.partition))
          .map(value =>
            s"${AnalyticsTopic.unwrap(value.topic)}:${value.partition}:${value.startOffset}:${value.endOffsetExclusive}"
          )
          .mkString("\n")
          .getBytes(StandardCharsets.UTF_8)
      )
      _ <- IO.raiseUnless(
        run.state == "Published" && run.rangeFingerprint == fingerprint && control.state == "Published" &&
          control.lastRunId.contains(
            runId.value
          ) && snapshot.runId == runId.value && run.generation == control.generation &&
          run.generation == snapshot.generation && run.revision == control.lastPublishedRevision && run.revision == snapshot.revision &&
          snapshot.expiresAt.exists(_.isAfter(now))
      )(invalid)
      after <- store.list(paths.root)
      _ <- IO.raiseUnless(after == authorizations)(invalid)
      _ <- IO.println(
        "NEW_PRIMARY_COMPLETED_PROOF mongoRunControlSnapshotMatched=true liveSnapshot=true authorizationValidUnchanged=true"
      )
      _ <- IO.println("NEW_PRIMARY_COMPLETED_PROOF_READ_ONLY_PASS")
    } yield ()
  }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      settings <- observed("CONFIGURATION")(AnalyticsRuntimeConfig.loadBatch[IO])
      common = settings.common
      oldKeyId <- IO.fromOption(sys.env.get("HIRING_HMAC_ROTATION_OLD_KEY_ID"))(
        AnalyticsError.InvalidConfiguration("isolated preflight requires its retiring key ID")
      )
      shift <- observed("ISOLATION")(
        IO.fromEither(
          HmacRetirementProofCalendar.shift(
            common.lakehouseRoot,
            common.mongoDatabase,
            settings.manifest.offsetRanges.headOption.map(value => AnalyticsTopic.unwrap(value.topic)).getOrElse(""),
            oldKeyId
          )
        )
      )
      _ <- IO.raiseUnless(
        (args.isEmpty || args == List("completed-proof")) && shift > 0L && common.hmac.previousKeyId.isEmpty &&
          common.hmac.previousSecretBase64.isEmpty && common.hmac.keyId == oldKeyId
            .replace("rotation-old-", "rotation-new-")
      )(
        AnalyticsError.InvalidConfiguration("isolated preflight requires its new-only key configuration")
      )
      pseudonymizer <- IO.fromEither(
        SubjectPseudonymizer
          .validateFromBase64(
            Some(common.hmac.secretBase64),
            common.hmac.keyId,
            None,
            None
          )
          .toEither
          .leftMap(_ => AnalyticsError.InvalidConfiguration("isolated preflight key configuration is invalid"))
      )
      paths <- IO.fromEither(AppModule.resolveLakehousePaths(common.lakehouseRoot))
      _ <- AppModule
        .sparkMongo[IO](
          common.mongoUri,
          common.sparkMaster,
          "hiring-new-key-read-only-preflight",
          sparkUiEnabled = Some(false),
          sparkLocalDirectory = common.sparkLocalDirectory
        )
        .use { case (spark, client, execution) =>
          client.getDatabase(common.mongoDatabase).flatMap { database =>
            val streams = new MongoPublisherStream(common.operational)
            val store = new MongoHmacKeyRetirementAuthorizationStore[IO](database, streams)
            val lock = new MongoAnalyticsLakehouseLock[IO](database, streams)
            lock.resource(paths.root).use { _ =>
              for {
                authorizations <- observed("AUTHORIZATION_READ")(store.list(paths.root))
                _ <- IO.raiseUnless(authorizations.exists(_.keyId == oldKeyId))(
                  AnalyticsError.InvalidConfiguration("isolated preflight retirement authorization is missing")
                )
                _ <- IO.println(
                  s"NEW_PRIMARY_PREFLIGHT stage=AUTHORIZATION_READ valid=true primaryRetired=${authorizations.exists(_.keyId == pseudonymizer.primaryKeyId)}"
                )
                _ <- observed("REGISTRY_DECISION")(execution.either {
                  val exists = DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(paths.hmacKeyRegistry))
                  val rows =
                    if (exists)
                      spark.read
                        .format("delta")
                        .load(SparkPhysicalLocation.resolve(paths.hmacKeyRegistry))
                        .select("keyId", "verifier")
                        .collect()
                        .toVector
                        .map(row => row.getString(0) -> row.getString(1))
                    else Vector.empty
                  val configured = pseudonymizer.keyVerifiers
                  val candidates = configured.filterNot { case (id, _) => rows.exists(_._1 == id) }
                  val storedKeys = candidates.collect {
                    case (id, _) if Vector(paths.bronze, paths.quarantine, paths.silver, paths.lateFacts).exists {
                          path =>
                            if (!DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) false
                            else {
                              val frame = spark.read.format("delta").load(SparkPhysicalLocation.resolve(path))
                              val tokens = Vector(
                                Option.when(frame.columns.contains("subjectTokens"))(
                                  frame.select(explode(col("subjectTokens")).as("token"))
                                ),
                                Option.when(frame.columns.contains("subjectToken"))(
                                  frame.select(col("subjectToken").as("token"))
                                )
                              ).flatten
                              tokens
                                .reduceOption(_.unionByName(_))
                                .exists(_.filter(col("token").startsWith(id + "_")).limit(1).count() > 0L)
                            }
                        } =>
                      id
                  }.toSet
                  val dataExists = Vector(
                    paths.bronze,
                    paths.quarantine,
                    paths.silver,
                    paths.lateFacts,
                    paths.funnelGold,
                    paths.timeToHireGold,
                    paths.skillsGold,
                    paths.manifests
                  ).exists(path => DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path)))
                  MongoAnalyticsLakehouseLock
                    .lockId(paths.root)
                    .flatMap(id =>
                      KeyMaterialContinuityDecision.evaluate(
                        exists,
                        dataExists,
                        rows,
                        configured,
                        storedKeys,
                        authorizations,
                        id,
                        pseudonymizer.primaryKeyId
                      )
                    )
                    .flatMap(added =>
                      Either.cond(
                        added.isEmpty,
                        (),
                        AnalyticsError
                          .InvalidConfiguration("read-only preflight would require new key registry entries")
                      )
                    )
                })
                _ <- IO.println("NEW_PRIMARY_PREFLIGHT stage=REGISTRY_DECISION valid=true registryWritesRequired=false")
                stage = new AnalyticsKeyContinuityStage[IO](paths, pseudonymizer, execution, root => store.list(root))
                _ <- observed("STORED_TOKEN_KEYS")(stage.validateStoredTokenKeys(spark))
                _ <- IO.println("NEW_PRIMARY_PREFLIGHT stage=STORED_TOKEN_KEYS valid=true")
                at <- IO.realTimeInstant
                _ <- Vector("SILVER" -> paths.silver, "LATE_FACTS" -> paths.lateFacts).traverse_ { case (name, path) =>
                  observed("PRIMARY_" + name)(execution {
                    if (!DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) false -> false
                    else {
                      val frame = spark.read.format("delta").load(SparkPhysicalLocation.resolve(path))
                      val present = frame.limit(1).count() > 0L
                      if (present && !frame.columns.contains("subjectToken")) true -> true
                      else if (frame.columns.contains("subjectToken")) {
                        val active =
                          if (frame.columns.contains("expiresAt"))
                            col("expiresAt").isNull || col("expiresAt") > lit(Timestamp.from(at))
                          else lit(true)
                        val incompatible = frame
                          .filter(
                            active && (col("subjectToken").isNull || !col("subjectToken")
                              .startsWith(pseudonymizer.primaryKeyId + "_"))
                          )
                          .limit(1)
                          .count() > 0L
                        present -> incompatible
                      } else present -> false
                    }
                  }).flatMap { case (present, incompatible) =>
                    IO.println(
                      s"NEW_PRIMARY_PREFLIGHT stage=PRIMARY_$name rowsPresent=$present incompatibleLiveRows=$incompatible"
                    ) *>
                      IO.raiseWhen(incompatible)(
                        AnalyticsError.InvalidConfiguration(
                          "unexpired analytical rows use a different HMAC key; retain the old primary until their retention expires"
                        )
                      )
                  }
                }
                _ <- IO.println("NEW_PRIMARY_PREFLIGHT_READ_ONLY_PASS")
                _ <-
                  if (args == List("completed-proof"))
                    completedProof(
                      settings,
                      paths,
                      pseudonymizer,
                      spark,
                      database,
                      execution,
                      streams,
                      authorizations,
                      store
                    )
                  else IO.unit
              } yield ()
            }
          }
        }
    } yield ExitCode.Success).handleErrorWith(error =>
      IO.println(s"NEW_PRIMARY_PREFLIGHT_ABORTED category=${category(error)} class=${error.getClass.getSimpleName}")
        .as(ExitCode.Error)
    )
}
