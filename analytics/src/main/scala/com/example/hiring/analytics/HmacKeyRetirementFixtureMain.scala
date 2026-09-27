package com.example.hiring.analytics

import com.example.hiring.analytics.batch.{AnalyticsLakehousePaths, KafkaConnection}

import cats.effect.{Clock, ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.{ConfigReader, ConfigSource}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import scala.jdk.CollectionConverters.*
import com.mongodb.reactivestreams.client.MongoClients

/** Opt-in local fixture. It preserves the named-volume lakehouse and uses the real wall clock. */
object HmacKeyRetirementFixtureMain extends IOApp {
  private final case class RawFixture(
      lakehouseRoot: Option[String],
      sparkMaster: Option[String],
      oldKeyId: Option[String],
      oldSecretBase64: Option[String],
      newKeyId: Option[String],
      newSecretBase64: Option[String],
      kafka: Option[RawKafka]
  )
  private final case class RawKafka(
      bootstrapServers: Option[String],
      username: Option[String],
      password: Option[String],
      securityProtocol: Option[String],
      allowPlaintext: Option[Boolean],
      topic: Option[String]
  )
  private final case class RawMongo(uri: Option[String], database: Option[String])
  private given ConfigReader[RawMongo] = ConfigReader.forProduct2("uri", "database")(RawMongo.apply)
  private given ConfigReader[RawKafka] = ConfigReader.forProduct6(
    "bootstrap-servers",
    "username",
    "password",
    "security-protocol",
    "allow-plaintext",
    "topic"
  )(RawKafka.apply)
  private given ConfigReader[RawFixture] = ConfigReader.forProduct7(
    "lakehouse-root",
    "spark-master",
    "old-key-id",
    "old-secret-base64",
    "new-key-id",
    "new-secret-base64",
    "kafka"
  )(RawFixture.apply)
  private val logger = Slf4jLogger.getLogger[IO]
  private val OldSubjectId = "d2d01aa7-56e0-43d7-aadc-0a5fd9d26da1"
  private val NewControlSubjectId = "084c58fe-787b-410b-b1bb-7193931f06e3"

  private val silverSchema = StructType(
    Seq(
      StructField("eventId", StringType),
      StructField("eventType", StringType),
      StructField("occurredAt", TimestampType),
      StructField("aggregateType", StringType),
      StructField("aggregateId", StringType),
      StructField("applicationId", StringType),
      StructField("jobId", StringType),
      StructField("newStatus", StringType),
      StructField("jobSkills", ArrayType(StringType)),
      StructField("subjectToken", StringType),
      StructField("subjectTokens", ArrayType(StringType)),
      StructField("eventFingerprint", StringType),
      StructField("expiresAt", TimestampType)
    )
  )

  private def required(name: String, value: Option[String]): Either[AnalyticsError, String] =
    value.filter(_.trim.nonEmpty).toRight(AnalyticsError.InvalidConfiguration(s"$name is required"))

  private def keyRing(raw: RawFixture, oldPrimary: Boolean): Either[AnalyticsError, SubjectPseudonymizer] =
    for {
      oldId <- required("fixture.old-key-id", raw.oldKeyId)
      oldSecret <- required("fixture.old-secret-base64", raw.oldSecretBase64)
      newId <- required("fixture.new-key-id", raw.newKeyId)
      newSecret <- required("fixture.new-secret-base64", raw.newSecretBase64)
      result <- SubjectPseudonymizer
        .validateFromBase64(
          Some(if (oldPrimary) oldSecret else newSecret),
          if (oldPrimary) oldId else newId,
          Some(if (oldPrimary) newId else oldId),
          Some(if (oldPrimary) newSecret else oldSecret)
        )
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
    } yield result

  private def eventRow(token: String, at: Instant): Row = {
    val eventId = UUID.randomUUID().toString
    Row(
      eventId,
      "APPLICATION_CREATED",
      Timestamp.from(at),
      "Application",
      UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      null,
      Seq("scala").asJava,
      token,
      Seq(token).asJava,
      "0" * 64,
      Timestamp.from(at.plusSeconds(30L * 86400L))
    )
  }

  private def fixturePath(paths: AnalyticsLakehousePaths): String =
    paths.root.stripSuffix("/") + "/control/hmac_retirement_fixture"

  private def fixtureRecord(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      at: Instant,
      name: String,
      reference: String = ""
  ): Unit = {
    val schema = StructType(
      Seq(
        StructField("stage", StringType, nullable = false),
        StructField("observedAt", TimestampType, nullable = false),
        StructField("reference", StringType, nullable = false)
      )
    )
    spark
      .createDataFrame(List(Row(name, Timestamp.from(at), reference)).asJava, schema)
      .write
      .format("delta")
      .mode("append")
      .save(fixturePath(paths))
  }

  private def stageTime(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      stage: String
  ): Either[AnalyticsError, Option[Instant]] =
    if (!DeltaTable.isDeltaTable(spark, fixturePath(paths))) Right(None)
    else {
      val rows = spark.read
        .format("delta")
        .load(fixturePath(paths))
        .filter(col("stage") === stage)
        .select("observedAt")
        .limit(2)
        .collect()
      Either.cond(
        rows.length <= 1,
        rows.headOption.map(_.getTimestamp(0).toInstant),
        AnalyticsError.InvalidConfiguration("retirement fixture has duplicate stage records")
      )
    }

  private def captureOldPaths(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      at: Instant
  ): Either[AnalyticsError, Unit] = {
    val root = new Path(paths.silver)
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val files = fs
      .listStatus(root)
      .toVector
      .flatMap { status =>
        if (status.isFile) Vector(status.getPath.toString)
        else if (status.isDirectory && status.getPath.getName == "_delta_log")
          fs.listStatus(status.getPath).toVector.filter(_.isFile).map(_.getPath.toString)
        else Vector.empty
      }
      .filter(path => path.endsWith(".parquet") || path.endsWith(".json"))
    Either.cond(
      files.nonEmpty && files.size <= 1000,
      files.foreach(path => fixtureRecord(spark, paths, at, "old-primary-physical-path", path)),
      AnalyticsError.InvalidConfiguration("old-key fixture physical path evidence is missing or unbounded")
    )
  }

  private def stageOld(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      at: Instant
  ): Either[AnalyticsError, Unit] = {
    Either
      .cond(
        !DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry) && !DeltaTable.isDeltaTable(spark, paths.silver),
        (),
        AnalyticsError.InvalidConfiguration("old-key fixture already exists; preserve its original volume")
      )
      .flatMap { _ =>
        val registrySchema = StructType(
          Seq(
            StructField("keyId", StringType, nullable = false),
            StructField("verifier", StringType, nullable = false)
          )
        )
        spark
          .createDataFrame(pseudonymizer.keyVerifiers.map((id, verifier) => Row(id, verifier)).asJava, registrySchema)
          .write
          .format("delta")
          .mode("errorifexists")
          .save(paths.hmacKeyRegistry)
        val token = pseudonymizer.tokenForNewRows(OldSubjectId)
        spark
          .createDataFrame(List(eventRow(token, at)).asJava, silverSchema)
          .write
          .format("delta")
          .mode("errorifexists")
          .save(paths.silver)
        fixtureRecord(spark, paths, at, "old-primary-silver-staged")
        captureOldPaths(spark, paths, at)
      }
  }

  private def seedNewControl(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      at: Instant
  ): Either[AnalyticsError, Unit] = {
    Either
      .cond(
        DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry) && DeltaTable.isDeltaTable(spark, paths.silver),
        (),
        AnalyticsError.InvalidConfiguration("old-key fixture is missing")
      )
      .map { _ =>
        val token = pseudonymizer.tokenForNewRows(NewControlSubjectId)
        spark
          .createDataFrame(List(eventRow(token, at)).asJava, silverSchema)
          .write
          .format("delta")
          .mode("append")
          .save(paths.silver)
        fixtureRecord(spark, paths, at, "new-primary-control-staged")
        ()
      }
  }

  private def publishOldEvent(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      raw: RawFixture,
      at: Instant
  ): Either[AnalyticsError, Unit] = {
    val settings = for {
      staged <- stageTime(spark, paths, "old-primary-silver-staged")
      published <- stageTime(spark, paths, "old-primary-event-published")
      _ <- Either.cond(
        staged.nonEmpty && published.isEmpty,
        (),
        AnalyticsError.InvalidConfiguration("old-key fixture must be staged and published only once")
      )
      config <- raw.kafka.toRight(
        AnalyticsError.InvalidConfiguration("fixture.kafka settings are required for the old-subject event")
      )
      bootstrap <- required("fixture.kafka.bootstrap-servers", config.bootstrapServers)
      topic <- required("fixture.kafka.topic", config.topic)
      connection <- KafkaConnection
        .validate(
          KafkaConnection(
            bootstrap,
            config.username.filter(_.nonEmpty),
            config.password.filter(_.nonEmpty),
            config.securityProtocol.getOrElse("SASL_SSL"),
            config.allowPlaintext.getOrElse(false)
          )
        )
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
    } yield (connection, topic)
    settings.map { case (connection, topic) =>
      val properties = new Properties()
      properties.setProperty("bootstrap.servers", connection.bootstrapServers)
      properties.setProperty("key.serializer", classOf[StringSerializer].getName)
      properties.setProperty("value.serializer", classOf[StringSerializer].getName)
      properties.setProperty("enable.idempotence", "true")
      properties.setProperty("acks", "all")
      properties.setProperty("delivery.timeout.ms", "30000")
      KafkaConnection.clientProperties(connection).foreach { case (key, value) => properties.setProperty(key, value) }
      val eventId = UUID.randomUUID().toString
      val jobId = UUID.randomUUID().toString
      val payload = s"""{"eventId":"$eventId","eventType":"JOB_CREATED","occurredAt":"$at", """ +
        s""""aggregateType":"Job","aggregateId":"$jobId","actorId":"$OldSubjectId","payload":{"job":{"skills":["Scala"]}}}"""
      val producer = new KafkaProducer[String, String](properties)
      try {
        val metadata = producer.send(new ProducerRecord[String, String](topic, eventId, payload)).get()
        producer.flush()
        fixtureRecord(
          spark,
          paths,
          at,
          "old-primary-event-published",
          s"${metadata.topic()}:${metadata.partition()}:${metadata.offset() + 1L}"
        )
      } finally producer.close()
    }
  }

  private def newEventRange(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths
  ): Either[AnalyticsError, String] = {
    if (!DeltaTable.isDeltaTable(spark, fixturePath(paths)))
      Left(AnalyticsError.InvalidConfiguration("new-primary event range is missing"))
    else {
      val rows = spark.read
        .format("delta")
        .load(fixturePath(paths))
        .filter(col("stage") === "new-primary-event-published")
        .select("reference")
        .limit(2)
        .collect()
      Either
        .cond(
          rows.length == 1,
          rows.head.getString(0),
          AnalyticsError.InvalidConfiguration("new-primary event range is missing or duplicated")
        )
        .flatMap(reference =>
          Either.cond(
            reference.matches("[A-Za-z0-9._-]+:[0-9]+:[0-9]+:[0-9]+"),
            reference,
            AnalyticsError.InvalidConfiguration("new-primary event range is malformed")
          )
        )
    }
  }

  private def printStageStatus(spark: SparkSession, paths: AnalyticsLakehousePaths): Either[AnalyticsError, Unit] =
    for {
      oldRow <- stageTime(spark, paths, "old-primary-silver-staged")
      oldEvent <- stageTime(spark, paths, "old-primary-event-published")
      newControl <- stageTime(spark, paths, "new-primary-control-staged")
      newEvent <- stageTime(spark, paths, "new-primary-event-published")
    } yield {
      println(s"OLD_ROW_STAGED=${oldRow.nonEmpty}")
      println(s"OLD_EVENT_PUBLISHED=${oldEvent.nonEmpty}")
      println(s"NEW_CONTROL_STAGED=${newControl.nonEmpty}")
      println(s"NEW_EVENT_PUBLISHED=${newEvent.nonEmpty}")
    }

  private def publishNewEvent(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      raw: RawFixture,
      at: Instant
  ): Either[AnalyticsError, Unit] = {
    val settings = for {
      staged <- stageTime(spark, paths, "new-primary-control-staged")
      published <- stageTime(spark, paths, "new-primary-event-published")
      _ <- Either.cond(
        staged.nonEmpty && published.isEmpty,
        (),
        AnalyticsError.InvalidConfiguration("new-key control must be staged and event published only once")
      )
      config <- raw.kafka.toRight(
        AnalyticsError.InvalidConfiguration("fixture.kafka settings are required for the new-subject event")
      )
      bootstrap <- required("fixture.kafka.bootstrap-servers", config.bootstrapServers)
      topic <- required("fixture.kafka.topic", config.topic)
      connection <- KafkaConnection
        .validate(
          KafkaConnection(
            bootstrap,
            config.username.filter(_.nonEmpty),
            config.password.filter(_.nonEmpty),
            config.securityProtocol.getOrElse("SASL_SSL"),
            config.allowPlaintext.getOrElse(false)
          )
        )
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
    } yield (connection, topic)
    settings.map { case (connection, topic) =>
      val properties = new Properties()
      properties.setProperty("bootstrap.servers", connection.bootstrapServers)
      properties.setProperty("key.serializer", classOf[StringSerializer].getName)
      properties.setProperty("value.serializer", classOf[StringSerializer].getName)
      properties.setProperty("enable.idempotence", "true")
      properties.setProperty("acks", "all")
      properties.setProperty("delivery.timeout.ms", "30000")
      KafkaConnection.clientProperties(connection).foreach { case (key, value) => properties.setProperty(key, value) }
      val eventId = UUID.randomUUID().toString
      val jobId = UUID.randomUUID().toString
      val payload = s"""{"eventId":"$eventId","eventType":"JOB_CREATED","occurredAt":"$at", """ +
        s""""aggregateType":"Job","aggregateId":"$jobId","actorId":"$NewControlSubjectId","payload":{"job":{"skills":["Scala"]}}}"""
      val producer = new KafkaProducer[String, String](properties)
      try {
        val metadata = producer.send(new ProducerRecord[String, String](topic, eventId, payload)).get()
        producer.flush()
        fixtureRecord(
          spark,
          paths,
          at,
          "new-primary-event-published",
          s"${metadata.topic()}:${metadata.partition()}:${metadata.offset()}:${metadata.offset() + 1L}"
        )
      } finally producer.close()
    }
  }

  private def requireRetirementAuthorization(root: String, oldKeyId: String): IO[Unit] =
    IO.blocking(ConfigSource.default.at("analytics.mongo").load[RawMongo]).flatMap {
      case Right(raw) =>
        for {
          uri <- IO.fromEither(required("analytics.mongo.uri", raw.uri))
          database <- IO.fromEither(required("analytics.mongo.database", raw.database))
          _ <- Resource.fromAutoCloseable(IO.delay(MongoClients.create(uri))).use { client =>
            new MongoHmacKeyRetirementAuthorizationStore(client.getDatabase(database)).list(root).flatMap { rows =>
              IO.raiseUnless(rows.exists(_.keyId == oldKeyId))(
                AnalyticsError.InvalidConfiguration("old-key retirement authorization is not persisted")
              )
            }
          }
        } yield ()
      case Left(_) => IO.raiseError(AnalyticsError.InvalidConfiguration("analytics.mongo settings are missing"))
    }

  /** Performs only elapsed maintenance; Delta's default seven-day VACUUM and 30-day log cleanup are not shortened. */
  private def maintain(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      oldKeyId: String,
      at: Instant
  ): Either[AnalyticsError, Unit] =
    for {
      control <- stageTime(spark, paths, "new-primary-control-staged")
      _ <- Either.cond(
        control.nonEmpty,
        (),
        AnalyticsError.InvalidConfiguration("new-primary control row has not been staged")
      )
      staged <- stageTime(spark, paths, "old-primary-silver-staged")
      stagedAt <- staged.toRight(AnalyticsError.InvalidConfiguration("old-key fixture stage timestamp is missing"))
      _ <- Either.cond(
        !at.isBefore(stagedAt.plusSeconds(30L * 86400L)),
        (),
        AnalyticsError.InvalidConfiguration("old-key Silver retention has not elapsed")
      )
      expiredStage <- stageTime(spark, paths, "old-primary-silver-expired")
      expiredAt <- expiredStage match {
        case Some(value) => Right(value)
        case None        =>
          Either
            .cond(
              DeltaTable.isDeltaTable(spark, paths.silver),
              (),
              AnalyticsError.InvalidConfiguration("old-key Silver table is missing")
            )
            .flatMap { _ =>
              DeltaTable.forPath(spark, paths.silver).delete(col("subjectToken").startsWith(oldKeyId + "_"))
              val remaining = spark.read
                .format("delta")
                .load(paths.silver)
                .filter(col("subjectToken").startsWith(oldKeyId + "_"))
                .limit(1)
                .count()
              Either
                .cond(
                  remaining == 0L,
                  (),
                  AnalyticsError.InvalidConfiguration("old-key Silver rows remain after elapsed cleanup")
                )
                .map { _ =>
                  fixtureRecord(spark, paths, at, "old-primary-silver-expired")
                  at
                }
            }
      }
      reclaimed <- stageTime(spark, paths, "old-primary-data-reclaimed")
      _ <- Right[AnalyticsError, Unit](()).map { _ =>
        if (!at.isBefore(expiredAt.plusSeconds(7L * 86400L)) && reclaimed.isEmpty) {
          DeltaTable.forPath(spark, paths.silver).vacuum().count()
          fixtureRecord(spark, paths, at, "old-primary-data-reclaimed")
        }
      }
      logCleaned <- stageTime(spark, paths, "old-primary-log-cleaned")
      _ <- Right[AnalyticsError, Unit](()).map { _ =>
        if (!at.isBefore(expiredAt.plusSeconds(30L * 86400L)) && logCleaned.isEmpty) {
          val tableIdentifier = paths.silver.replace("`", "``")
          spark.sql(
            s"ALTER TABLE delta.`$tableIdentifier` SET TBLPROPERTIES " +
              s"('analytics.retirementCheckpointNonce' = '${UUID.randomUUID()}')"
          )
          val log = DeltaLog.forTable(spark, paths.silver)
          log.checkpointAndCleanUpDeltaLog(log.update(), None)
          fixtureRecord(spark, paths, at, "old-primary-log-cleaned")
        }
      }
      controlToken = pseudonymizer.tokenForNewRows(NewControlSubjectId)
      controlCount = spark.read
        .format("delta")
        .load(paths.silver)
        .filter(col("subjectToken") === controlToken)
        .limit(1)
        .count()
      _ <- Either.cond(
        controlCount == 1L,
        (),
        AnalyticsError.InvalidConfiguration("new-primary control row was lost during old-key cleanup")
      )
    } yield ()

  override def run(args: List[String]): IO[ExitCode] = args match {
    case List(
          action @ ("stage-old" | "publish-old-event" | "seed-new-control" | "maintain" | "publish-new-event" |
          "new-event-range" | "stage-status")
        ) =>
      (for {
        raw <- IO
          .blocking(ConfigSource.default.at("analytics.key-retirement-fixture").load[RawFixture])
          .flatMap {
            case Right(value) => IO.pure(value)
            case Left(_)      =>
              IO.raiseError[RawFixture](
                AnalyticsError.InvalidConfiguration("key-retirement fixture HOCON is missing or malformed")
              )
          }
        standardRoot <- IO.blocking(ConfigSource.default.at("analytics.lakehouse.root").load[String])
        root <- IO.fromEither(
          required(
            "analytics.lakehouse.root",
            raw.lakehouseRoot.orElse(standardRoot.toOption)
          )
        )
        master <- IO.fromEither(required("fixture.spark-master", raw.sparkMaster))
        pseudonymizer <- IO.fromEither(keyRing(raw, action == "stage-old"))
        _ <-
          if (action == "publish-new-event")
            requireRetirementAuthorization(root, raw.oldKeyId.getOrElse(""))
          else IO.unit
        at <- Clock[IO].realTimeInstant
        _ <- Resource
          .make(
            IO.blocking(
              org.apache.spark.sql.classic.SparkSession
                .builder()
                .appName("hiring-hmac-retirement-fixture")
                .master(master)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()
            )
          )(spark => IO.blocking(spark.stop()))
          .use { spark =>
            IO.blocking {
              val paths = AnalyticsLakehousePaths(root)
              if (action == "stage-old") stageOld(spark, paths, pseudonymizer, at)
              else if (action == "publish-old-event") publishOldEvent(spark, paths, raw, at)
              else if (action == "seed-new-control") seedNewControl(spark, paths, pseudonymizer, at)
              else if (action == "publish-new-event") publishNewEvent(spark, paths, raw, at)
              else if (action == "new-event-range")
                newEventRange(spark, paths).map(range => println("NEW_EVENT_RANGE=" + range))
              else if (action == "stage-status") printStageStatus(spark, paths)
              else maintain(spark, paths, pseudonymizer, raw.oldKeyId.getOrElse(""), at)
            }.flatMap(IO.fromEither(_))
          }
        _ <- logger.info(s"HMAC retirement fixture $action at $at")
      } yield ExitCode.Success).handleErrorWith {
        case error: AnalyticsError => logger.error(error.getMessage).as(ExitCode.Error)
        case error                 =>
          logger
            .error(s"HMAC retirement fixture failed (${error.getClass.getSimpleName})")
            .as(ExitCode.Error)
      }
    case _ =>
      logger
        .error("fixture action must be stage-old, publish-old-event, seed-new-control, or maintain")
        .as(ExitCode.Error)
  }
}
