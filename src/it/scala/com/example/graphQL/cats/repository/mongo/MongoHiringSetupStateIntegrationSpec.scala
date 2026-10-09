package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.hiring.testing.LocalTestServices
import com.mongodb.MongoClientSettings
import com.mongodb.client.model.{Filters, Sorts}
import com.mongodb.event.{CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent}
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.bson.{BsonArray, BsonDocument, BsonString, Document}
import org.bson.json.{JsonMode, JsonWriterSettings}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Whole-setup evidence: the stored shape produced on a fresh database and the restart behaviour of the ledger. */
final class MongoHiringSetupStateIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val ExtendedJson = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).indent(true).build()
  private val SchemaCommandNames =
    Set("create", "collMod", "createIndexes", "dropIndexes", "update", "insert", "delete")
  private val DriverEnvelope = Set("lsid", "$db", "$readPreference", "$clusterTime", "txnNumber", "apiVersion")

  private val ExpectedLedgerIds = List(
    "001_user_job_revisions",
    "002_candidate_search_profile_verification",
    "003_event_outbox_subject_references",
    "004_analytics_report_control",
    "005_analytics_deletion_receipts",
    "006_job_geo_points",
    "007_interview_workflow_storage",
    "008_interview_subject_cleanup",
    "009_interview_inbox_identity",
    "010_interview_workflow_attempts",
    "011_interview_publication_fencing",
    "012_attributable_producer_registrations",
    "013_hiring_workflow_integrity",
    "014_deleted_account_embeddings",
    "015_interview_cleanup_integrity",
    "016_candidate_residence_integrity"
  )

  /** Records schema-changing commands, which the shared access fixture deliberately ignores. */
  private final class SchemaCommands(database: String) extends CommandListener {
    private val observed = new ConcurrentLinkedQueue[BsonDocument]()
    override def commandStarted(event: CommandStartedEvent): Unit =
      if (event.getDatabaseName == database && SchemaCommandNames.contains(event.getCommandName)) {
        // Driver command documents are pooled raw buffers: render them inside the callback, never afterwards.
        val command = BsonDocument.parse(event.getCommand.toJson(ExtendedJson))
        DriverEnvelope.foreach(field => { val _ = command.remove(field) })
        val _ = observed.add(command)
      }
    override def commandSucceeded(event: CommandSucceededEvent): Unit = ()
    override def commandFailed(event: CommandFailedEvent): Unit = ()
    def snapshot: IO[List[BsonDocument]] = IO.delay(observed.iterator().asScala.toList)
    def clear: IO[Unit] = IO.delay(observed.clear())
  }

  private def recording(
      fixture: MongoAccessEvaluationSupport.Fixture
  ): Resource[IO, (MongoDatabase[IO], SchemaCommands)] =
    for {
      name <- Resource.eval(IO.randomUUID.map(id => "hiring_test_" + id.toString.replace("-", "")))
      commands <- Resource.eval(IO.delay(new SchemaCommands(name)))
      settings = MongoClientSettings
        .builder(MongoDatabaseProbe.effectiveSettings(fixture.uri))
        .addCommandListener(commands)
        .build()
      client <- MongoClient.create[IO](settings)
      database <- LocalTestServices.databaseNamed(client, name)
    } yield (database, commands)

  private def ledgerRows(database: MongoDatabase[IO]): IO[List[Document]] =
    Mongo4catsCollections
      .documents(database, MongoCollections.HiringMigrationLedger)
      .flatMap(_.find(Filters.empty()).sort(Sorts.ascending(MongoFields.Id)).all)
      .map(_.toList)

  /** Ledger rows, collection options (validators) and index definitions: the complete stored setup shape. */
  private def storedShape(database: MongoDatabase[IO]): IO[BsonDocument] =
    for {
      ledger <- ledgerRows(database)
      collections <- database.listCollections.map(_.toList.map(_.toBsonDocument))
      names = collections.flatMap(entry => Option(entry.getString("name")).map(_.getValue)).sorted
      indexes <- names.traverse { name =>
        Mongo4catsCollections
          .documents(database, name)
          .flatMap(_.listIndexes[Document])
          .map(values =>
            name -> values.toList.map(index => index.toBsonDocument()).sortBy(_.getString("name").getValue)
          )
      }
    } yield {
      val options = new BsonDocument()
      names.foreach { name =>
        collections.find(_.getString("name").getValue == name).foreach { entry =>
          val _ = options.append(name, Option(entry.getDocument("options", null)).getOrElse(new BsonDocument()))
        }
      }
      val indexDocument = new BsonDocument()
      indexes.foreach { case (name, values) => val _ = indexDocument.append(name, new BsonArray(values.asJava)) }
      new BsonDocument("ledger", new BsonArray(ledger.map(_.toBsonDocument()).asJava))
        .append("collections", new BsonArray(names.map(new BsonString(_)).asJava))
        .append("options", options)
        .append("indexes", indexDocument)
    }

  private def dump(name: String, content: String): IO[Unit] =
    sys.env.get("HIRING_TEST_SETUP_STATE_DUMP").traverse_ { directory =>
      IO.blocking {
        val root = Path.of(directory)
        val _ = Files.createDirectories(root)
        val _ = Files.writeString(root.resolve(name), content, StandardCharsets.UTF_8)
      }
    }

  private def renderCommands(commands: List[BsonDocument]): String =
    commands.map(_.toJson(ExtendedJson)).mkString("\n")

  private def schemaCommands(commands: List[BsonDocument]): List[BsonDocument] =
    commands.filter(command => Set("create", "collMod", "createIndexes", "dropIndexes").contains(command.getFirstKey))

  private def ledgerWrites(commands: List[BsonDocument]): List[BsonDocument] =
    commands.filter(command =>
      Set("update", "insert", "delete").contains(command.getFirstKey) &&
        command.getString(command.getFirstKey).getValue == MongoCollections.HiringMigrationLedger
    )

  test("fresh database setup completes every migration once and records the stored shape") {
    mongoResource.use { fixture =>
      recording(fixture).use { case (database, commands) =>
        for {
          _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
          observed <- commands.snapshot
          shape <- storedShape(database)
          ledger <- ledgerRows(database)
          _ <- dump("fresh-state.json", shape.toJson(ExtendedJson))
          _ <- dump("fresh-schema-commands.json", renderCommands(schemaCommands(observed)))
          _ <- dump("fresh-ledger-writes.json", renderCommands(ledgerWrites(observed)))
        } yield {
          assertEquals(ledger.map(_.getString(MongoFields.Id)), ExpectedLedgerIds)
          ledger.foreach { row =>
            assertEquals(row.get(MongoFields.Version), Long.box(1L), clues(row.toJson))
            assertEquals(row.getString(MongoFields.State), "Complete", clues(row.toJson))
            assert(!row.containsKey(MongoFields.LastId), clues(row.toJson))
          }
          assert(schemaCommands(observed).exists(_.getFirstKey == "collMod"))
          assert(schemaCommands(observed).exists(_.getFirstKey == "createIndexes"))
        }
      }
    }
  }

  test("two processes starting the same fresh database converge on one completed shape") {
    mongoResource.use { fixture =>
      recording(fixture).use { case (database, _) =>
        for {
          _ <- (
            MongoHiringSetup.initialize(database, Diagnostics.noop),
            MongoHiringSetup.initialize(database, Diagnostics.noop)
          ).parTupled
          shape <- storedShape(database)
          ledger <- ledgerRows(database)
          _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
          restarted <- storedShape(database)
        } yield {
          assertEquals(ledger.map(_.getString(MongoFields.Id)), ExpectedLedgerIds)
          assert(ledger.forall(_.getString(MongoFields.State) == "Complete"))
          assertEquals(restarted.toJson(ExtendedJson), shape.toJson(ExtendedJson))
        }
      }
    }
  }

  test("a step failing mid-way leaves its proof Running and every later start reports the typed step error") {
    mongoResource.use { fixture =>
      recording(fixture).use { case (database, _) =>
        val invalidPoint =
          new Document("type", "Point").append("coordinates", java.util.List.of(Double.box(200d), Double.box(0d)))
        val job = new Document(MongoFields.Id, "job-invalid-point")
          .append(MongoFields.Version, Long.box(0L))
          .append(MongoFields.Location, new Document(MongoFields.Point, invalidPoint))
        for {
          jobs <- Mongo4catsCollections.documents(database, MongoCollections.Jobs)
          _ <- jobs.insertOne(job)
          first <- MongoHiringSetup.initialize(database, Diagnostics.noop).attempt
          proof <- ledgerRows(database).map(_.find(_.getString(MongoFields.Id) == MigrationIds.JobGeoPoints.value))
          second <- MongoHiringSetup.initialize(database, Diagnostics.noop).attempt
          _ <- jobs.updateOne(
            Filters.eq(MongoFields.Id, job.getString(MongoFields.Id)),
            com.mongodb.client.model.Updates.set(
              MongoFields.LocationPoint,
              new Document("type", "Point").append("coordinates", java.util.List.of(Double.box(20d), Double.box(0d)))
            )
          )
          _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
          repaired <- ledgerRows(database).map(_.find(_.getString(MongoFields.Id) == MigrationIds.JobGeoPoints.value))
        } yield {
          val expected =
            MigrationError.StepFailed(MigrationIds.JobGeoPoints, "found an invalid point at job job-invalid-point")
          assertEquals(first, Left(expected))
          assertEquals(second, Left(expected))
          assertEquals(proof.map(_.getString(MongoFields.State)), Some("Running"))
          assertEquals(proof.map(_.get(MongoFields.Version)), Some(Long.box(1L)))
          assertEquals(repaired.map(_.getString(MongoFields.State)), Some("Complete"))
        }
      }
    }
  }

  test("validator drift behind a completed proof fails closed with the drifted collection and is not overwritten") {
    mongoResource.use { fixture =>
      recording(fixture).use { case (database, _) =>
        for {
          _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
          setup <- MongoHiringSetup.setupDatabase(database)
          _ <- setup.runCommand(new Document("collMod", MongoCollections.Users).append("validator", new Document()))
          ledgerBefore <- ledgerRows(database)
          drifted <- MongoHiringSetup.initialize(database, Diagnostics.noop).attempt
          ledgerAfterDrift <- ledgerRows(database)
          stillDrifted <- MongoHiringValidators.userValidatorMatches(setup)
          _ <- MongoHiringValidators.createUserValidator(setup)
          corrupt <- Mongo4catsCollections
            .documents(database, MongoCollections.HiringMigrationLedger)
            .flatMap(
              _.updateOne(
                Filters.eq(MongoFields.Id, MigrationIds.UserJobRevisions.value),
                com.mongodb.client.model.Updates.set(MongoFields.Version, Int.box(1))
              )
            ) *> MongoHiringSetup.initialize(database, Diagnostics.noop).attempt
          ledgerAfterCorrupt <- ledgerRows(database)
        } yield {
          assertEquals(ledgerAfterDrift.size, 16)
          assert(ledgerAfterDrift.forall(_.getString(MongoFields.State) == "Complete"))
          assertEquals(ledgerAfterDrift, ledgerBefore)
          val revisions = ledgerAfterCorrupt.find(_.getString(MongoFields.Id) == MigrationIds.UserJobRevisions.value)
          assertEquals(revisions.map(_.get(MongoFields.Version)), Some(Int.box(1): AnyRef))
          assertEquals(
            ledgerAfterCorrupt.filterNot(_.getString(MongoFields.Id) == MigrationIds.UserJobRevisions.value),
            ledgerBefore.filterNot(_.getString(MongoFields.Id) == MigrationIds.UserJobRevisions.value)
          )
          assertEquals(drifted, Left(MigrationError.ValidatorMismatch(MongoCollections.Users)))
          assert(!stillDrifted)
          assertEquals(
            corrupt,
            Left(MigrationError.LedgerCorrupt(MigrationIds.UserJobRevisions, "unsupported version"))
          )
        }
      }
    }
  }

  test("restart on a completed ledger re-verifies without re-running or rewriting any migration") {
    mongoResource.use { fixture =>
      recording(fixture).use { case (database, commands) =>
        for {
          _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
          before <- storedShape(database)
          _ <- commands.clear
          _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
          observed <- commands.snapshot
          after <- storedShape(database)
          _ <- dump("restart-state.json", after.toJson(ExtendedJson))
          _ <- dump("restart-schema-commands.json", renderCommands(schemaCommands(observed)))
          _ <- dump("restart-ledger-writes.json", renderCommands(ledgerWrites(observed)))
        } yield {
          assertEquals(after.toJson(ExtendedJson), before.toJson(ExtendedJson))
          assertEquals(ledgerWrites(observed).map(_.toJson(ExtendedJson)), Nil)
          assertEquals(
            schemaCommands(observed).filter(_.getFirstKey == "createIndexes").map(_.toJson(ExtendedJson)),
            Nil
          )
        }
      }
    }
  }
}
