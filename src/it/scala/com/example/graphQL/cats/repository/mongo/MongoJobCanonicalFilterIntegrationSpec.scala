package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.pagination.{JobPageRequest, PageSize}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.search.*
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, Updates}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*
import scala.util.chaining.*

/** MSC-08 / MSC-10.1: job `skills` and `city` filters compare canonical values; migration 027 backfills them, creates
  * the replacement indexes and drops the superseded ones.
  */
final class MongoJobCanonicalFilterIntegrationSpec extends MongoJobDiscoveryFixture {
  private val Jobs = MongoCollections.Jobs
  private val Step = MigrationIds.JobCanonicalFilters
  private val page = JobPageRequest(Some(JobStatus.Open), None, PageSize.fromInt(50).toOption.get)

  private def init(database: MongoDatabase[IO]): IO[Unit] = MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def jobs(database: MongoDatabase[IO]) = MongoRepositoryTestSupport.collection(database, Jobs)

  private def ledger(database: MongoDatabase[IO]): IO[List[Document]] =
    MongoRepositoryTestSupport
      .collection(database, MongoCollections.HiringMigrationLedger)
      .flatMap(_.find(Filters.empty()).all)
      .map(_.toList)

  private def stepRow(database: MongoDatabase[IO]): IO[Option[Document]] =
    ledger(database).map(_.find(_.getString(MongoFields.Id) == Step.value))

  private def indexNames(database: MongoDatabase[IO]): IO[Set[String]] =
    jobs(database).flatMap(_.listIndexes[Document]).map(_.toList.map(_.getString("name")).toSet)

  private def rejected(result: Either[Throwable, ?]): Boolean = result.left.exists {
    case error: MongoWriteException => error.getError.getCode == 121
    case _                          => false
  }

  private def jobWith(index: Int, skills: Set[String], city: Option[String]): Job = {
    val base = job(index)
    base.copy(skills = skills, location = base.location.copy(city = city))
  }

  /** A database that completed 026 under the earlier binary: no canonical fields, the V2 validator, the display-keyed
    * indexes, no 027/028 proof.
    */
  private def rewindTo026(database: MongoDatabase[IO]): IO[Unit] =
    for {
      setup <- MongoHiringSetup.setupDatabase(database)
      coll <- jobs(database)
      _ <- MongoHiringValidators.install(setup, Jobs, MongoHiringValidators.jobValidatorV2)
      _ <- coll.updateMany(
        Filters.empty(),
        Updates.combine(Updates.unset(MongoFields.SkillsCanonical), Updates.unset(MongoFields.LocationCityCanonical))
      )
      _ <- MongoJobCanonicalFilterMigrations.LegacyIndexNames.zip(List(legacyOpenCity, legacyEmbeddingMeta)).traverse_ {
        case (name, (keys, options)) => coll.createIndex(keys, options.name(name))
      }
      _ <- MongoHiringIndexSetup.jobCanonicalFilterIndexes.traverse_(spec =>
        setup.runCommand(new Document("dropIndexes", Jobs).append("index", spec.options.getName))
      )
      rows <- MongoRepositoryTestSupport.collection(database, MongoCollections.HiringMigrationLedger)
      _ <- rows.deleteMany(Filters.in(MongoFields.Id, Step.value, MigrationIds.BinaryEmbeddingVectors.value))
    } yield ()

  private def legacyOpenCity: (Bson, IndexOptions) =
    (
      Indexes.compoundIndex(
        Indexes.ascending(MongoFields.Status, MongoFields.LocationCity),
        Indexes.descending(MongoFields.CreatedAt, MongoFields.Id)
      ),
      new IndexOptions()
    )

  private def legacyEmbeddingMeta: (Bson, IndexOptions) =
    (
      Indexes.ascending(
        MongoFields.EmbeddingMetaModel,
        MongoFields.Status,
        MongoFields.LocationCity,
        MongoFields.RecruiterId
      ),
      new IndexOptions()
    )

  private def seedLegacy(database: MongoDatabase[IO], count: Int): IO[List[Job]] = {
    val values = (1 to count).toList.map { index =>
      index % 4 match {
        case 0 => jobWith(index, Set("Scala", " Kafka "), Some("Kyiv"))
        case 1 => jobWith(index, Set("scala", "SCALA"), Some(" KYIV"))
        case 2 => jobWith(index, Set("Java"), None)
        case _ => jobWith(index, Set.empty, Some("Lviv"))
      }
    }
    values.traverse_(value => MongoRepositoryTestSupport.insertOne(database, Jobs, MongoHiringCodecs.job(value))) *>
      rewindTo026(database).as(values)
  }

  private def assertBackfilled(database: MongoDatabase[IO], values: List[Job]): IO[Unit] =
    jobs(database).flatMap(_.find(Filters.empty()).all).map { rows =>
      assertEquals(rows.size, values.size)
      rows.foreach { row =>
        val skills = row.getList(MongoFields.Skills, classOf[String]).asScala.toList
        assertEquals(
          row.getList(MongoFields.SkillsCanonical, classOf[String]).asScala.toList,
          skills.map(SearchTerm.canonical),
          clue(row.toJson)
        )
        val location = row.get(MongoFields.Location, classOf[Document])
        assertEquals(
          Option(location.getString(MongoFields.CityCanonical)),
          Option(location.getString(MongoFields.City)).map(SearchTerm.canonical),
          clue(row.toJson)
        )
      }
    }

  test("MSC-08.1 list, discovery and facets match skills and city ignoring case and surrounding spaces") {
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO],
        Some(policy)
      )
      val kyivScala = jobWith(1, Set("Scala", "Kafka"), Some("Kyiv"))
      val upperScala = jobWith(2, Set("scala"), Some("KYIV"))
      val lviv = jobWith(3, Set("Java"), Some("Lviv"))
      def ids(values: List[Job]): Set[JobId] = values.map(_.id).toSet
      for {
        _ <- init(fixture.database)
        _ <- List(kyivScala, upperScala, lviv).traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, Jobs, MongoHiringCodecs.job(value))
        )
        byCity <- success(repository.findOpen(JobSearchFilter(Some(" kyiv "), Set.empty, None), page))
        bySkill <- success(repository.findOpen(JobSearchFilter(None, Set("SCALA"), None), page))
        byBoth <- success(repository.findOpen(JobSearchFilter(Some("kYiV"), Set("scala", " KAFKA"), None), page))
        none <- success(repository.findOpen(JobSearchFilter(Some("kyi"), Set("scala"), None), page))
        nearby <- success(
          repository.nearbyJobs(
            scope,
            NearbyJobsQuery(center, 10d, JobSearchFilter(Some("KYIV"), Set("sCaLa"), None)),
            10
          )
        )
        facets <- success(
          repository.jobDiscoveryFacets(
            scope,
            JobFacetQuery(JobSearchFilter(Some("kyiv"), Set("SCALA"), None), None),
            None,
            None
          )
        )
        _ = assertEquals(ids(byCity), Set(kyivScala.id, upperScala.id))
        _ = assertEquals(ids(bySkill), Set(kyivScala.id, upperScala.id))
        _ = assertEquals(ids(byBoth), Set(kyivScala.id))
        _ = assertEquals(none, Nil)
        _ = assertEquals(nearby.map(_.job.id).toSet, Set(kyivScala.id, upperScala.id))
        // Both spellings count as one city, reported once with the smallest spelling; skills report canonical values.
        _ = assertEquals(facets.cities, List(JobFacetBucket("KYIV", 2L)))
        _ = assertEquals(
          facets.skills.toSet,
          Set(JobFacetBucket("scala", 2L), JobFacetBucket("kafka", 1L))
        )
      } yield ()
    }
  }

  test(
    "MSC-08.1 any-of skills match a job with at least one canonical skill; all-of needs every one; closed jobs never match"
  ) {
    mongoResource.use { fixture =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val both = jobWith(1, Set("Scala", "Kafka"), None)
      val scalaOnly = jobWith(2, Set("Scala"), None)
      val rust = jobWith(3, Set("Rust"), None)
      val closedKafka = jobWith(4, Set("Kafka"), None).copy(state = JobState.Closed(now))
      def ids(values: List[Job]): Set[JobId] = values.map(_.id).toSet
      def search(skills: Set[String], mode: SkillMatchMode) =
        success(repository.findOpen(JobSearchFilter(None, skills, None, mode), page)).map(ids)
      for {
        _ <- init(fixture.database)
        _ <- List(both, scalaOnly, rust, closedKafka).traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, Jobs, MongoHiringCodecs.job(value))
        )
        allOf <- search(Set("Scala", "Kafka"), SkillMatchMode.AllOf)
        anyOf <- search(Set("Scala", "Kafka"), SkillMatchMode.AnyOf)
        spelled <- search(Set(" SCALA ", "kafka"), SkillMatchMode.AnyOf)
        sameSkillTwice <- search(Set("Scala", "scala"), SkillMatchMode.AnyOf)
        unknown <- search(Set("Haskell"), SkillMatchMode.AnyOf)
      } yield {
        assertEquals(allOf, Set(both.id))
        assertEquals(anyOf, Set(both.id, scalaOnly.id))
        assertEquals(spelled, Set(both.id, scalaOnly.id))
        assertEquals(sameSkillTwice, Set(both.id, scalaOnly.id))
        assertEquals(unknown, Set.empty[JobId])
      }
    }
  }

  test(
    "MSC-08.1 a hand-built any-of skills filter on open jobs is answered by an index scan and returns the expected count"
  ) {
    mongoResource.use { fixture =>
      val values = (1 to 12).toList.map { index =>
        jobWith(index, Set(if (index % 2 == 0) "Scala" else "Rust"), None)
          .copy(createdAt = now.plusSeconds(index.toLong))
      }
      val filter = new Document("status", JobStatus.Open.toString)
        .append(MongoFields.SkillsCanonical, new Document("$in", List("scala", "kafka").asJava))
      val command = new Document("find", Jobs)
        .append("filter", filter)
        .append("sort", new Document(MongoFields.CreatedAt, -1).append(MongoFields.Id, -1))
        .append("limit", 3)
      for {
        _ <- init(fixture.database)
        _ <- values.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, Jobs, MongoHiringCodecs.job(value))
        )
        explained <- MongoAccessEvaluationSupport
          .command(fixture.database, new Document("explain", command).append("verbosity", "executionStats"))
        json <- IO.fromEither(io.circe.parser.parse(explained.toJson))
        stages = json.hcursor
          .downField("queryPlanner")
          .downField("winningPlan")
          .focus
          .toList
          .flatMap(plan => plan.findAllByKey("stage") ++ plan.findAllByKey("indexName"))
          .flatMap(_.asString)
        stats = json.hcursor.downField("executionStats")
        _ = println(
          s"MSC-08.1 any-of evidence: keysExamined=${stats.get[Long]("totalKeysExamined")} " +
            s"docsExamined=${stats.get[Long]("totalDocsExamined")} nReturned=${stats.get[Long]("nReturned")} plan=$stages"
        )
      } yield {
        assert(stages.contains("IXSCAN"), stages)
        assertEquals(stats.get[Long]("nReturned"), Right(3L))
      }
    }
  }

  test("MSC-08.1 a content edit keeps the canonical fields in step with the display fields") {
    mongoResource.use { fixture =>
      val original = jobWith(1, Set("Scala"), Some("Kyiv"))
      val edited = original.copy(
        skills = Set("Rust", " Kafka"),
        location = original.location.copy(city = Some("LVIV")),
        updatedAt = original.updatedAt.plusSeconds(5)
      )
      val remote = edited.copy(location = edited.location.copy(city = None))
      def update(value: Job) =
        jobs(fixture.database).flatMap(
          _.updateOne(Filters.eq(MongoFields.Id, value.id.value.toString), MongoJobContentUpdate(value).bson)
        )
      def stored =
        MongoRepositoryTestSupport
          .findOne(fixture.database, Jobs, Filters.eq(MongoFields.Id, original.id.value.toString))
          .map(_.get)
      for {
        _ <- init(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, Jobs, MongoHiringCodecs.job(original))
        _ <- update(edited)
        afterEdit <- stored
        _ <- update(remote)
        afterRemote <- stored
      } yield {
        assertEquals(
          afterEdit.getList(MongoFields.SkillsCanonical, classOf[String]).asScala.toList,
          List("kafka", "rust")
        )
        assertEquals(
          afterEdit.get(MongoFields.Location, classOf[Document]).getString(MongoFields.CityCanonical),
          "lviv"
        )
        assertEquals(
          afterRemote.get(MongoFields.Location, classOf[Document]).containsKey(MongoFields.CityCanonical),
          false
        )
      }
    }
  }

  test("MSC-08.1 jobs validator V3 keeps canonical fields present, paired and consistent in size") {
    mongoResource.use { fixture =>
      def base(id: String) = new Document(MongoFields.Id, id)
        .append(MongoFields.Version, Long.box(0L))
        .append(MongoFields.Skills, List("Scala").asJava)
        .append(MongoFields.SkillsCanonical, List("scala").asJava)
        .append(MongoFields.Location, new Document(MongoFields.Country, "Cyprus"))
      def insert(document: Document) = jobs(fixture.database).flatMap(_.insertOne(document)).attempt
      for {
        _ <- init(fixture.database)
        valid <- insert(base("valid"))
        withCity <- insert(
          base("city").append(
            MongoFields.Location,
            new Document(MongoFields.Country, "Cyprus")
              .append(MongoFields.City, "Kyiv")
              .append(MongoFields.CityCanonical, "kyiv")
          )
        )
        noCanonical <- insert(base("no-canonical").tap(_.remove(MongoFields.SkillsCanonical)))
        sizeMismatch <- insert(base("size").append(MongoFields.SkillsCanonical, List("scala", "kafka").asJava))
        cityUnpaired <- insert(
          base("unpaired").append(
            MongoFields.Location,
            new Document(MongoFields.Country, "Cyprus").append(MongoFields.City, "Kyiv")
          )
        )
        canonicalUnpaired <- insert(
          base("canonical-only").append(
            MongoFields.Location,
            new Document(MongoFields.Country, "Cyprus").append(MongoFields.CityCanonical, "kyiv")
          )
        )
        emptyCanonical <- insert(base("empty").append(MongoFields.SkillsCanonical, List("").asJava))
      } yield {
        assert(valid.isRight && withCity.isRight, clue((valid, withCity)))
        List(noCanonical, sizeMismatch, cityUnpaired, canonicalUnpaired, emptyCanonical).foreach(result =>
          assert(rejected(result), clue(result))
        )
      }
    }
  }

  test("MSC-08.1 a 256-character skill and city whose canonical forms are 512 characters pass the validators") {
    mongoResource.use { fixture =>
      val worst = "İ" * 256
      val value = jobWith(1, Set(worst), Some(worst))
      for {
        _ <- init(fixture.database)
        inserted <- jobs(fixture.database).flatMap(_.insertOne(MongoHiringCodecs.job(value))).attempt
        _ <- init(fixture.database)
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          Jobs,
          Filters.eq(MongoFields.Id, value.id.value.toString)
        )
      } yield {
        assert(inserted.isRight, clue(inserted))
        assertEquals(
          stored.map(_.get(MongoFields.Location, classOf[Document]).getString(MongoFields.CityCanonical).length),
          Some(512)
        )
      }
    }
  }

  test("MSC-10.1 migration 027 backfills a database at 026, swaps the indexes and writes nothing on restart") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        values <- seedLegacy(fixture.database, 1_100)
        beforeNames <- indexNames(fixture.database)
        _ <- init(fixture.database)
        done <- stepRow(fixture.database)
        afterNames <- indexNames(fixture.database)
        _ <- assertBackfilled(fixture.database, values)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        current <- MongoHiringValidators.validatorMatches(setup, Jobs, MongoHiringValidators.jobValidator)
        before <- ledger(fixture.database)
        _ <- init(fixture.database)
        after <- ledger(fixture.database)
      } yield {
        assert(MongoJobCanonicalFilterMigrations.LegacyIndexNames.forall(beforeNames.contains), clue(beforeNames))
        assert(MongoJobCanonicalFilterMigrations.LegacyIndexNames.forall(!afterNames.contains(_)), clue(afterNames))
        assert(
          MongoHiringIndexSetup.jobCanonicalFilterIndexes.forall(spec => afterNames.contains(spec.options.getName))
        )
        assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(done.map(_.containsKey(MongoFields.LastId)), Some(false))
        assert(current)
        assertEquals(after.toSet, before.toSet)
      }
    }
  }

  test("MSC-10.1 two processes starting a database at 026 converge on one backfilled shape") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        values <- seedLegacy(fixture.database, 700)
        _ <- (init(fixture.database), init(fixture.database)).parTupled
        rows <- ledger(fixture.database)
        names <- indexNames(fixture.database)
        _ <- assertBackfilled(fixture.database, values)
      } yield {
        assert(rows.forall(_.getString(MongoFields.State) == "Complete"), clue(rows))
        assert(rows.exists(_.getString(MongoFields.Id) == Step.value))
        assert(MongoJobCanonicalFilterMigrations.LegacyIndexNames.forall(!names.contains(_)), clue(names))
      }
    }
  }

  test("MSC-10.1 migration 027 fails closed with a checkpoint on an unreadable job and resumes once it is gone") {
    mongoResource.use { fixture =>
      // `skills` that is not a list of text cannot be canonicalized; V2 admits it, V3 does not.
      val unreadable = new Document(MongoFields.Id, "zz-unreadable")
        .append(MongoFields.Version, Long.box(0L))
        .append(MongoFields.Skills, List(Int.box(1)).asJava)
        .append(MongoFields.Location, new Document(MongoFields.Country, "Cyprus"))
      for {
        _ <- init(fixture.database)
        values <- seedLegacy(fixture.database, 600)
        coll <- jobs(fixture.database)
        _ <- coll.insertOne(unreadable)
        failed <- init(fixture.database).attempt
        running <- stepRow(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        installedWhileInvalid <- MongoHiringValidators.validatorMatches(
          setup,
          Jobs,
          MongoHiringValidators.jobValidatorV3
        )
        _ <- coll.deleteOne(Filters.eq(MongoFields.Id, "zz-unreadable"))
        _ <- init(fixture.database)
        done <- stepRow(fixture.database)
        _ <- assertBackfilled(fixture.database, values)
      } yield {
        assertEquals(failed, Left(MigrationError.StepFailed(Step, s"found invalid stored data in $Jobs")))
        assertEquals(running.map(_.getString(MongoFields.State)), Some("Running"))
        assert(running.exists(_.containsKey(MongoFields.LastId)), clue(running))
        assert(!installedWhileInvalid)
        assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  test("MSC-10.1 a completed migration 027 reopens when an earlier binary recreates a superseded index") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        coll <- jobs(fixture.database)
        legacy = legacyOpenCity
        _ <- coll.createIndex(legacy._1, legacy._2.name(MongoJobCanonicalFilterMigrations.LegacyIndexNames.head))
        present <- indexNames(fixture.database)
        _ <- init(fixture.database)
        names <- indexNames(fixture.database)
        state <- stepRow(fixture.database)
      } yield {
        assert(present.contains(MongoJobCanonicalFilterMigrations.LegacyIndexNames.head))
        assert(MongoJobCanonicalFilterMigrations.LegacyIndexNames.forall(!names.contains(_)), clue(names))
        assertEquals(state.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  test("MSC-10.1 a reopened migration 027 does not downgrade the installed successor jobs validator") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        installedBefore <- MongoHiringValidators.validatorMatches(setup, Jobs, MongoHiringValidators.jobValidator)
        coll <- jobs(fixture.database)
        legacy = legacyOpenCity
        _ <- coll.createIndex(legacy._1, legacy._2.name(MongoJobCanonicalFilterMigrations.LegacyIndexNames.head))
        reopened <- indexNames(fixture.database)
        _ <- init(fixture.database)
        names <- indexNames(fixture.database)
        state <- stepRow(fixture.database)
        installedAfter <- MongoHiringValidators.validatorMatches(setup, Jobs, MongoHiringValidators.jobValidator)
      } yield {
        assert(installedBefore)
        assert(reopened.contains(MongoJobCanonicalFilterMigrations.LegacyIndexNames.head))
        assert(MongoJobCanonicalFilterMigrations.LegacyIndexNames.forall(!names.contains(_)), clue(names))
        assertEquals(state.map(_.getString(MongoFields.State)), Some("Complete"))
        assert(installedAfter)
      }
    }
  }

  test("MSC-10.1 drift of the jobs validator behind the completed migration 027 fails closed and is not overwritten") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        _ <- setup.runCommand(new Document("collMod", Jobs).append("validator", new Document()))
        drifted <- init(fixture.database).attempt
        stillDrifted <- MongoHiringValidators.validatorMatches(setup, Jobs, MongoHiringValidators.jobValidator)
      } yield {
        assertEquals(drifted, Left(MigrationError.ValidatorMismatch(Jobs)))
        assert(!stillDrifted)
      }
    }
  }

  test("MSC-08.1 the replacement indexes serve the canonical list and discovery filters without a sort stage") {
    mongoResource.use { fixture =>
      val values = (1 to 12).toList.map { index =>
        val base = jobWith(index, Set("Scala"), Some(if (index <= 6) "Kyiv" else "Lviv"))
        base.copy(createdAt = now.plusSeconds(index.toLong))
      }
      val command = new Document("find", Jobs)
        .append("filter", MongoJobRepository.discoveryFilter(JobSearchFilter(Some("KYIV"), Set("SCALA"), None), None))
        .append("sort", new Document(MongoFields.CreatedAt, -1).append(MongoFields.Id, -1))
        .append("limit", 3)
      for {
        _ <- init(fixture.database)
        _ <- values.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, Jobs, MongoHiringCodecs.job(value))
        )
        explained <- MongoAccessEvaluationSupport
          .command(fixture.database, new Document("explain", command).append("verbosity", "executionStats"))
        json <- IO.fromEither(io.circe.parser.parse(explained.toJson))
        stages = json.hcursor
          .downField("queryPlanner")
          .downField("winningPlan")
          .focus
          .toList
          .flatMap(plan => plan.findAllByKey("stage") ++ plan.findAllByKey("indexName"))
          .flatMap(_.asString)
        stats = json.hcursor.downField("executionStats")
        _ = println(
          s"MSC-08.1 evidence: keysExamined=${stats.get[Long]("totalKeysExamined")} " +
            s"docsExamined=${stats.get[Long]("totalDocsExamined")} nReturned=${stats.get[Long]("nReturned")} plan=$stages"
        )
      } yield {
        assert(stages.contains("IXSCAN"), stages)
        assert(stages.contains(MongoIndexNames.JobsOpenCityCreated), stages)
        assert(!stages.contains("SORT"), stages)
        assertEquals(stats.get[Long]("nReturned"), Right(3L))
        assertEquals(stats.get[Long]("totalKeysExamined"), Right(3L))
        assertEquals(stats.get[Long]("totalDocsExamined"), Right(3L))
      }
    }
  }

}
