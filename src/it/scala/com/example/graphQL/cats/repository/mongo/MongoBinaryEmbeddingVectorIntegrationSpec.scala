package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, InsertManyOptions, Updates}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import scala.jdk.CollectionConverters.*
import scala.util.Random
import scala.util.chaining.*
import java.util.UUID

/** MSC-09 / MSC-10.1: embeddings are stored as float32 BSON binary vectors; migration 028 converts float64 arrays. */
final class MongoBinaryEmbeddingVectorIntegrationSpec extends MongoJobDiscoveryFixture {
  private val Users = MongoCollections.Users
  private val Jobs = MongoCollections.Jobs
  private val Step = MigrationIds.BinaryEmbeddingVectors
  private val Dimensions = 8

  private def init(database: MongoDatabase[IO]): IO[Unit] = MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def collection(database: MongoDatabase[IO], name: String) =
    MongoRepositoryTestSupport.collection(database, name)

  private def ledger(database: MongoDatabase[IO]): IO[List[Document]] =
    collection(database, MongoCollections.HiringMigrationLedger).flatMap(_.find(Filters.empty()).all).map(_.toList)

  private def stepRow(database: MongoDatabase[IO]): IO[Option[Document]] =
    ledger(database).map(_.find(_.getString(MongoFields.Id) == Step.value))

  private def rejected(result: Either[Throwable, ?]): Boolean = result.left.exists {
    case error: MongoWriteException => error.getError.getCode == 121
    case _                          => false
  }

  private def meta(index: Int) = EmbeddingMeta("voyage-4-lite", s"hash-$index", now.plusSeconds(index.toLong))

  /** Elements are exactly representable only as float32, so a lossy conversion would show. */
  private def vector(index: Int, dimensions: Int = Dimensions): List[Float] =
    List.tabulate(dimensions)(d => (index.toFloat * 0.1f) + (d.toFloat / 3f) - 0.7f)

  private def candidate(index: Int, embedding: Option[EntityEmbedding] = None): User =
    User(
      UserId(new UUID(2L, index.toLong)),
      Some(email(s"candidate$index@example.com")),
      s"candidate$index",
      UserRole.Candidate,
      Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
      now,
      embedding = embedding
    )

  private def arrayForm(values: List[Float]): java.util.List[java.lang.Double] =
    values.map(value => java.lang.Double.valueOf(value.toDouble)).asJava

  private def rawEmbedding(database: MongoDatabase[IO], name: String, id: String): IO[AnyRef] =
    MongoRepositoryTestSupport
      .findOne(database, name, Filters.eq(MongoFields.Id, id))
      .map(_.flatMap(row => Option(row.get(MongoFields.Embedding))).getOrElse(fail(s"no embedding on $id")))

  private def typed(database: MongoDatabase[IO], name: String, bsonType: String): IO[Long] =
    MongoRepositoryTestSupport.count(database, name, Filters.`type`(MongoFields.Embedding, bsonType))

  /** A database that completed 027 under the earlier binary: float64 array vectors, users V2, jobs V3, no 028 proof. */
  private def seedLegacy(database: MongoDatabase[IO], count: Int): IO[Unit] =
    for {
      _ <- init(database)
      setup <- MongoHiringSetup.setupDatabase(database)
      _ <- MongoHiringValidators.install(setup, Users, MongoHiringValidators.userValidatorV2)
      _ <- MongoHiringValidators.install(setup, Jobs, MongoHiringValidators.jobValidatorV3)
      _ <- (1 to count).toList.traverse_ { index =>
        MongoRepositoryTestSupport.insertOne(database, Users, MongoHiringCodecs.user(candidate(index))) *>
          MongoRepositoryTestSupport.insertOne(database, Jobs, MongoHiringCodecs.job(job(index)))
      }
      users <- collection(database, Users)
      jobs <- collection(database, Jobs)
      _ <- (1 to count).toList.traverse_ { index =>
        val stored = Updates.combine(
          Updates.set(MongoFields.Embedding, arrayForm(vector(index))),
          Updates.set(
            MongoFields.EmbeddingMeta,
            new Document(MongoFields.Model, meta(index).model)
              .append(MongoFields.SourceHash, meta(index).sourceHash)
              .append(MongoFields.UpdatedAt, java.util.Date.from(meta(index).updatedAt))
          )
        )
        users.updateOne(Filters.eq(MongoFields.Id, candidate(index).id.value.toString), stored) *>
          jobs.updateOne(Filters.eq(MongoFields.Id, job(index).id.value.toString), stored)
      }
      rows <- collection(database, MongoCollections.HiringMigrationLedger)
      _ <- rows.deleteOne(Filters.eq(MongoFields.Id, Step.value))
    } yield ()

  private def assertConverted(database: MongoDatabase[IO], count: Int): IO[Unit] =
    for {
      userArrays <- typed(database, Users, "array")
      jobArrays <- typed(database, Jobs, "array")
      userVectors <- typed(database, Users, "binData")
      jobVectors <- typed(database, Jobs, "binData")
      users <- collection(database, Users).flatMap(_.find(Filters.exists(MongoFields.Embedding)).all)
      jobs <- collection(database, Jobs).flatMap(_.find(Filters.exists(MongoFields.Embedding)).all)
    } yield {
      assertEquals((userArrays, jobArrays), (0L, 0L))
      assertEquals((userVectors, jobVectors), (count.toLong, count.toLong))
      val expectedUsers = (1 to count).map(index => candidate(index).id.value.toString -> vector(index)).toMap
      val expectedJobs = (1 to count).map(index => job(index).id.value.toString -> vector(index)).toMap
      def floats(row: Document) = MongoEmbeddingVectors.decode(row.get(MongoFields.Embedding)).get
      assertEquals(users.map(row => row.getString(MongoFields.Id) -> floats(row)).toMap, expectedUsers)
      assertEquals(jobs.map(row => row.getString(MongoFields.Id) -> floats(row)).toMap, expectedJobs)
    }

  test(
    "MSC-09.1 stored vectors are float32 binData and a vector read back for reuse equals the stored floats exactly"
  ) {
    mongoResource.use { fixture =>
      val users = MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val jobsRepository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val probe = List(0.1f, -0.2f, 1e-30f, 3.4028235e38f, java.lang.Float.MIN_VALUE, 1f / 3f, 123456.789f, 0f)
      val embedding = EntityEmbedding(probe, meta(1))
      val replacement = EntityEmbedding(probe.reverse, meta(2))
      for {
        _ <- init(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          Users,
          MongoHiringCodecs.user(candidate(1, Some(embedding)))
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          Jobs,
          MongoHiringCodecs.job(job(1).copy(embedding = Some(embedding)))
        )
        userRaw <- rawEmbedding(fixture.database, Users, candidate(1).id.value.toString)
        jobRaw <- rawEmbedding(fixture.database, Jobs, job(1).id.value.toString)
        storedUser <- success(users.findVersioned(candidate(1).id))
        storedJob <- success(jobsRepository.findVersioned(job(1).id))
        // The derived-data write is a targeted $set of the binary vector and never advances the content revision.
        _ <- success(users.updateEmbedding(storedUser.get, replacement))
        _ <- success(jobsRepository.updateEmbedding(storedJob.get, replacement))
        userAfter <- success(users.findVersioned(candidate(1).id))
        jobAfter <- success(jobsRepository.findVersioned(job(1).id))
        userBinary <- typed(fixture.database, Users, "binData")
        jobBinary <- typed(fixture.database, Jobs, "binData")
      } yield {
        assertEquals(MongoEmbeddingVectors.decode(userRaw), Some(probe))
        assertEquals(MongoEmbeddingVectors.decode(jobRaw), Some(probe))
        assertEquals(storedUser.flatMap(_.value.embedding), Some(embedding))
        assertEquals(storedJob.flatMap(_.value.embedding), Some(embedding))
        assertEquals(userAfter.flatMap(_.value.embedding), Some(replacement))
        assertEquals(jobAfter.flatMap(_.value.embedding), Some(replacement))
        assertEquals(userAfter.map(_.version), storedUser.map(_.version))
        assertEquals(jobAfter.map(_.version), storedJob.map(_.version))
        assertEquals((userBinary, jobBinary), (1L, 1L))
      }
    }
  }

  test("MSC-09.1 storage: 1024-dimension float32 binary vectors take about a third of the float64 array form") {
    mongoResource.use { fixture =>
      val random = new Random(20261021L)
      val documents = 1_000
      val dimensions = 1_024
      def values = List.fill(dimensions)(random.nextFloat() * 2f - 1f)
      def row(id: Int, embedding: AnyRef) =
        new Document(MongoFields.Id, f"doc-$id%05d").append(MongoFields.Embedding, embedding)
      val vectors = List.fill(documents)(values)
      def stats(setup: MongoHiringSetup.SetupDatabase, name: String) =
        setup.runCommand(new Document("collStats", name)).map { result =>
          (result.getNumber("size").longValue(), result.getNumber("storageSize").longValue())
        }
      for {
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        legacy <- collection(fixture.database, "vector_size_float64_array")
        binary <- collection(fixture.database, "vector_size_float32_binary")
        timings <- vectors.zipWithIndex.grouped(100).toList.traverse { chunk =>
          for {
            array <- legacy
              .insertMany(chunk.map { case (v, id) => row(id, arrayForm(v)) }, new InsertManyOptions())
              .timed
            vector <- binary
              .insertMany(
                chunk.map { case (v, id) => row(id, MongoEmbeddingVectors.encode(v)) },
                new InsertManyOptions()
              )
              .timed
          } yield (array._1.toMillis, vector._1.toMillis)
        }
        _ = {
          def percentile(values: List[Long], fraction: Double): Long =
            values.sorted.apply(math.max(0, math.ceil(values.size * fraction).toInt - 1))
          println(
            s"MSC-09.1 write evidence (100-document insert batches, isolated stack): " +
              s"float64 array p50=${percentile(timings.map(_._1), 0.5)}ms p95=${percentile(timings.map(_._1), 0.95)}ms; " +
              s"float32 binData p50=${percentile(timings.map(_._2), 0.5)}ms p95=${percentile(timings.map(_._2), 0.95)}ms"
          )
        }
        (legacySize, legacyStorage) <- stats(setup, "vector_size_float64_array")
        (binarySize, binaryStorage) <- stats(setup, "vector_size_float32_binary")
        _ = println(
          s"MSC-09.1 storage evidence ($documents docs x $dimensions dims): " +
            s"float64 array size=$legacySize storageSize=$legacyStorage; float32 binData size=$binarySize " +
            s"storageSize=$binaryStorage; size ratio=${binarySize.toDouble / legacySize.toDouble}"
        )
      } yield {
        assert(binarySize * 2 < legacySize, clue((binarySize, legacySize)))
        assert(binaryStorage < legacyStorage, clue((binaryStorage, legacyStorage)))
      }
    }
  }

  test("MSC-09.1 the validators require a binary vector and reject the float64 array form") {
    mongoResource.use { fixture =>
      val vectorValue = MongoEmbeddingVectors.encode(vector(1))
      def userRow(index: Int, embedding: Option[AnyRef]) =
        MongoHiringCodecs.user(candidate(index)).tap(row => embedding.foreach(row.put(MongoFields.Embedding, _)))
      def jobRow(index: Int, embedding: Option[AnyRef]) =
        MongoHiringCodecs.job(job(index)).tap(row => embedding.foreach(row.put(MongoFields.Embedding, _)))
      def insert(name: String, document: Document) =
        collection(fixture.database, name).flatMap(_.insertOne(document)).attempt
      val metaDocument = new Document(MongoFields.Model, "m")
        .append(MongoFields.SourceHash, "h")
        .append(MongoFields.UpdatedAt, java.util.Date.from(now))
      def withMeta(document: Document) = document.tap(_.put(MongoFields.EmbeddingMeta, metaDocument))
      for {
        _ <- init(fixture.database)
        okUser <- insert(Users, withMeta(userRow(1, Some(vectorValue))))
        okJob <- insert(Jobs, withMeta(jobRow(1, Some(vectorValue))))
        arrayUser <- insert(Users, withMeta(userRow(2, Some(arrayForm(vector(2))))))
        arrayJob <- insert(Jobs, withMeta(jobRow(2, Some(arrayForm(vector(2))))))
        textUser <- insert(Users, withMeta(userRow(3, Some("text"))))
        textJob <- insert(Jobs, withMeta(jobRow(3, Some("text"))))
      } yield {
        assert(okUser.isRight && okJob.isRight, clue((okUser, okJob)))
        List(arrayUser, arrayJob, textUser, textJob).foreach(result => assert(rejected(result), clue(result)))
      }
    }
  }

  test("MSC-10.1 migration 028 converts a database at 027 exactly and writes nothing on restart") {
    mongoResource.use { fixture =>
      val count = 1_100
      for {
        _ <- seedLegacy(fixture.database, count)
        _ <- init(fixture.database)
        done <- stepRow(fixture.database)
        _ <- assertConverted(fixture.database, count)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        usersCurrent <- MongoHiringValidators.validatorMatches(setup, Users, MongoHiringValidators.userValidator)
        jobsCurrent <- MongoHiringValidators.validatorMatches(setup, Jobs, MongoHiringValidators.jobValidator)
        before <- ledger(fixture.database)
        _ <- init(fixture.database)
        after <- ledger(fixture.database)
      } yield {
        assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(done.map(_.containsKey(MongoFields.LastId)), Some(false))
        assert(usersCurrent && jobsCurrent)
        assertEquals(after.toSet, before.toSet)
      }
    }
  }

  test("MSC-10.1 two processes starting a database at 027 converge on one converted shape") {
    mongoResource.use { fixture =>
      for {
        _ <- seedLegacy(fixture.database, 700)
        _ <- (init(fixture.database), init(fixture.database)).parTupled
        rows <- ledger(fixture.database)
        _ <- assertConverted(fixture.database, 700)
      } yield assert(rows.forall(_.getString(MongoFields.State) == "Complete"), clue(rows))
    }
  }

  test("MSC-10.1 migration 028 fails closed with a checkpoint on a malformed vector and resumes once it is gone") {
    mongoResource.use { fixture =>
      val count = 600
      // Sorts after every seeded job: the users target finishes first, so the checkpoint names the jobs target.
      val malformed = MongoHiringCodecs.job(job(9_999))
      for {
        _ <- seedLegacy(fixture.database, count)
        jobs <- collection(fixture.database, Jobs)
        _ <- jobs.insertOne(malformed.tap(_.put(MongoFields.Embedding, List("not-a-number").asJava)))
        failed <- init(fixture.database).attempt
        running <- stepRow(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        usersWhileFailed <- MongoHiringValidators.validatorMatches(setup, Users, MongoHiringValidators.userValidator)
        convertedUsers <- typed(fixture.database, Users, "binData")
        _ <- jobs.deleteOne(Filters.eq(MongoFields.Id, malformed.getString(MongoFields.Id)))
        _ <- init(fixture.database)
        done <- stepRow(fixture.database)
        _ <- assertConverted(fixture.database, count)
      } yield {
        assertEquals(failed, Left(MigrationError.StepFailed(Step, s"found a malformed vector in $Jobs")))
        assertEquals(running.map(_.getString(MongoFields.State)), Some("Running"))
        assert(running.exists(_.getString(MongoFields.LastId).startsWith("01:")), clue(running))
        assert(!usersWhileFailed)
        assertEquals(convertedUsers, count.toLong)
        assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  test(
    "MSC-10.1 drift of a successor validator behind the completed migration 028 fails closed and is not overwritten"
  ) {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        _ <- setup.runCommand(new Document("collMod", Users).append("validator", new Document()))
        drifted <- init(fixture.database).attempt
        stillDrifted <- MongoHiringValidators.validatorMatches(setup, Users, MongoHiringValidators.userValidator)
      } yield {
        assertEquals(drifted, Left(MigrationError.ValidatorMismatch(Users)))
        assert(!stillDrifted)
      }
    }
  }

}
