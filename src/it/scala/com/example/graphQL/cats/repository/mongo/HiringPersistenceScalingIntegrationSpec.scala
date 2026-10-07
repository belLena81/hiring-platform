package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.Queue
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.search.*
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, Updates}
import com.example.graphQL.cats.shared.crypto.SourceHash
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.bson.{BsonDocument, Document}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Opt-in, serialized synthetic measurements on independently owned disposable Mongo namespaces. */
final class HiringPersistenceScalingIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 45.minutes
  override def munitFixtures: List[munit.AnyFixture[?]] =
    if (java.lang.Boolean.getBoolean("hiring.scaling.measure")) super.munitFixtures else Nil
  private val support = MongoAccessEvaluationSupport
  private val now = Instant.parse("2026-10-07T12:00:00Z")
  private val seed = 20261007L
  private val warmup = 100
  private val samples = 500
  private val outboxSize = 2048
  private val center = GeoPoint(35.1856d, 33.3823d)
  private def id(value: String): UUID = UUID.nameUUIDFromBytes(s"$seed:$value".getBytes(StandardCharsets.UTF_8))
  private val recruiter = User(
    UserId(id("recruiter")),
    None,
    "Synthetic scaling recruiter",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
    now
  )
  private val candidate = User(
    UserId(id("candidate")),
    None,
    "Synthetic scaling candidate",
    UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
    now
  )
  private def job(index: Int): Job = {
    val plain = Job(
      JobId(id(s"job:$index")),
      recruiter.id,
      s"Synthetic job $index",
      "Synthetic description " * 200,
      List("Build systems"),
      Set("Scala", s"Skill$index"),
      Location(
        "Cyprus",
        "Nicosia",
        remote = false,
        coordinates = Some(GeoPoint(center.latitude + (index % 20) * 0.00001d, center.longitude))
      ),
      JobStatus.Open,
      now.minusSeconds(index.toLong),
      now
    )
    plain.copy(embedding =
      Some(
        EntityEmbedding(
          List.fill(1024)(0.125f),
          EmbeddingMeta("synthetic-scaling-1024", SourceHash.sha256(SearchableText.job(plain)), now)
        )
      )
    )
  }

  private val jobIds = Vector.tabulate(256)(index => JobId(id(s"job:$index")))

  private final case class Result(completed: Boolean, units: Int = 1, writeMilliseconds: Option[Double] = None)
  private final case class Sample(milliseconds: Double, result: Result)
  private final case class Measurement(report: Json, p95: Double, throughput: Double)
  private final case class OutboxRun(measurement: Measurement, write: Measurement, indexBytes: Long)
  private def requireResult[A](effect: RepositoryIO[A]): IO[A] = effect.value.flatMap {
    case Right(value) => IO.pure(value)
    case Left(error)  => IO.raiseError(new AssertionError(s"Unexpected repository result: $error"))
  }
  private def percentile(values: List[Double], fraction: Double): Double =
    values.sorted.lift((math.ceil(values.size * fraction).toInt - 1).max(0)).getOrElse(0d)
  private def jsonDocument(value: Document): Json = parse(value.toJson).getOrElse(Json.Null)
  private def enabled: Unit = assume(
    java.lang.Boolean.getBoolean("hiring.scaling.measure"),
    "Run explicitly with -Dhiring.scaling.measure=true after serializing competing workloads"
  )

  private def observe(fixture: support.Fixture, concurrency: Int)(operation: Int => IO[Result]): IO[Measurement] = {
    def requests(indices: List[Int]): IO[List[Sample]] = Stream
      .emits(indices)
      .covary[IO]
      .parEvalMapUnordered(concurrency) { index =>
        for {
          start <- IO.monotonic
          result <- operation(index)
          end <- IO.monotonic
        } yield Sample((end - start).toNanos.toDouble / 1000000d, result)
      }
      .compile
      .toList
    for {
      _ <- requests((0 until warmup).toList)
      _ <- fixture.commands.clear
      resourcesBefore <- fixture.sampleResources
      heapBefore <- IO.delay(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory())
      start <- IO.monotonic
      observed <- requests((warmup until warmup + samples).toList)
      end <- IO.monotonic
      resourcesAfter <- fixture.sampleResources
      heapAfter <- IO.delay(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory())
      commands <- fixture.commands.snapshot
      responses <- fixture.commands.responseCounts
      completed = observed.filter(_.result.completed)
      latencies = completed.map(_.milliseconds)
      writeLatencies = completed.flatMap(_.result.writeMilliseconds)
      elapsed = (end - start).toNanos.toDouble / 1000000000d
      throughput = completed.map(_.result.units).sum.toDouble / elapsed
      p95 = percentile(latencies, .95d)
      report = Json.obj(
        "concurrency" -> Json.fromInt(concurrency),
        "warmup" -> Json.fromInt(warmup),
        "attempts" -> Json.fromInt(observed.size),
        "completed" -> Json.fromInt(completed.size),
        "expectedConflictsOrEmpty" -> Json.fromInt(observed.size - completed.size),
        "unexpectedErrors" -> Json.fromInt(0),
        "elapsedSeconds" -> Json.fromDoubleOrNull(elapsed),
        "completedUnitsPerSecond" -> Json.fromDoubleOrNull(throughput),
        "attemptsPerSecond" -> Json.fromDoubleOrNull(observed.size / elapsed),
        "completionP50Ms" -> Json.fromDoubleOrNull(percentile(latencies, .5d)),
        "completionP95Ms" -> Json.fromDoubleOrNull(p95),
        "completionP99Ms" -> Json.fromDoubleOrNull(percentile(latencies, .99d)),
        "writeCompletionSamples" -> Json.fromInt(writeLatencies.size),
        "writeCompletionP95Ms" -> Option
          .when(writeLatencies.nonEmpty)(Json.fromDoubleOrNull(percentile(writeLatencies, .95d)))
          .getOrElse(Json.Null),
        "jvmUsedHeapBytesBefore" -> Json.fromLong(heapBefore),
        "jvmUsedHeapBytesAfter" -> Json.fromLong(heapAfter),
        "heapMeasurement" -> Json.fromString(
          "Used-heap boundary samples, not allocation totals or a controlled GC comparison"
        ),
        "attemptP95Ms" -> Json.fromDoubleOrNull(percentile(observed.map(_.milliseconds), .95d)),
        "responseCounts" -> responses,
        "commands" -> Json.obj(commands.groupBy(commandName).toList.map { case (name, rows) =>
          name -> Json.fromInt(rows.size)
        }*),
        "resourcesBefore" -> resourcesBefore,
        "resourcesAfter" -> resourcesAfter
      )
    } yield Measurement(report, p95, throughput)
  }

  private def commandName(command: BsonDocument): String =
    List(
      "find",
      "aggregate",
      "findAndModify",
      "update",
      "insert",
      "delete",
      "getMore",
      "commitTransaction",
      "abortTransaction"
    )
      .find(command.containsKey)
      .getOrElse("other")

  private def storage(fixture: support.Fixture, collection: String): IO[Document] = for {
    admin <- fixture.client.getDatabase("admin")
    _ <- support.command(admin, new Document("fsync", 1))
    value <- support.command(fixture.database, new Document("collStats", collection))
  } yield value

  private def seedJobs(fixture: support.Fixture): IO[Unit] = for {
    _ <- MongoHiringIndexSetup.create(fixture.database)
    users <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
    _ <- users.insertMany(List(recruiter, candidate).map(MongoHiringCodecs.user(_)))
    jobs <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Jobs)
    _ <- jobs.insertMany((0 until 256).toList.map(index => MongoHiringCodecs.job(job(index))))
  } yield ()

  private def submissionRun(fixture: support.Fixture, projected: Boolean, hot: Boolean, concurrency: Int): IO[Json] = {
    val jobs = MongoJobRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop
    )
    val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
    def operation(index: Int): IO[Result] = {
      val selected = jobIds(if (hot) 0 else index % 256)
      val read =
        if (projected) jobs.findSubmissionSnapshot(selected)
        else
          jobs
            .findVersioned(selected)
            .map(_.map(value => JobSubmissionSnapshot(value.value.id, value.value.status, value.version)))
      for {
        snapshot <- requireResult(read).flatMap(value => IO.fromOption(value)(new AssertionError("Missing seeded job")))
        application = Application.create(
          ApplicationId(id(s"submission:$index")),
          UserId(id(s"applicant:$index")),
          selected,
          now
        )
        event = ApplicationEvent(
          ApplicationEventId(id(s"submission-event:$index")),
          application.id,
          None,
          ApplicationStatus.Created,
          application.candidateId,
          now,
          None,
          None
        )
        writeStart <- IO.monotonic
        result <- applications.createForOpenJob(snapshot, application, event).value
        writeEnd <- IO.monotonic
        outcome <- result match {
          case Right(_) =>
            IO.pure(Result(true, writeMilliseconds = Some((writeEnd - writeStart).toNanos.toDouble / 1000000d)))
          case Left(RepositoryError.Conflict) => IO.pure(Result(false, 0))
          case Left(error) => IO.raiseError(new AssertionError(s"Unexpected submission failure: $error"))
        }
      } yield outcome
    }
    for {
      _ <- IO.println(s"Measuring submission: projection=$projected popularJob=$hot concurrency=$concurrency")
      _ <- seedJobs(fixture)
      readsMetrics <- observe(fixture, concurrency) { index =>
        val selected = jobIds(if (hot) 0 else index % 256)
        (if (projected) requireResult(jobs.findSubmissionSnapshot(selected)).void
         else requireResult(jobs.findVersioned(selected)).void).as(Result(true))
      }
      metrics <- observe(fixture, concurrency)(operation)
      applicationsCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.Applications)
      eventsCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.ApplicationEvents)
      commands <- fixture.commands.snapshot
      reads = commands.filter(command =>
        command.containsKey("find") && command.getString("find").getValue == MongoCollections.Jobs
      )
      first <- requireResult(
        if (projected) jobs.findSubmissionSnapshot(jobIds(0))
        else
          jobs
            .findVersioned(jobIds(0))
            .map(_.map(value => JobSubmissionSnapshot(value.value.id, value.value.status, value.version)))
      )
      _ <- IO(assertEquals(applicationsCount, eventsCount))
      _ <- IO.whenA(hot)(IO(assertEquals(first.map(_.revision), Some(applicationsCount))))
      projectedBsonBytes <- MongoRepositoryTestSupport
        .collection(fixture.database, MongoCollections.Jobs)
        .flatMap(
          _.aggregate[Document](
            List(
              new Document("$project", new Document("_id", 1).append("status", 1).append("version", 1)),
              new Document(
                "$group",
                new Document("_id", null)
                  .append("bytes", new Document("$sum", new Document("$bsonSize", "$$ROOT")))
              )
            )
          ).first
        )
      bsonBytes <- MongoRepositoryTestSupport
        .collection(fixture.database, MongoCollections.Jobs)
        .flatMap(
          _.aggregate[Document](
            List(
              new Document(
                "$group",
                new Document("_id", null)
                  .append("bytes", new Document("$sum", new Document("$bsonSize", "$$ROOT")))
              )
            )
          ).first
        )
    } yield Json.obj(
      "kind" -> Json.fromString(if (hot) "popularJob" else "distributedJobs"),
      "projected" -> Json.fromBoolean(projected),
      "metrics" -> metrics.report,
      "submissionReadMetrics" -> readsMetrics.report,
      "embeddingDimension" -> Json.fromInt(1024),
      "jobReadCommands" -> Json.fromInt(reads.size),
      "observedProjection" -> reads.headOption
        .flatMap(row => Option(row.get("projection")))
        .fold(Json.Null)(value => parse(value.toString).getOrElse(Json.Null)),
      "applicationCountIncludingWarmup" -> Json.fromLong(applicationsCount),
      "historyCountIncludingWarmup" -> Json.fromLong(eventsCount),
      "jobCorpusBsonBytes" -> bsonBytes.fold(Json.Null)(jsonDocument),
      "projectedJobCorpusBsonBytes" -> projectedBsonBytes.fold(Json.Null)(jsonDocument)
    )
  }

  private def claimQuery: Document = new Document("find", MongoCollections.EventOutbox)
    .append(
      "filter",
      new Document(
        "$or",
        List(
          new Document("state", "Retryable").append("availableAt", new Document("$lte", Date.from(now))),
          new Document("state", "InFlight").append("leaseUntil", new Document("$lte", Date.from(now)))
        ).asJava
      )
    )
    .append("sort", new Document("availableAt", 1).append("occurredAt", 1).append("_id", 1))
    .append("projection", new Document("_id", 1).append("availableAt", 1).append("occurredAt", 1))
    .append("limit", 64)

  private def outboxRun(
      fixture: support.Fixture,
      candidateIndexes: Boolean,
      shared: Boolean,
      concurrency: Int
  ): IO[OutboxRun] = {
    val repository =
      MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
    def operation(workers: Queue[IO, String]): IO[Result] = workers.take.bracket { worker =>
      for {
        claimed <- requireResult(
          repository.claim(worker, "hiring-publisher-" + id("generation"), now, now.plusSeconds(60), 1)
        )
        _ <- claimed.traverse_(value =>
          requireResult(
            repository.markPublished(value.event.eventId, value.leaseToken, now, now.plusSeconds(7.days.toSeconds))
          )
        )
      } yield Result(claimed.nonEmpty, claimed.size)
    }(workers.offer)
    for {
      _ <- IO.println(
        s"Measuring outbox: candidateIndexes=$candidateIndexes sharedRecruiter=$shared concurrency=$concurrency"
      )
      _ <- MongoHiringIndexSetup.create(fixture.database)
      collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.EventOutbox)
      documents <- IO.fromEither(
        (0 until outboxSize).toList
          .traverse { index =>
            val application = Application.create(
              ApplicationId(id(s"outbox-application:$index")),
              UserId(id(s"outbox-candidate:$index")),
              JobId(id(s"outbox-job:${index % 256}")),
              now
            )
            val actor = if (shared && index % 4 != 0) recruiter.id else UserId(id(s"outbox-recruiter:$index"))
            val event = ApplicationEvent(
              ApplicationEventId(id(s"outbox-event:$index")),
              application.id,
              Some(ApplicationStatus.Created),
              ApplicationStatus.Accepted,
              actor,
              now.minusMillis(index.toLong),
              None,
              None
            )
            MongoHiringCodecs
              .outboxRecord(OperationalEvents.statusChanged(event.id.value, application, event), now.minusSeconds(1))
              .map { document =>
                if (index % 5 == 0) document.append("state", "Published").append("publishedAt", Date.from(now))
                else if (index % 5 == 1)
                  document
                    .append("state", "InFlight")
                    .append("leaseUntil", Date.from(now.minusSeconds(1)))
                    .append("leaseOwner", "expired-owner")
                    .append("leaseToken", "expired-token")
                else document
              }
          }
          .leftMap(reason => new AssertionError(reason))
      )
      actorIds = documents.map(_.getString("actorId")).toSet
      subjects = documents.flatMap(_.getList("subjectIds", classOf[String]).asScala).distinct.sorted
      subjectUsers = subjects.map { raw =>
        val role = if (actorIds.contains(raw)) UserRole.Recruiter else UserRole.Candidate
        val profile =
          if (role == UserRole.Recruiter)
            UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))
          else UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))
        User(UserId(UUID.fromString(raw)), None, s"Synthetic scaling $role $raw", role, Some(profile), now)
      }
      users <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
      _ <- users.insertMany(subjectUsers.map(MongoHiringCodecs.user(_)))
      _ <- collection.insertMany(documents)
      _ <- IO.whenA(candidateIndexes)(
        collection
          .createIndex(
            Indexes.ascending("availableAt", "occurredAt", "_id"),
            new IndexOptions().name("scaling_retryable_claim").partialFilterExpression(Filters.eq("state", "Retryable"))
          )
          .void *>
          collection
            .createIndex(
              Indexes.ascending("leaseUntil", "availableAt", "occurredAt", "_id"),
              new IndexOptions().name("scaling_expired_claim").partialFilterExpression(Filters.eq("state", "InFlight"))
            )
            .void
      )
      bytes <- storage(fixture, MongoCollections.EventOutbox)
      explain <- support.command(
        fixture.database,
        new Document("explain", claimQuery).append("verbosity", "executionStats")
      )
      write <- indexWriteCost(fixture, documents, candidateIndexes, concurrency)
      workers <- Queue.bounded[IO, String](concurrency)
      _ <- (0 until concurrency).toList.traverse_(index => workers.offer(s"scaling-$index"))
      metrics <- observe(fixture, concurrency)(_ => operation(workers))
      commands <- fixture.commands.snapshot
      claimsAttempted = commands.count(command =>
        command.containsKey("findAndModify") &&
          command.getString("findAndModify").getValue == MongoCollections.EventOutbox
      )
      scannedPages = commands.count(command =>
        command.containsKey("find") &&
          command.getString("find").getValue == MongoCollections.EventOutbox
      )
      activeClaims <- MongoRepositoryTestSupport.count(
        fixture.database,
        MongoCollections.EventOutbox,
        Filters.and(Filters.eq("state", "InFlight"), Filters.regex("leaseOwner", "^scaling-"))
      )
      activeSubjectLeases <- MongoRepositoryTestSupport.count(
        fixture.database,
        MongoCollections.OutboxSubjectFences,
        Filters.exists("leaseToken")
      )
      fencesCount <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.OutboxSubjectFences)
      registrationsCount <- MongoRepositoryTestSupport.count(fixture.database, MongoProducerRegistrations.Collection)
      cursorsCount <- MongoRepositoryTestSupport.count(
        fixture.database,
        MongoProducerRegistrations.ClaimCursorsCollection
      )
      totalRecords =
        subjectUsers.size.toLong + outboxSize + warmup + samples + fencesCount + registrationsCount + cursorsCount
      _ <- IO(assert(totalRecords <= 10000L))
      published <- MongoRepositoryTestSupport.count(
        fixture.database,
        MongoCollections.EventOutbox,
        Filters.eq("state", "Published")
      )
      remaining <- MongoRepositoryTestSupport.count(
        fixture.database,
        MongoCollections.EventOutbox,
        Filters.in("state", "Retryable", "InFlight")
      )
      _ <- IO(assertEquals(activeClaims, 0L))
      _ <- IO(assertEquals(activeSubjectLeases, 0L))
      _ <- IO(assertEquals(published + remaining, outboxSize.toLong))
      backlog <- collection
        .find(Filters.in("state", "Retryable", "InFlight"))
        .sort(com.mongodb.client.model.Sorts.ascending("occurredAt"))
        .limit(1)
        .first
      report = metrics.report.deepMerge(
        Json.obj(
          "kind" -> Json.fromString(if (shared) "sharedRecruiter" else "distinctRecruiters"),
          "candidateIndexes" -> Json.fromBoolean(candidateIndexes),
          "seededActiveSubjectUsers" -> Json.fromInt(subjectUsers.size),
          "totalOwnedDatasetRecords" -> Json.fromLong(totalRecords),
          "fixedIndexWritePairMetrics" -> write.report,
          "outboxRecords" -> Json.fromInt(outboxSize),
          "claimAttemptsIncludingConflicts" -> Json.fromInt(claimsAttempted),
          "scannedPages" -> Json.fromInt(scannedPages),
          "publishedIncludingSeededAndWarmup" -> Json.fromLong(published),
          "remaining" -> Json.fromLong(remaining),
          "activeOwnedClaims" -> Json.fromLong(activeClaims),
          "activeSubjectLeases" -> Json.fromLong(activeSubjectLeases),
          "backlogAgeSecondsAtFixedClock" -> backlog.fold(Json.Null)(row =>
            Json.fromDoubleOrNull(
              java.time.Duration.between(row.getDate("occurredAt").toInstant, now).toMillis.toDouble / 1000d
            )
          ),
          "indexBytes" -> Json.fromLong(bytes.get("totalIndexSize", classOf[Number]).longValue()),
          "collectionBsonBytes" -> Json.fromLong(bytes.get("size", classOf[Number]).longValue()),
          "nativeOrExplainBeforeWorkload" -> jsonDocument(explain),
          "publicationBoundary" -> Json.fromString("Mongo claim plus durable acknowledgment; no Kafka/network latency")
        )
      )
    } yield OutboxRun(metrics.copy(report = report), write, bytes.get("totalIndexSize", classOf[Number]).longValue())
  }

  /** Independent fixed cohorts isolate ordinary index update cost from claim selection and transaction conflicts. */
  private def indexWriteCost(
      fixture: support.Fixture,
      documents: List[Document],
      candidateIndexes: Boolean,
      concurrency: Int
  ): IO[Measurement] = {
    val name = "outbox_index_write_cost"
    for {
      collection <- MongoRepositoryTestSupport.collection(fixture.database, name)
      _ <- collection.insertMany(documents.take(warmup + samples).map(row => new Document(row)))
      _ <- collection
        .createIndex(
          Indexes.ascending("state", "availableAt", "leaseUntil", "occurredAt", "_id"),
          new IndexOptions().name("outbox_claim")
        )
        .void
      _ <- collection.createIndex(Indexes.ascending("subjectIds"), new IndexOptions().name("outbox_subjects")).void
      _ <- collection
        .createIndex(
          Indexes.ascending("retentionExpiresAt"),
          new IndexOptions()
            .name("published_retention")
            .expireAfter(0L, java.util.concurrent.TimeUnit.SECONDS)
            .partialFilterExpression(Filters.eq("state", "Published"))
        )
        .void
      _ <- IO.whenA(candidateIndexes)(
        collection
          .createIndex(
            Indexes.ascending("availableAt", "occurredAt", "_id"),
            new IndexOptions().name("retryable_claim").partialFilterExpression(Filters.eq("state", "Retryable"))
          )
          .void *>
          collection
            .createIndex(
              Indexes.ascending("leaseUntil", "availableAt", "occurredAt", "_id"),
              new IndexOptions().name("expired_claim").partialFilterExpression(Filters.eq("state", "InFlight"))
            )
            .void
      )
      measured <- observe(fixture, concurrency) { index =>
        val row = documents(index)
        val filter = Filters.eq("_id", row.getString("_id"))
        for {
          first <- collection.updateOne(
            filter,
            Updates.combine(Updates.set("state", "InFlight"), Updates.set("leaseUntil", Date.from(now.plusSeconds(60))))
          )
          second <- collection.updateOne(
            filter,
            Updates.combine(
              Updates.set("state", row.getString("state")),
              Option(row.get("leaseUntil")).fold(Updates.unset("leaseUntil"))(value => Updates.set("leaseUntil", value))
            )
          )
          _ <- IO(assertEquals(first.getMatchedCount, 1L))
          _ <- IO(assertEquals(second.getMatchedCount, 1L))
        } yield Result(true)
      }
    } yield measured.copy(report =
      measured.report.deepMerge(
        Json.obj(
          "boundary" -> Json.fromString(
            "Two acknowledged direct-driver state/index updates on fixed independent rows; excludes claim selection, transactions and Kafka"
          )
        )
      )
    )
  }

  private def discoveryRun(fixture: support.Fixture, concurrency: Int): IO[Json] = for {
    _ <- IO.println(s"Measuring exact facets and dense geographic pages: concurrency=$concurrency")
    _ <- seedJobs(fixture)
    policy <- DiscoveryQueryPolicy.create(5.seconds, 4)
    jobs = MongoJobRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop,
      Some(policy)
    )
    users = MongoUserRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop
    )
    scope <- IO.fromEither(
      HiringReadScope
        .validated(ActorContext(candidate.id, candidate.role), candidate, ActorAuthorization(users))
        .leftMap(error => new AssertionError(error.toString))
    )
    nearby = NearbyJobsQuery(center, 10d, JobSearchFilter(None, Set.empty, None))
    firstPage <- requireResult(jobs.nearbyJobs(scope, nearby, 21))
    later = firstPage.lastOption.map(value => NearbyJobCursor(value.distanceKm, value.job.id, nearby.fingerprint))
    rows <- List("exactHighCardinalityFacets", "denseRadius", "denseRadiusLaterPage").traverse { kind =>
      def call: IO[Result] = kind match {
        case "exactHighCardinalityFacets" =>
          requireResult(jobs.jobDiscoveryFacets(scope, JobFacetQuery(JobSearchFilter(None, Set.empty, None), None)))
            .as(Result(true))
        case "denseRadius" => requireResult(jobs.nearbyJobs(scope, nearby, 21)).map(result => Result(true, result.size))
        case _             =>
          requireResult(jobs.nearbyJobs(scope, nearby.copy(after = later), 21)).map(result => Result(true, result.size))
      }
      for {
        metrics <- observe(fixture, concurrency)(_ => call)
        commands <- fixture.commands.snapshot
        explained <- commands.find(_.containsKey("aggregate")).traverse { command =>
          val plain = Document.parse(command.toJson)
          List("lsid", "$db", "$clusterTime", "readConcern", "txnNumber", "autocommit", "startTransaction").foreach(
            field => { val _ = plain.remove(field) }
          )
          support
            .command(fixture.database, new Document("explain", plain).append("verbosity", "executionStats"))
            .map(jsonDocument)
        }
      } yield Json.obj(
        "kind" -> Json.fromString(kind),
        "metrics" -> metrics.report,
        "planAfterWorkload" -> explained.getOrElse(Json.Null),
        "eligibleJobs" -> Json.fromInt(256),
        "distinctSkills" -> Json.fromInt(257),
        "semantics" -> Json.fromString("Exact full-set facets; bounded radius pages; no sampling/truncation change")
      )
    }
  } yield Json.fromValues(rows)

  private def metadata(fixture: support.Fixture): IO[Json] = for {
    captured <- IO.realTimeInstant
    build <- support.command(fixture.database, new Document("buildInfo", 1))
    fingerprint <- IO.blocking {
      val digest = java.security.MessageDigest.getInstance("SHA-256")
      List(
        "src/main/scala/com/example/graphQL/cats/repository/mongo/MongoJobRepository.scala",
        "src/main/scala/com/example/graphQL/cats/repository/mongo/MongoApplicationRepository.scala",
        "src/main/scala/com/example/graphQL/cats/repository/mongo/MongoOperationalEventRepositories.scala",
        "src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringIndexSetup.scala",
        "src/it/scala/com/example/graphQL/cats/repository/mongo/HiringPersistenceScalingIntegrationSpec.scala"
      ).foreach(path => digest.update(Files.readAllBytes(Paths.get(path))))
      digest.digest().map(byte => f"${byte & 0xff}%02x").mkString
    }
  } yield Json.obj(
    "capturedAt" -> Json.fromString(captured.toString),
    "mongoVersion" -> Json.fromString(build.getString("version")),
    "sourceSha256" -> Json.fromString(fingerprint),
    "javaVersion" -> Json.fromString(System.getProperty("java.version")),
    "environment" -> Json.fromString(
      "Disposable local test namespaces on manifest-owned Mongo service; no Atlas/providers/Kafka/network timing"
    )
  )

  private def writeReport(name: String, value: Json): IO[Unit] = IO.blocking {
    val directory = Paths.get(".local", "data", "hiring-persistence-scaling")
    val _ = Files.createDirectories(directory)
    val _ = Files.writeString(directory.resolve(name + ".json"), value.spaces2)
  }

  test("measure submission projection and popular-job contention in three paired isolated cohorts") {
    enabled
    mongoResource.use { fixture =>
      (1 to 3).toList
        .traverse { pair =>
          List(1, 8)
            .traverse { concurrency =>
              List(false, true)
                .traverse { hot =>
                  List(false, true).traverse { projected =>
                    support.isolatedFixture(fixture).use { cohort =>
                      submissionRun(cohort, projected, hot, concurrency)
                        .map(value => value.deepMerge(Json.obj("pair" -> Json.fromInt(pair))))
                    }
                  }
                }
                .map(_.flatten)
            }
            .map(_.flatten)
        }
        .flatMap(rows =>
          metadata(fixture).flatMap(meta =>
            writeReport(
              "submission",
              meta.deepMerge(
                Json.obj(
                  "seed" -> Json.fromLong(seed),
                  "pairs" -> Json.fromInt(3),
                  "runs" -> Json.fromValues(rows.flatten),
                  "scope" -> Json
                    .fromString("Repository transactions and pure snapshot projection; no API or provider timing")
                )
              )
            )
          )
        )
    }
  }

  test("compare native OR claim index candidates under distinct and shared-recruiter contention") {
    enabled
    mongoResource.use { fixture =>
      (1 to 3).toList
        .traverse { pair =>
          List(1, 8)
            .traverse { concurrency =>
              List(false, true).traverse { shared =>
                for {
                  baseline <- support.isolatedFixture(fixture).use(outboxRun(_, false, shared, concurrency))
                  candidate <- support.isolatedFixture(fixture).use(outboxRun(_, true, shared, concurrency))
                  improvement =
                    if (baseline.measurement.throughput > 0d)
                      candidate.measurement.throughput / baseline.measurement.throughput - 1d
                    else 0d
                  claimP95Improvement =
                    if (baseline.measurement.p95 > 0d) 1d - candidate.measurement.p95 / baseline.measurement.p95 else 0d
                  p95Ratio =
                    if (baseline.write.p95 > 0d) candidate.write.p95 / baseline.write.p95 else Double.PositiveInfinity
                  growth = candidate.indexBytes.toDouble / baseline.indexBytes - 1d
                  adopted = (improvement >= .15d || claimP95Improvement >= .15d) && p95Ratio <= 1.10d && growth <= .25d
                } yield Json.obj(
                  "pair" -> Json.fromInt(pair),
                  "concurrency" -> Json.fromInt(concurrency),
                  "sharedRecruiter" -> Json.fromBoolean(shared),
                  "baseline" -> baseline.measurement.report,
                  "candidate" -> candidate.measurement.report,
                  "completedThroughputImprovement" -> Json.fromDoubleOrNull(improvement),
                  "claimCompletionP95Improvement" -> Json.fromDoubleOrNull(claimP95Improvement),
                  "writeCompletionP95Ratio" -> Json.fromDoubleOrNull(p95Ratio),
                  "indexGrowth" -> Json.fromDoubleOrNull(growth),
                  "passesAllGates" -> Json.fromBoolean(adopted)
                )
              }
            }
            .map(_.flatten)
        }
        .flatMap(rows =>
          metadata(fixture).flatMap(meta =>
            writeReport(
              "outbox-index-comparison",
              meta.deepMerge(
                Json.obj(
                  "seed" -> Json.fromLong(seed),
                  "pairs" -> Json.fromInt(3),
                  "comparisons" -> Json.fromValues(rows.flatten),
                  "adoption" -> Json.fromString(
                    "Candidate is adoptable only if all three pairs at both concurrencies and both skews pass; production catalog unchanged by benchmark"
                  )
                )
              )
            )
          )
        )
    }
  }

  test("measure full-set exact facets dense radius and later pages at concurrency one and eight") {
    enabled
    mongoResource.use { fixture =>
      (1 to 3).toList
        .traverse { pair =>
          List(1, 8).traverse { concurrency =>
            support
              .isolatedFixture(fixture)
              .use(discoveryRun(_, concurrency))
              .map(value =>
                Json.obj("pair" -> Json.fromInt(pair), "concurrency" -> Json.fromInt(concurrency), "workloads" -> value)
              )
          }
        }
        .flatMap(rows =>
          metadata(fixture).flatMap(meta =>
            writeReport(
              "discovery",
              meta.deepMerge(
                Json.obj(
                  "seed" -> Json.fromLong(seed),
                  "runs" -> Json.fromValues(rows.flatten),
                  "status" -> Json.fromString("Descriptive measurements; no query/schema change adopted")
                )
              )
            )
          )
        )
    }
  }
}
