package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import io.circe.Json
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class MongoEmbeddingCoverageIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private val now = Instant.parse("2026-10-08T12:00:00Z")
  private val limits = EmbeddingCoverageLimits(maxEntitiesPerKind = 1000, pageSize = 2, maxTime = 10.seconds)
  private def request(model: Option[String], bounds: EmbeddingCoverageLimits = limits) =
    EmbeddingCoverageScanRequest(model, now, now.minusSeconds(15 * 60), bounds)

  private def database: Resource[IO, MongoDatabase[IO]] =
    mongoResource.evalMap(fixture =>
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).as(fixture.database)
    )

  private def successful[A](result: RepositoryIO[A]): IO[A] =
    result.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failure: $error")), IO.pure))

  private def insert(db: MongoDatabase[IO], collection: String, documents: List[Document]): IO[Unit] =
    documents.grouped(1000).toList.traverse_ { batch =>
      MongoRepositoryTestSupport.collection(db, collection).flatMap(_.insertMany(batch)).void
    }

  private def jobWith(
      n: Int,
      status: JobStatus = JobStatus.Open,
      embedding: Option[(String, Boolean)] = None
  ): Job = {
    val base = Job(
      JobId(UUID.randomUUID()),
      UserId(UUID.randomUUID()),
      s"Scala Engineer $n",
      "Hiring infrastructure",
      List("Cats Effect"),
      Set("Scala"),
      Location("Cyprus", "Nicosia", remote = true),
      status,
      now.minusSeconds(3600),
      now.minusSeconds(1800),
      Option.when(status == JobStatus.Closed)(now.minusSeconds(1800))
    )
    val hash = SourceHash.sha256(SearchableText.job(base))
    base.copy(embedding = embedding.map { case (model, fresh) =>
      EntityEmbedding(List(0.1f), EmbeddingMeta(model, if (fresh) hash else "stale", now.minusSeconds(1700)))
    })
  }

  private def candidateWith(
      n: Int,
      optIn: Boolean = false,
      role: UserRole = UserRole.Candidate,
      embedding: Option[(String, Boolean)] = None
  ): User = {
    val profile = CandidateProfile(
      Set("Scala"),
      Some(s"Engineer $n"),
      None,
      Some(CandidateResidence("Cyprus", Some("Nicosia"))),
      Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
      optIn
    )
    val hash = SourceHash.sha256(SearchableText.candidate(profile))
    User(
      UserId(UUID.randomUUID()),
      Some(email(s"candidate-$n-${UUID.randomUUID()}@example.com")),
      s"Candidate $n ${UUID.randomUUID()}",
      role,
      Some(
        if (role == UserRole.Candidate) UserProfile.Candidate(profile)
        else UserProfile.Recruiter(RecruiterProfile("Acme", None))
      ),
      now.minusSeconds(3600),
      embedding = embedding.map { case (model, fresh) =>
        EntityEmbedding(List(0.1f), EmbeddingMeta(model, if (fresh) hash else "stale", now.minusSeconds(1700)))
      }
    )
  }

  private def jobKey(job: Job) = EmbeddingWorkKey(EmbeddingWorkKind.Job, job.id.value.toString)
  private def candidateKey(user: User) = EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, user.id.value.toString)

  private def cell(
      report: EmbeddingCoverageObservation,
      kind: EmbeddingWorkKind,
      freshness: EmbeddingFreshness,
      repair: EmbeddingRepairState,
      failure: Option[EmbeddingWorkFailure] = None
  ): Long = report.tally.cells.getOrElse(EmbeddingCoverageCellKey(kind, freshness, repair, failure), 0L)

  test("ECR-02 ECR-03 the cross-tab counts every searchable entity once on a seeded database") {
    database.use { db =>
      val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val enqueuedAt = now.minusSeconds(600)
      val current = jobWith(1, embedding = Some("model-a" -> true))
      val currentWaiting = jobWith(2, embedding = Some("model-a" -> true))
      val changedRetrying = jobWith(3, embedding = Some("model-a" -> false))
      val leaseExpired = jobWith(4)
      val otherModel = jobWith(5, embedding = Some("model-b" -> true))
      val failed = jobWith(6)
      val orphan = jobWith(7)
      val closed = jobWith(8, status = JobStatus.Closed)
      val strayClosed = jobWith(9, status = JobStatus.Closed)
      val oldProcessing = jobWith(12, status = JobStatus.Closed)
      val atBoundary = jobWith(10, status = JobStatus.Closed)
      val pastBoundary = jobWith(11, status = JobStatus.Closed)
      val candidateCurrent = candidateWith(1, embedding = Some("model-a" -> true))
      val candidateWaiting = candidateWith(2)
      val candidateFailed = candidateWith(3, optIn = true)
      val recruiter = candidateWith(4, role = UserRole.Recruiter)
      val jobs =
        List(
          current,
          currentWaiting,
          changedRetrying,
          leaseExpired,
          otherModel,
          failed,
          orphan,
          closed,
          strayClosed,
          atBoundary,
          pastBoundary
        )
      def claimed(key: EmbeddingWorkKey, lease: Instant, at: Instant = enqueuedAt): IO[ClaimedEmbeddingWork] =
        successful(work.enqueue(key, at)) *>
          successful(work.claim("worker", at, lease)).flatMap(IO.fromOption(_)(new AssertionError("claim")))
      for {
        _ <- insert(db, MongoCollections.Jobs, jobs.map(MongoHiringCodecs.job(_)))
        _ <- insert(
          db,
          MongoCollections.Users,
          List(candidateCurrent, candidateWaiting, candidateFailed, recruiter).map(MongoHiringCodecs.user(_))
        )
        retry <- claimed(jobKey(changedRetrying), now.plusSeconds(60))
        _ <- successful(work.retry(retry, now.plusSeconds(3600)))
        _ <- claimed(jobKey(leaseExpired), enqueuedAt.plusSeconds(300))
        _ <- claimed(jobKey(otherModel), now.plusSeconds(600))
        exhausted <- claimed(jobKey(failed), now.plusSeconds(60), now.minusSeconds(10800))
        _ <- successful(work.fail(exhausted, EmbeddingWorkFailure.InvalidResponse, now))
        _ <- claimed(jobKey(oldProcessing), now.plusSeconds(600), now.minusSeconds(7200))
        tooLarge <- claimed(candidateKey(candidateFailed), now.plusSeconds(60), now.minusSeconds(10800))
        _ <- successful(work.fail(tooLarge, EmbeddingWorkFailure.DocumentTooLarge, now))
        _ <- successful(work.enqueue(jobKey(currentWaiting), now.minusSeconds(30)))
        _ <- successful(work.enqueue(candidateKey(candidateWaiting), now.minusSeconds(30)))
        _ <- successful(work.enqueue(jobKey(strayClosed), now.minusSeconds(7200)))
        // stuckBefore is now - 15 min and the comparison is strict: exactly at the bound is not stuck, 1 ms older is.
        _ <- successful(work.enqueue(jobKey(atBoundary), now.minusSeconds(15 * 60)))
        _ <- successful(work.enqueue(jobKey(pastBoundary), now.minusSeconds(15 * 60).minusMillis(1)))
        repository = new MongoEmbeddingCoverageRepository(db, Diagnostics.noop)
        unfiltered <- successful(repository.observe(request(None)))
        filtered <- successful(repository.observe(request(Some("model-a"))))
      } yield {
        import EmbeddingFreshness.*, EmbeddingRepairState.*
        val job = EmbeddingWorkKind.Job
        val profile = EmbeddingWorkKind.CandidateProfile
        assertEquals(unfiltered.tally.scanned(job), 7L)
        assertEquals(unfiltered.tally.scanned(profile), 3L)
        assertEquals(
          unfiltered.kinds.map(kind => kind.kind -> (kind.searchableCount, kind.truncated)).toMap,
          Map(job -> (7L, false), profile -> (3L, false))
        )
        assertEquals(unfiltered.tally.cells.values.sum, 10L)
        assertEquals(cell(unfiltered, job, Current, NoQueuedWork), 1L)
        assertEquals(cell(unfiltered, job, Current, Waiting), 1L)
        assertEquals(cell(unfiltered, job, ContentChanged, Retrying), 1L)
        assertEquals(cell(unfiltered, job, NotEmbedded, LeaseExpired), 1L)
        assertEquals(cell(unfiltered, job, Current, InProgress), 1L)
        assertEquals(cell(unfiltered, job, NotEmbedded, Failed, Some(EmbeddingWorkFailure.InvalidResponse)), 1L)
        assertEquals(cell(unfiltered, job, NotEmbedded, NoQueuedWork), 1L)
        assertEquals(cell(unfiltered, profile, Current, NoQueuedWork), 1L)
        assertEquals(cell(unfiltered, profile, NotEmbedded, Waiting), 1L)
        assertEquals(
          cell(unfiltered, profile, NotEmbedded, Failed, Some(EmbeddingWorkFailure.DocumentTooLarge)),
          1L
        )
        assertEquals(cell(filtered, job, ModelMismatch, InProgress), 1L)
        assertEquals(cell(filtered, job, Current, InProgress), 0L)
        assertEquals(filtered.tally.scanned(job), 7L)
        assertEquals(filtered.tally.orphanedGaps, 1L)
        assertEquals(filtered.tally.models.get(job -> "model-b"), Some(1L))
        assertEquals(filtered.tally.models.get(job -> "model-a"), Some(3L))
        val report = EmbeddingCoverageReport.assemble(filtered, Some("model-a"), now)
        assertEquals(
          report.checks.map(check => check.name -> (check.status, check.offendingCount)),
          List(
            EmbeddingCoverageCheckName.NoOrphanedGap -> (EmbeddingCoverageCheckStatus.Failed, 1L),
            EmbeddingCoverageCheckName.NoStuckWork -> (EmbeddingCoverageCheckStatus.Failed, 3L)
          )
        )
        assertEquals(report.lagEntityKinds, List(EmbeddingWorkKind.Job))
        // Failed rows 3 h old are neither stuck nor the oldest waiting work.
        assert(report.oldestQueuedWorkAgeSeconds.contains(7200L))
      }
    }
  }

  test("keyset paging covers every page without double counting") {
    database.use { db =>
      val jobs = (1 to 7).toList.map(n => jobWith(n))
      for {
        _ <- insert(db, MongoCollections.Jobs, jobs.map(MongoHiringCodecs.job(_)))
        observation <- successful(new MongoEmbeddingCoverageRepository(db, Diagnostics.noop).observe(request(None)))
      } yield {
        assertEquals(observation.tally.scanned(EmbeddingWorkKind.Job), 7L)
        assertEquals(observation.tally.orphanedGaps, 7L)
        assertEquals(observation.queue, EmbeddingQueueObservation(false, 0L, None))
      }
    }
  }

  test("ECR-06 a capped scan sets truncated and the checks are inconclusive") {
    database.use { db =>
      val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val jobs = (1 to 5).toList.map(n => jobWith(n, embedding = Some("model-a" -> true)))
      val capped = limits.copy(maxEntitiesPerKind = 3)
      for {
        _ <- insert(db, MongoCollections.Jobs, jobs.map(MongoHiringCodecs.job(_)))
        _ <- jobs.traverse_(job => successful(work.enqueue(jobKey(job), now.minusSeconds(30))))
        observation <- successful(
          new MongoEmbeddingCoverageRepository(db, Diagnostics.noop).observe(request(None, capped))
        )
        exact <- successful(
          new MongoEmbeddingCoverageRepository(db, Diagnostics.noop)
            .observe(request(None, capped.copy(maxEntitiesPerKind = 5)))
        )
      } yield {
        val report = EmbeddingCoverageReport.assemble(observation, None, now)
        val job = report.kinds.find(_.kind == EmbeddingWorkKind.Job).get
        assertEquals((job.searchableCount, job.scannedCount, job.truncated), (5L, 3L, true))
        assert(observation.queue.truncated)
        assertEquals(report.checks.map(_.status), List.fill(2)(EmbeddingCoverageCheckStatus.Inconclusive))
        val whole = EmbeddingCoverageReport.assemble(exact, None, now)
        assertEquals(whole.kinds.find(_.kind == EmbeddingWorkKind.Job).map(_.truncated), Some(false))
        assertEquals(whole.checks.map(_.status), List.fill(2)(EmbeddingCoverageCheckStatus.Passed))
      }
    }
  }

  private def stages(plan: Json): List[String] = {
    val cursor = plan.hcursor
    val self = cursor.get[String]("stage").toOption.toList ++ cursor.get[String]("indexName").toOption.map("idx=" + _)
    self ++ cursor.downField("inputStage").focus.toList.flatMap(stages) ++
      cursor.downField("inputStages").values.toList.flatMap(_.toList.flatMap(stages))
  }

  private def summary(label: String, explain: Document): String = {
    val json = io.circe.parser.parse(explain.toJson).getOrElse(Json.Null)
    val pipeline = json.hcursor.downField("stages").downArray
    val cursorStage = pipeline.downField("$cursor")
    val root = if (cursorStage.succeeded) cursorStage else json.hcursor
    val plan = root.downField("queryPlanner").downField("winningPlan").focus.getOrElse(Json.Null)
    val stats = root.downField("executionStats")
    def number(name: String) = stats.get[Long](name).toOption.fold("?")(_.toString)
    val lookup =
      json.hcursor.downField("stages").values.toList.flatMap(_.toList).flatMap(_.hcursor.downField("$lookup").focus)
    val lookupText = lookup.map(stage =>
      s"lookup(docsExamined=${stage.hcursor.get[Long]("totalDocsExamined").toOption.getOrElse(-1L)}, keysExamined=${stage.hcursor.get[Long]("totalKeysExamined").toOption.getOrElse(-1L)}, indexesUsed=${stage.hcursor.get[List[String]]("indexesUsed").toOption.getOrElse(Nil).mkString("[", ",", "]")})"
    )
    s"$label: plan=${stages(plan).mkString(">")} nReturned=${number("nReturned")} keysExamined=${number("totalKeysExamined")} docsExamined=${number("totalDocsExamined")} millis=${number("executionTimeMillis")} ${lookupText.mkString}"
  }

  test("a driver or server failure inside the scan yields a typed unavailable error, not a report") {
    database.use { db =>
      val invalid = limits.copy(pageSize = -1)
      new MongoEmbeddingCoverageRepository(db, Diagnostics.noop).observe(request(None, invalid)).value.map { result =>
        assertEquals(result, Left(RepositoryError.Unavailable))
      }
    }
  }

  test("entity pages run under snapshot read concern") {
    database.use { db =>
      val jobs = (1 to 3).toList.map(n => jobWith(n))
      for {
        _ <- insert(db, MongoCollections.Jobs, jobs.map(MongoHiringCodecs.job(_)))
        _ <- MongoAccessEvaluationSupport.command(db, new Document("profile", 2))
        _ <- successful(new MongoEmbeddingCoverageRepository(db, Diagnostics.noop).observe(request(None)))
        _ <- MongoAccessEvaluationSupport.command(db, new Document("profile", 0))
        entries <- MongoRepositoryTestSupport
          .collection(db, "system.profile")
          .flatMap(
            _.find(new Document("command.aggregate", MongoCollections.Jobs)).boundedStream(100).compile.toList
          )
      } yield {
        val pages = entries.filter(_.toJson.contains("$lookup"))
        assert(pages.nonEmpty, "expected profiled entity page aggregations")
        pages.foreach(entry => assert(entry.toJson.contains("snapshot"), entry.toJson))
        val others = entries.filterNot(_.toJson.contains("$lookup"))
        others.foreach(entry => assert(!entry.toJson.contains("snapshot"), entry.toJson))
      }
    }
  }

  test("ECR-10 records scan and queue plans for a seeded local dataset") {
    assume(java.lang.Boolean.getBoolean("hiring.scaling.measure"), "Explicit serialized measurement opt-in required")
    database.use { db =>
      val perKind = 20000
      val queued = 2000
      val jobs = (1 to perKind).toList.map { n =>
        val status = if (n % 10 == 0) JobStatus.Closed else JobStatus.Open
        val embedding = n % 4 match {
          case 0 => None
          case 1 => Some("model-a" -> true)
          case 2 => Some("model-a" -> false)
          case _ => Some("model-b" -> true)
        }
        jobWith(n, status, embedding)
      }
      val users = (1 to perKind).toList.map { n =>
        candidateWith(
          n,
          optIn = n % 2 == 0,
          role = if (n % 10 == 0) UserRole.Recruiter else UserRole.Candidate,
          embedding = if (n % 3 == 0) None else Some("model-a" -> (n % 3 == 1))
        )
      }
      val workRows = (jobs
        .take(queued / 2)
        .map(jobKey) ++ users.filter(_.role == UserRole.Candidate).take(queued / 2).map(candidateKey))
        .map(key =>
          new Document(MongoFields.Id, key.value)
            .append(MongoFields.Kind, key.kind.toString)
            .append(MongoFields.WorkEntityId, key.entityId)
            .append(MongoFields.CreatedAt, Date.from(now.minusSeconds(60)))
            .append(MongoFields.UpdatedAt, Date.from(now.minusSeconds(60)))
            .append(MongoFields.Generation, 1L)
            .append(MongoFields.Attempts, 0)
            .append(MongoFields.State, "Ready")
            .append(MongoFields.AvailableAt, Date.from(now.minusSeconds(60)))
        )
      val bounds = EmbeddingCoverageLimits.default
      for {
        _ <- insert(db, MongoCollections.Jobs, jobs.map(MongoHiringCodecs.job(_)))
        _ <- insert(db, MongoCollections.Users, users.map(MongoHiringCodecs.user(_)))
        _ <- insert(db, MongoCollections.EmbeddingWork, workRows)
        repository = new MongoEmbeddingCoverageRepository(db, Diagnostics.noop)
        timed <- repository.observe(request(Some("model-a"), bounds)).value.timed
        observation <- IO.fromEither(timed._2.left.map(error => new AssertionError(error.toString)))
        midJob = jobs.map(_.id.value.toString).sorted.drop(perKind / 2).head
        midUser = users.map(_.id.value.toString).sorted.drop(perKind / 2).head
        plans = List(
          "jobs first page" -> (
            MongoCollections.Jobs,
            MongoEmbeddingCoverageRepository
              .entityPagePipeline(MongoEmbeddingCoverageRepository.JobScan, None, bounds.pageSize)
          ),
          "jobs mid page" -> (
            MongoCollections.Jobs,
            MongoEmbeddingCoverageRepository.entityPagePipeline(
              MongoEmbeddingCoverageRepository.JobScan,
              Some(midJob),
              bounds.pageSize
            )
          ),
          "candidates first page" -> (
            MongoCollections.Users,
            MongoEmbeddingCoverageRepository
              .entityPagePipeline(MongoEmbeddingCoverageRepository.CandidateScan, None, bounds.pageSize)
          ),
          "candidates mid page" -> (
            MongoCollections.Users,
            MongoEmbeddingCoverageRepository.entityPagePipeline(
              MongoEmbeddingCoverageRepository.CandidateScan,
              Some(midUser),
              bounds.pageSize
            )
          ),
          "jobs eligible count" -> (
            MongoCollections.Jobs,
            MongoEmbeddingCoverageRepository.countPipeline(MongoEmbeddingCoverageRepository.JobScan)
          ),
          "candidates eligible count" -> (
            MongoCollections.Users,
            MongoEmbeddingCoverageRepository.countPipeline(MongoEmbeddingCoverageRepository.CandidateScan)
          ),
          "queue job aggregation" -> (
            MongoCollections.EmbeddingWork,
            MongoEmbeddingCoverageRepository
              .queuePipeline(MongoEmbeddingCoverageRepository.JobScan, request(None, bounds))
          ),
          "queue candidate aggregation" -> (
            MongoCollections.EmbeddingWork,
            MongoEmbeddingCoverageRepository.queuePipeline(
              MongoEmbeddingCoverageRepository.CandidateScan,
              request(None, bounds)
            )
          )
        )
        explains <- plans.traverse { case (label, (collection, pipeline)) =>
          MongoAccessEvaluationSupport
            .command(
              db,
              new Document(
                "explain",
                new Document("aggregate", collection)
                  .append("pipeline", pipeline.asJava)
                  .append("cursor", new Document())
              ).append("verbosity", "executionStats")
            )
            .map(label -> _)
        }
        lines = explains.map { case (label, explain) => summary(label, explain) }
        _ <- MongoAccessEvaluationSupport.command(db, new Document("profile", 2))
        _ <- MongoRepositoryTestSupport
          .collection(db, MongoCollections.Jobs)
          .flatMap(
            _.aggregate[Document](
              MongoEmbeddingCoverageRepository.entityPagePipeline(
                MongoEmbeddingCoverageRepository.JobScan,
                Some(midJob),
                bounds.pageSize
              )
            ).boundedStream(bounds.pageSize).compile.toList
          )
        profiled <- MongoRepositoryTestSupport
          .collection(db, "system.profile")
          .flatMap(
            _.find(new Document("command.aggregate", MongoCollections.Jobs))
              .sort(new Document("ts", -1))
              .limit(1)
              .boundedStream(1)
              .compile
              .toList
          )
        _ <- MongoAccessEvaluationSupport.command(db, new Document("profile", 0))
        profileLine = profiled.headOption.fold("no profile entry")(entry =>
          s"keysExamined=${entry.get("keysExamined")} docsExamined=${entry.get("docsExamined")} nreturned=${entry.get("nreturned")} planSummary=${entry.get("planSummary")} durationMillis=${entry.get("millis")}"
        )
        _ <- IO.println(s"ECR-10 profile jobs mid page including lookup: $profileLine")
        output = Path.of(".local", "logs", "embedding-coverage")
        _ <- IO.blocking {
          Files.createDirectories(output)
          explains.foreach { case (label, explain) =>
            Files.writeString(output.resolve(label.replace(' ', '-') + ".json"), explain.toJson)
          }
        }
        _ <- IO.println(
          s"ECR-10 dataset: jobs=$perKind users=$perKind embedding_work=${workRows.size}; full scan wall=${timed._1.toMillis}ms; " +
            s"kinds=${observation.kinds}; orphanedGaps=${observation.tally.orphanedGaps}; queue=${observation.queue}"
        )
        _ <- lines.traverse_(line => IO.println(s"ECR-10 explain $line"))
      } yield {
        assertEquals(observation.kinds.map(_.truncated), List(false, false))
        assertEquals(observation.tally.scanned(EmbeddingWorkKind.Job), (perKind - perKind / 10).toLong)
        assertEquals(observation.tally.scanned(EmbeddingWorkKind.CandidateProfile), (perKind - perKind / 10).toLong)
        assert(observation.tally.cells.nonEmpty)
      }
    }
  }
}
