package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import org.bson.Document
import scala.concurrent.duration.*

final class MongoSearchAuthorizationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val model = "synthetic-search"
  private val profile = CandidateProfile(Set("Scala"), Some("Engineer"), None)
  private val candidateUser = candidate.copy(
    profile = Some(UserProfile.Candidate(profile)),
    embedding =
      Some(EntityEmbedding(List(1f), EmbeddingMeta(model, SourceHash.sha256(SearchableText.candidate(profile)), now)))
  )
  private val queryJob = openJob.copy(embedding =
    Some(EntityEmbedding(List(1f), EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(openJob)), now)))
  )

  private def replace(db: mongo4cats.database.MongoDatabase[IO], collection: String, document: Document): IO[Unit] =
    MongoAccessEvaluationSupport
      .command(
        db,
        new Document("update", collection).append(
          "updates",
          java.util.List.of(
            new Document("q", new Document("_id", document.getString("_id"))).append("u", document)
          )
        )
      )
      .flatMap(result => IO(assertEquals(result.get("n", classOf[Number]).intValue(), 1)))

  test("Mongo final eligibility joins gate current actor, open ownership and query source after prechecks") {
    mongoResource.use { fixture =>
      val db = fixture.database
      val users = new MongoUserRepository(
        db,
        MongoRepositoryTestSupport.noTransaction,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val search =
        new MongoSemanticSearchRepository(db, "jv", "cv", "jl", "cl", 100, 100, diagnostics = Diagnostics.noop)
      val authorization = ActorAuthorization(users)
      def scope(user: User): HiringReadScope = HiringReadScope
        .validated(ActorContext(user.id, user.role), user, authorization)
        .toOption
        .getOrElse(fail("invalid scope"))
      val recruiterScope = scope(recruiter)
      val candidateScope = scope(candidateUser)
      val currentJob = JobSearchEligibility.fromJob(queryJob)
      for {
        _ <- MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(recruiter))
        _ <- MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(candidateUser))
        _ <- MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(queryJob))
        _ <- MongoHiringSetup.initialize(db, Diagnostics.noop)
        accepted <- search.authorizedCandidateEligibility(recruiterScope, currentJob, List(candidateUser.id)).value
        _ = assertEquals(accepted.map(_.map(_.id)), Right(List(candidateUser.id)))
        _ <- List("ownership", "actor", "closed", "source", "model").traverse_ { mutation =>
          val mutatedJob = mutation match {
            case "ownership" => queryJob.copy(recruiterId = candidateUser.id)
            case "closed"    => queryJob.copy(status = JobStatus.Closed)
            case "source"    => queryJob.copy(description = "Changed after precheck")
            case "model"     =>
              queryJob.copy(embedding = queryJob.embedding.map(e => e.copy(meta = e.meta.copy(model = "other"))))
            case _ => queryJob
          }
          for {
            _ <- replace(
              db,
              MongoCollections.Users,
              MongoHiringCodecs.user(
                if (mutation == "actor") recruiter.copy(accountStatus = AccountStatus.Deleted, profile = None)
                else recruiter
              )
            )
            _ <- replace(db, MongoCollections.Jobs, MongoHiringCodecs.job(mutatedJob))
            denied <- search.authorizedCandidateEligibility(recruiterScope, currentJob, List(candidateUser.id)).value
          } yield assertEquals(denied, Right(Nil), mutation)
        }
        _ <- replace(db, MongoCollections.Jobs, MongoHiringCodecs.job(queryJob))
        visible <- search.authorizedJobEligibility(candidateScope, List(queryJob.id), None).value
        _ = assertEquals(visible.map(_.map(_.job.id)), Right(List(queryJob.id)))
        _ <- replace(
          db,
          MongoCollections.Users,
          MongoHiringCodecs.user(candidateUser.copy(accountStatus = AccountStatus.Deleted, profile = None))
        )
        deniedJobs <- search.authorizedJobEligibility(candidateScope, List(queryJob.id), None).value
        _ = assertEquals(deniedJobs, Right(Nil))
        _ <- replace(
          db,
          MongoCollections.Users,
          MongoHiringCodecs.user(
            candidateUser.copy(profile =
              Some(
                UserProfile.Candidate(profile.copy(experienceSummary = Some("Changed after recommendation precheck")))
              )
            )
          )
        )
        staleQuery <- search
          .authorizedJobEligibility(
            candidateScope,
            List(queryJob.id),
            Some(CandidateSearchEligibility.fromUser(candidateUser))
          )
          .value
        _ = assertEquals(staleQuery, Right(Nil))
      } yield ()
    }
  }
}
