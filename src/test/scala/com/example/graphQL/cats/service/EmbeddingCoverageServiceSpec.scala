package com.example.graphQL.cats.service

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.domain.model.{AccountStatus, EntityEmbedding, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.{EmbeddingCoverageRepository, RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.search.*
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

final class EmbeddingCoverageServiceSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-10-08T12:00:00Z")
  private val adminId = UserId(UUID.fromString("40000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("40000000-0000-0000-0000-000000000002"))
  private val candidateId = UserId(UUID.fromString("40000000-0000-0000-0000-000000000003"))
  private val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
  private val recruiter = User(recruiterId, None, "Recruiter", UserRole.Recruiter, None, now)
  private val candidate = User(candidateId, None, "Candidate", UserRole.Candidate, None, now)
  private val duplicateAdmin =
    admin.copy(id = UserId(UUID.fromString("40000000-0000-0000-0000-000000000004")), adminSingleton = false)
  private val deletedAdmin = admin.copy(accountStatus = AccountStatus.Deleted, deletedAt = Some(now))
  private val empty = EmbeddingCoverageObservation(
    EmbeddingCoverageTally.empty,
    Nil,
    EmbeddingQueueObservation(truncated = false, 0L, None)
  )

  private final class Recording(requests: Ref[IO, List[EmbeddingCoverageScanRequest]])
      extends EmbeddingCoverageRepository {
    override def observe(request: EmbeddingCoverageScanRequest): RepositoryIO[EmbeddingCoverageObservation] =
      RepositoryIO.lift(requests.update(request :: _).as(empty))
  }

  private def users(values: User*) = new TestUsers(values.map(user => user.id -> user).toMap)

  private def live(known: User*)(requests: Ref[IO, List[EmbeddingCoverageScanRequest]]) =
    EmbeddingCoverageService.live(users(known*), new Recording(requests), 5.minutes, clock = FixedTestClock.at(now))

  private def actor(user: User) = ActorContext(user.id, user.role)

  test("ECR-05 only the stored active Admin can run the report and the scan uses 3x the retry cap") {
    for {
      requests <- Ref.of[IO, List[EmbeddingCoverageScanRequest]](Nil)
      result <- live(admin)(requests).report(actor(admin), Some("model-a")).value
      seen <- requests.get
    } yield {
      assertEquals(result.map(_.expectedModel), Right(Some("model-a")))
      assertEquals(result.map(_.asOf), Right(now))
      assertEquals(seen.map(_.stuckBefore), List(now.minusSeconds(15 * 60)))
      assertEquals(seen.map(_.expectedModel), List(Some("model-a")))
    }
  }

  test(
    "ECR-05 Candidates, Recruiters, deleted and non-singleton Admins and unknown actors are rejected before any scan"
  ) {
    val denied = Left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
    for {
      requests <- Ref.of[IO, List[EmbeddingCoverageScanRequest]](Nil)
      service = live(admin, recruiter, candidate, deletedAdmin.copy(id = UserId(UUID.randomUUID())))(requests)
      byRecruiter <- service.report(actor(recruiter), None).value
      byCandidate <- service.report(actor(candidate), None).value
      byDeleted <- live(deletedAdmin)(requests).report(actor(deletedAdmin), None).value
      byNonSingleton <- live(duplicateAdmin)(requests).report(actor(duplicateAdmin), None).value
      byAdminClaim <- service.report(ActorContext(recruiterId, UserRole.Admin), None).value
      unknown <- service.report(ActorContext(UserId(UUID.randomUUID()), UserRole.Admin), None).value
      seen <- requests.get
    } yield {
      List(byRecruiter, byCandidate, byDeleted, byNonSingleton, byAdminClaim, unknown).foreach(assertEquals(_, denied))
      assertEquals(seen, Nil)
    }
  }

  test("expectedModel must be non-blank and at most 128 characters when present") {
    for {
      requests <- Ref.of[IO, List[EmbeddingCoverageScanRequest]](Nil)
      service = live(admin)(requests)
      blank <- service.report(actor(admin), Some("  ")).value
      tooLong <- service.report(actor(admin), Some("m" * 129)).value
      maximum <- service.report(actor(admin), Some("m" * 128)).value
      seen <- requests.get
    } yield {
      assert(blank.left.exists(_.isInstanceOf[UseCaseError.ValidationFailed]))
      assert(tooLong.left.exists(_.isInstanceOf[UseCaseError.ValidationFailed]))
      assert(maximum.isRight)
      assertEquals(seen.size, 1)
    }
  }

  test("expectedModel is trimmed so surrounding whitespace cannot mismatch every embedding") {
    for {
      requests <- Ref.of[IO, List[EmbeddingCoverageScanRequest]](Nil)
      result <- live(admin)(requests).report(actor(admin), Some("  model-a ")).value
      seen <- requests.get
      padded <- live(admin)(requests).report(actor(admin), Some(" " + "m" * 128 + " ")).value
    } yield {
      assertEquals(result.map(_.expectedModel), Right(Some("model-a")))
      assertEquals(seen.map(_.expectedModel), List(Some("model-a")))
      assert(padded.isRight)
    }
  }

  test("the default capability denies every caller as unauthorized") {
    EmbeddingCoverageUseCases.denyAll.report(actor(admin), None).value.map { result =>
      assertEquals(result, Left(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
    }
  }

  test("ECR-08 disabled vector search yields the typed unavailable error for an Admin, not zero counts") {
    val unavailable = Left(UseCaseError.Search(SearchError.VectorSearchUnavailable))
    for {
      admin_ <- EmbeddingCoverageService.vectorSearchDisabled(users(admin, recruiter)).report(actor(admin), None).value
      other <- EmbeddingCoverageService
        .vectorSearchDisabled(users(admin, recruiter))
        .report(actor(recruiter), None)
        .value
    } yield {
      assertEquals(admin_, unavailable)
      assertEquals(other, Left(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
    }
  }

  test("repository failures stay typed") {
    val failing = new EmbeddingCoverageRepository {
      override def observe(request: EmbeddingCoverageScanRequest): RepositoryIO[EmbeddingCoverageObservation] =
        RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
    }
    EmbeddingCoverageService
      .live(users(admin), failing, 5.minutes, clock = FixedTestClock.at(now))
      .report(actor(admin), None)
      .value
      .map { result =>
        assertEquals(result, Left(UseCaseError.Repository(RepositoryError.Unavailable)))
      }
  }

  private final class TestUsers(values: Map[UserId, User]) extends ServiceFixtures.VersionedUserRepositoryTestAdapter {
    override def find(id: UserId): RepositoryIO[Option[User]] = RepositoryIO.fromEither(Right(values.get(id)))
    override def findMany(ids: List[UserId]): RepositoryIO[List[User]] =
      RepositoryIO.fromEither(Right(ids.flatMap(values.get)))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): RepositoryIO[Unit] =
      RepositoryIO.fromEither(Right(()))
  }
}
