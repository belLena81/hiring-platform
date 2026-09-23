package com.example.graphQL.cats.service

import cats.effect.IO
import com.example.graphQL.cats.domain.model.{EntityEmbedding, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{
  AnalyticsReportRepository,
  AnalyticsReportSnapshot,
  RepositoryError,
  UserRepository
}
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

final class AnalyticsReportingServiceSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-22T00:00:00Z")
  private val adminId = UserId(UUID.fromString("30000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("30000000-0000-0000-0000-000000000002"))
  private val snapshot = AnalyticsReportSnapshot(now, Nil, None, Nil)

  test("published analytics are available only to the stored active Admin") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val service = new AnalyticsReportingService(new TestUsers(Map(adminId -> admin)), new TestReports(Some(snapshot)))
    service.report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(now.minusSeconds(60), now)).value.map {
      result =>
        assertEquals(result, Right(snapshot))
    }
  }

  test("analytics rejects a period longer than the retained published window") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val service = new AnalyticsReportingService(new TestUsers(Map(adminId -> admin)), new TestReports(Some(snapshot)))
    service
      .report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(now.minusSeconds(31L * 24L * 60L * 60L), now))
      .value
      .map { result =>
        assertEquals(result, Left(UseCaseError.Analytics(AnalyticsError.InvalidPeriod)))
      }
  }

  test("analytics rejects an extreme period without epoch arithmetic overflow") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val service = new AnalyticsReportingService(new TestUsers(Map(adminId -> admin)), new TestReports(Some(snapshot)))
    service.report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(Instant.MIN, Instant.MAX)).value.map {
      result =>
        assertEquals(result, Left(UseCaseError.Analytics(AnalyticsError.InvalidPeriod)))
    }
  }

  test("a non-Admin actor cannot read aggregate analytics") {
    val recruiter = User(recruiterId, None, "Recruiter", UserRole.Recruiter, None, now)
    val service =
      new AnalyticsReportingService(new TestUsers(Map(recruiterId -> recruiter)), new TestReports(Some(snapshot)))
    service
      .report(ActorContext(recruiterId, UserRole.Recruiter), AnalyticsPeriod(now.minusSeconds(60), now))
      .value
      .map { result =>
        assertEquals(result, Left(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
      }
  }

  private final class TestReports(value: Option[AnalyticsReportSnapshot]) extends AnalyticsReportRepository {
    override def latest: IO[Either[RepositoryError, Option[AnalyticsReportSnapshot]]] = IO.pure(Right(value))
  }

  private final class TestUsers(values: Map[UserId, User]) extends UserRepository {
    override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] = IO.pure(Right(values.get(id)))
    override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] =
      IO.pure(Right(ids.flatMap(values.get)))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
      IO.pure(Right(()))
  }
}
