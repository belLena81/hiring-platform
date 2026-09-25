package com.example.graphQL.cats.service

import cats.effect.IO
import com.example.graphQL.cats.domain.model.{AccountStatus, EntityEmbedding, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{
  AnalyticsFunnelDay,
  AnalyticsReportRepository,
  AnalyticsReportSnapshot,
  AnalyticsSkillPostingDay,
  RepositoryError
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

  test("analytics accepts the exact 30-day inclusive period") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val service = new AnalyticsReportingService(new TestUsers(Map(adminId -> admin)), new TestReports(Some(snapshot)))
    val from = now.minusSeconds(30L * 24L * 60L * 60L)
    service.report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(from, now)).value.map { result =>
      assertEquals(result, Right(snapshot))
    }
  }

  test("analytics returns only daily rows inside the requested inclusive period and preserves asOf") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val before = now.minusSeconds(2L * 24L * 60L * 60L)
    val from = now.minusSeconds(24L * 60L * 60L)
    val after = now.plusSeconds(24L * 60L * 60L)
    val rows = List(before, from, now, after).map(day => AnalyticsFunnelDay(day, 1, 0, 0, 0, 0, 0))
    val skills = List(before, from, now, after).map(day => AnalyticsSkillPostingDay(day, "scala", 1))
    val report = AnalyticsReportSnapshot(now, rows, None, skills)
    val service = new AnalyticsReportingService(new TestUsers(Map(adminId -> admin)), new TestReports(Some(report)))

    service.report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(from, now)).value.map { result =>
      assertEquals(result, Right(report.copy(funnel = rows.slice(1, 3), skillPostingActivity = skills.slice(1, 3))))
    }
  }

  test("analytics rejects a reversed period") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val service = new AnalyticsReportingService(new TestUsers(Map(adminId -> admin)), new TestReports(Some(snapshot)))
    service.report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(now, now.minusSeconds(1))).value.map {
      result =>
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

  test("a deleted Admin cannot read aggregate analytics") {
    val deletedAdmin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true).copy(
      accountStatus = AccountStatus.Deleted,
      deletedAt = Some(now)
    )
    val service =
      new AnalyticsReportingService(new TestUsers(Map(adminId -> deletedAdmin)), new TestReports(Some(snapshot)))
    service
      .report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(now.minusSeconds(60), now))
      .value
      .map { result =>
        assertEquals(result, Left(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
      }
  }

  test("analytics reports a typed unavailable outcome when no published snapshot exists") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val service = new AnalyticsReportingService(new TestUsers(Map(adminId -> admin)), new TestReports(None))
    service.report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(now.minusSeconds(60), now)).value.map {
      result =>
        assertEquals(result, Left(UseCaseError.Analytics(AnalyticsError.ReportsUnavailable)))
    }
  }

  test("analytics reports a typed unavailable outcome when the report store fails") {
    val admin = User(adminId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
    val service = new AnalyticsReportingService(
      new TestUsers(Map(adminId -> admin)),
      new TestReports(None, Some(RepositoryError.Unavailable))
    )
    service.report(ActorContext(adminId, UserRole.Admin), AnalyticsPeriod(now.minusSeconds(60), now)).value.map {
      result =>
        assertEquals(result, Left(UseCaseError.Repository(RepositoryError.Unavailable)))
    }
  }

  private final class TestReports(
      value: Option[AnalyticsReportSnapshot],
      failure: Option[RepositoryError] = None
  ) extends AnalyticsReportRepository {
    override def latest: IO[Either[RepositoryError, Option[AnalyticsReportSnapshot]]] =
      IO.pure(failure.toLeft(value))
  }

  private final class TestUsers(values: Map[UserId, User]) extends ServiceFixtures.VersionedUserRepositoryTestAdapter {
    override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] = IO.pure(Right(values.get(id)))
    override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] =
      IO.pure(Right(ids.flatMap(values.get)))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
      IO.pure(Right(()))
  }
}
