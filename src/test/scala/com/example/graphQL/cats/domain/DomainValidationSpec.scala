package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.error.DomainValidationError.{BlankField, EmptyCollection, InvalidCoordinates}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{GeoPoint, Job, JobStatus, Location, User, UserRole}
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class DomainValidationSpec extends FunSuite {
  private val now = Instant.parse("2026-09-16T10:15:30Z")
  private val userId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  private val location = Location("Ukraine", "Kyiv", remote = true)

  test("user validation accumulates independent field errors with ValidatedNel") {
    val result = User.validate(userId, Some(" "), "", UserRole.Candidate, None, now)

    assertEquals(result.leftMap(_.toList).toEither, Left(List(BlankField("email"), BlankField("name"))))
  }

  test("job validation trims accepted values and accumulates blank collection errors") {
    val result = Job.validate(
      jobId,
      userId,
      " Senior Scala Developer ",
      " ",
      List(" ", ""),
      Set(" ", ""),
      location,
      JobStatus.Open,
      now,
      now
    )

    assertEquals(
      result.leftMap(_.toList).toEither,
      Left(
        List(
          BlankField("description"),
          EmptyCollection("requirements"),
          EmptyCollection("skills")
        )
      )
    )
  }

  test("job validation rejects blank location fields before persistence") {
    val result = Job.validate(
      jobId,
      userId,
      " ",
      "Build services",
      List("Scala"),
      Set("Cats Effect"),
      Location(" ", "", remote = true),
      JobStatus.Open,
      now,
      now
    )

    assertEquals(
      result.leftMap(_.toList).toEither,
      Left(
        List(
          BlankField("title"),
          BlankField("country"),
          BlankField("city")
        )
      )
    )
  }

  test("geographic job points require finite coordinates within WGS84 bounds") {
    assert(GeoPoint.validate(35.2, 33.4).isRight)
    assert(GeoPoint.validate(90.01, 33.4).isLeft)
    assert(GeoPoint.validate(35.2, Double.NaN).isLeft)
    assert(Location.validate("Cyprus", "Nicosia", remote = false, Some(GeoPoint(35.2, 181d))).isInvalid)
  }

  test("job validation retains accepted location coordinates") {
    val point = GeoPoint(35.2, 33.4)
    val result = Job.validate(
      jobId,
      userId,
      "Scala Developer",
      "Build services",
      List("Scala"),
      Set("Cats Effect"),
      Location("Cyprus", "Nicosia", remote = false, Some(point)),
      JobStatus.Open,
      now,
      now
    )

    assertEquals(result.toEither.map(_.location.coordinates), Right(Some(point)))
  }

  test("job validation rejects invalid coordinates and accumulates independent field errors") {
    List(
      GeoPoint(90.01, 33.4),
      GeoPoint(35.2, 181d),
      GeoPoint(Double.NaN, 33.4),
      GeoPoint(35.2, Double.PositiveInfinity)
    ).foreach { point =>
      val result = Job.validate(
        jobId,
        userId,
        " ",
        "Build services",
        List("Scala"),
        Set("Cats Effect"),
        Location("Cyprus", "Nicosia", remote = false, Some(point)),
        JobStatus.Open,
        now,
        now
      )

      assertEquals(result.leftMap(_.toList).toEither, Left(List(BlankField("title"), InvalidCoordinates)))
    }
  }

  test("valid job validation preserves ADT status and normalized text") {
    val result = Job.validate(
      jobId,
      userId,
      " Senior Scala Developer ",
      " Build services ",
      List(" Scala ", "Cats Effect"),
      Set(" Scala ", " MongoDB "),
      location,
      JobStatus.Open,
      now,
      now
    )

    assert(result.isValid)
    val job = result.toOption.getOrElse(fail("Expected valid job"))
    assertEquals(job.title, "Senior Scala Developer")
    assertEquals(job.requirements, List("Scala", "Cats Effect"))
    assertEquals(job.skills, Set("Scala", "MongoDB"))
    assertEquals(job.status, JobStatus.Open)
  }
}
