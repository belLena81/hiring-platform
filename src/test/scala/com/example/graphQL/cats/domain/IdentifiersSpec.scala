package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId, parse}
import java.util.UUID
import munit.FunSuite
import scala.compiletime.testing.typeCheckErrors

final class IdentifiersSpec extends FunSuite {
  private val uuid = UUID.fromString("00000000-0000-0000-0000-000000000001")

  test("named identifier constructors preserve UUID values") {
    assertEquals(UserId(uuid).value, uuid)
    assertEquals(JobId(uuid).value, uuid)
    assertEquals(ApplicationId(uuid).value, uuid)
    assertEquals(ApplicationEventId(uuid).value, uuid)
  }

  test("parse constructs typed identifiers from UUID strings") {
    assertEquals(parse(uuid.toString)(UserId.apply), Right(UserId(uuid)))
    assertEquals(parse(uuid.toString)(JobId.apply), Right(JobId(uuid)))
    assertEquals(parse(uuid.toString)(ApplicationId.apply), Right(ApplicationId(uuid)))
    assertEquals(parse(uuid.toString)(ApplicationEventId.apply), Right(ApplicationEventId(uuid)))
  }

  test("parse returns a failure for malformed identifier strings") {
    assert(parse("not-a-uuid")(UserId.apply).isLeft)
  }

  test("identifier aliases remain type-safe despite sharing a UUID representation") {
    val errors = typeCheckErrors("""
      import java.util.UUID
      import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
      val userId: UserId = UserId(UUID.randomUUID())
      val jobId: JobId = userId
    """)

    assert(errors.nonEmpty)
  }
}
