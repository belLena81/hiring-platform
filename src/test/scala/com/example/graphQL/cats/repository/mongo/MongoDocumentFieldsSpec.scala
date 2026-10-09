package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.MongoHiringCodecs.StoredDocumentError.{InvalidField, MissingField}
import com.example.graphQL.cats.service.RepositoryError
import com.mongodb.{MongoWriteException, ServerAddress, WriteError}
import munit.FunSuite
import org.bson.{BsonDocument, Document}
import org.bson.types.Decimal128

import java.time.Instant
import java.util.{Date, UUID}

final class MongoDocumentFieldsSpec extends FunSuite {
  import MongoDocumentFields.*

  private def doc(value: Any): Document = new Document("f", value)
  private val empty = new Document()

  test("required string: present, absent, null and wrong type") {
    assertEquals(requiredString(doc("x"), "f"), Right("x"))
    assertEquals(requiredString(empty, "f"), Left(MissingField("f")))
    assertEquals(requiredString(doc(null), "f"), Left(MissingField("f")))
    assertEquals(requiredString(doc(1), "f"), Left(InvalidField("f")))
    assertEquals(optionalString(empty, "f"), Right(None))
    assertEquals(optionalString(doc(null), "f"), Right(None))
    assertEquals(optionalString(doc("x"), "f"), Right(Some("x")))
    assertEquals(optionalString(doc(1), "f"), Left(InvalidField("f")))
  }

  test("boolean and double readers") {
    assertEquals(requiredBoolean(doc(true), "f"), Right(true))
    assertEquals(requiredBoolean(doc("true"), "f"), Left(InvalidField("f")))
    assertEquals(requiredBoolean(empty, "f"), Left(MissingField("f")))
    assertEquals(optionalBoolean(empty, "f"), Right(None))
    assertEquals(optionalBoolean(doc("x"), "f"), Left(InvalidField("f")))
    assertEquals(requiredDouble(doc(1.5), "f"), Right(1.5))
    assertEquals(requiredDouble(doc(2), "f"), Right(2.0))
    assertEquals(requiredDouble(doc("2"), "f"), Left(InvalidField("f")))
    assertEquals(optionalDouble(empty, "f"), Right(None))
  }

  test("exact-type readers accept only their BSON integer type") {
    assertEquals(requiredInt32(doc(Integer.valueOf(7)), "f"), Right(7))
    assertEquals(requiredInt32(doc(java.lang.Long.valueOf(7L)), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt32(doc(java.lang.Double.valueOf(7d)), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt32(empty, "f"), Left(MissingField("f")))
    assertEquals(optionalInt32(empty, "f"), Right(None))
    assertEquals(optionalInt32(doc(java.lang.Long.valueOf(1L)), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt64(doc(java.lang.Long.valueOf(7L)), "f", min = 0L), Right(7L))
    assertEquals(requiredInt64(doc(Integer.valueOf(7)), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt64(doc(java.lang.Double.valueOf(7d)), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt64(doc(java.lang.Long.valueOf(-1L)), "f", min = 0L), Left(InvalidField("f")))
    assertEquals(requiredInt64(doc("7"), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt64(doc(null), "f"), Left(MissingField("f")))
    assertEquals(optionalInt64(empty, "f"), Right(None))
    assertEquals(optionalInt64(doc(Integer.valueOf(1)), "f"), Left(InvalidField("f")))
  }

  test("integer readers accept Int32 and Int64 with a range check, and nothing else") {
    assertEquals(requiredLong(doc(Integer.valueOf(7)), "f"), Right(7L))
    assertEquals(requiredLong(doc(java.lang.Long.valueOf(Long.MaxValue)), "f"), Right(Long.MaxValue))
    assertEquals(requiredLong(doc(java.lang.Long.valueOf(Long.MinValue)), "f"), Right(Long.MinValue))
    assertEquals(requiredLong(doc(java.lang.Double.valueOf(3.0)), "f"), Left(InvalidField("f")))
    assertEquals(requiredLong(doc("1"), "f"), Left(InvalidField("f")))
    assertEquals(requiredLong(empty, "f"), Left(MissingField("f")))
    assertEquals(optionalLong(empty, "f"), Right(None))
    assertEquals(optionalLong(doc(Integer.valueOf(2)), "f"), Right(Some(2L)))
    assertEquals(requiredInt(doc(java.lang.Long.valueOf(5L)), "f"), Right(5))
    assertEquals(requiredInt(doc(java.lang.Long.valueOf(Int.MaxValue.toLong)), "f"), Right(Int.MaxValue))
    assertEquals(requiredInt(doc(java.lang.Long.valueOf(Int.MinValue.toLong)), "f"), Right(Int.MinValue))
    assertEquals(requiredInt(doc(java.lang.Long.valueOf(Int.MaxValue.toLong + 1)), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt(doc(java.lang.Long.valueOf(Int.MinValue.toLong - 1)), "f"), Left(InvalidField("f")))
    assertEquals(requiredInt(doc(java.lang.Double.valueOf(6.0)), "f"), Left(InvalidField("f")))
    assertEquals(optionalInt(doc(null), "f"), Right(None))
    assertEquals(optionalInt(doc("1"), "f"), Left(InvalidField("f")))
  }

  test("integer lower bound is inclusive") {
    assertEquals(requiredInt(doc(Integer.valueOf(0)), "f", min = 0), Right(0))
    assertEquals(requiredInt(doc(Integer.valueOf(-1)), "f", min = 0), Left(InvalidField("f")))
    assertEquals(requiredLong(doc(java.lang.Long.valueOf(0L)), "f", min = 0L), Right(0L))
    assertEquals(requiredLong(doc(java.lang.Long.valueOf(-1L)), "f", min = 0L), Left(InvalidField("f")))
    assertEquals(optionalLong(doc(java.lang.Long.valueOf(-1L)), "f", min = 0L), Left(InvalidField("f")))
    assertEquals(requiredNumberAsLong(doc(java.lang.Long.valueOf(-1L)), "f", min = 0L), Left(InvalidField("f")))
    assertEquals(requiredNumberAsLong(doc(java.lang.Long.valueOf(0L)), "f", min = 0L), Right(0L))
  }

  test("number readers accept any Number whose value is exactly integral and in range") {
    assertEquals(requiredNumberAsLong(doc(Integer.valueOf(7)), "f"), Right(7L))
    assertEquals(requiredNumberAsLong(doc(java.lang.Long.valueOf(Long.MaxValue)), "f"), Right(Long.MaxValue))
    assertEquals(requiredNumberAsLong(doc(java.lang.Long.valueOf(Long.MinValue)), "f"), Right(Long.MinValue))
    assertEquals(requiredNumberAsLong(doc(java.lang.Double.valueOf(3.0)), "f"), Right(3L))
    assertEquals(requiredNumberAsLong(doc(java.lang.Float.valueOf(2.0f)), "f"), Right(2L))
    assertEquals(
      requiredNumberAsLong(doc(java.lang.Double.valueOf(-9.223372036854775808e18)), "f"),
      Right(Long.MinValue)
    )
    assertEquals(requiredNumberAsLong(doc(new java.math.BigDecimal("4.00")), "f"), Right(4L))
    assertEquals(requiredNumberAsLong(doc(java.math.BigInteger.valueOf(9L)), "f"), Right(9L))
    assertEquals(requiredNumberAsInt(doc(java.lang.Double.valueOf(6.0)), "f"), Right(6))
    assertEquals(optionalNumberAsLong(empty, "f"), Right(None))
    assertEquals(optionalNumberAsLong(doc(Integer.valueOf(1)), "f"), Right(Some(1L)))
    assertEquals(optionalNumberAsLong(doc("1"), "f"), Left(InvalidField("f")))
    // Divergence from `Number.longValue` / `intValue`, which truncated or wrapped these silently.
    assertEquals(requiredNumberAsLong(doc(java.lang.Double.valueOf(1.5)), "f"), Left(InvalidField("f")))
    assertEquals(requiredNumberAsLong(doc(java.lang.Double.valueOf(Double.NaN)), "f"), Left(InvalidField("f")))
    assertEquals(
      requiredNumberAsLong(doc(java.lang.Double.valueOf(Double.PositiveInfinity)), "f"),
      Left(InvalidField("f"))
    )
    assertEquals(
      requiredNumberAsLong(doc(java.lang.Double.valueOf(9.223372036854775807e18)), "f"),
      Left(InvalidField("f"))
    )
    assertEquals(
      requiredNumberAsLong(doc(new java.math.BigInteger("9223372036854775808")), "f"),
      Left(InvalidField("f"))
    )
    assertEquals(requiredNumberAsLong(doc(new java.math.BigDecimal("1.5")), "f"), Left(InvalidField("f")))
    assertEquals(
      requiredNumberAsInt(doc(java.lang.Long.valueOf(Int.MaxValue.toLong + 1)), "f"),
      Left(InvalidField("f"))
    )
    assertEquals(
      requiredNumberAsInt(doc(java.lang.Long.valueOf(Int.MinValue.toLong - 1)), "f"),
      Left(InvalidField("f"))
    )
    // Non-Number BSON numerics and strings are rejected.
    assertEquals(requiredNumberAsLong(doc(new Decimal128(1L)), "f"), Left(InvalidField("f")))
    assertEquals(requiredNumberAsLong(doc("1"), "f"), Left(InvalidField("f")))
    assertEquals(requiredNumberAsLong(empty, "f"), Left(MissingField("f")))
  }

  test("instant reads java.util.Date at millisecond precision") {
    val at = Instant.parse("2026-01-02T03:04:05.678Z")
    assertEquals(requiredInstant(doc(Date.from(at)), "f"), Right(at))
    assertEquals(requiredInstant(doc(at.toString), "f"), Left(InvalidField("f")))
    assertEquals(requiredInstant(empty, "f"), Left(MissingField("f")))
    assertEquals(optionalInstant(empty, "f"), Right(None))
    assertEquals(optionalInstant(doc(Date.from(at)), "f"), Right(Some(at)))
    assertEquals(optionalInstant(doc(5L), "f"), Left(InvalidField("f")))
  }

  test("uuid accepts only the canonical lower-case form") {
    val id = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
    assertEquals(requiredUuid(doc(id.toString), "f"), Right(id))
    assertEquals(requiredUuid(doc(id.toString.toUpperCase), "f"), Left(InvalidField("f")))
    assertEquals(requiredUuid(doc("1-1-1-1-1"), "f"), Left(InvalidField("f")))
    assertEquals(requiredUuid(doc("nope"), "f"), Left(InvalidField("f")))
    assertEquals(requiredUuid(doc(id), "f"), Left(InvalidField("f")))
    assertEquals(requiredUuid(empty, "f"), Left(MissingField("f")))
    assertEquals(optionalUuid(empty, "f"), Right(None))
    assertEquals(optionalUuid(doc("1-1-1-1-1"), "f"), Left(InvalidField("f")))
    assertEquals(parseUuid("g", id.toString), Right(id))
    assertEquals(parseUuid("g", "x"), Left(InvalidField("g")))
  }

  test("string lists require every element to be a string") {
    assertEquals(requiredStringVector(doc(java.util.Arrays.asList("a", "b")), "f"), Right(Vector("a", "b")))
    assertEquals(requiredStringList(doc(java.util.Arrays.asList("a")), "f"), Right(List("a")))
    assertEquals(requiredStringVector(doc(new java.util.ArrayList[String]()), "f"), Right(Vector.empty))
    assertEquals(
      requiredStringVector(doc(java.util.Arrays.asList("a", Integer.valueOf(1))), "f"),
      Left(InvalidField("f"))
    )
    assertEquals(requiredStringVector(doc("a"), "f"), Left(InvalidField("f")))
    assertEquals(requiredStringVector(empty, "f"), Left(MissingField("f")))
    assertEquals(optionalStringVector(empty, "f"), Right(None))
    assertEquals(optionalStringVector(doc(java.util.Arrays.asList("a")), "f"), Right(Some(Vector("a"))))
    assertEquals(optionalStringVector(doc(5), "f"), Left(InvalidField("f")))
  }

  test("nested documents and document arrays") {
    val nested = new Document("k", 1)
    assertEquals(requiredDocument(doc(nested), "f"), Right(nested))
    assertEquals(requiredDocument(doc("x"), "f"), Left(InvalidField("f")))
    assertEquals(requiredDocument(empty, "f"), Left(MissingField("f")))
    assertEquals(optionalDocument(empty, "f"), Right(None))
    assertEquals(optionalDocument(doc(3), "f"), Left(InvalidField("f")))
    assertEquals(requiredDocumentVector(doc(java.util.Arrays.asList(nested)), "f"), Right(Vector(nested)))
    assertEquals(requiredDocumentVector(doc(java.util.Arrays.asList(nested, "x")), "f"), Left(InvalidField("f")))
    assertEquals(requiredDocumentVector(empty, "f"), Left(MissingField("f")))
  }

  test("enum readers resolve by name through the provided parser") {
    enum Color { case Red, Blue }
    val parse = byName(Color.values)
    assertEquals(requiredEnum(doc("Red"), "f")(parse), Right(Color.Red))
    assertEquals(requiredEnum(doc("red"), "f")(parse), Left(InvalidField("f")))
    assertEquals(requiredEnum(doc(1), "f")(parse), Left(InvalidField("f")))
    assertEquals(requiredEnum(empty, "f")(parse), Left(MissingField("f")))
    assertEquals(optionalEnum(empty, "f")(parse), Right(None))
    assertEquals(optionalEnum(doc("Blue"), "f")(parse), Right(Some(Color.Blue)))
    assertEquals(optionalEnum(doc("Green"), "f")(parse), Left(InvalidField("f")))
  }

  test("toRepository collapses field detail to InvalidStoredData") {
    assertEquals(toRepository(requiredString(empty, "f")), Left(RepositoryError.InvalidStoredData))
    assertEquals(toRepository(requiredString(doc("x"), "f")), Right("x"))
  }

  test("MongoDuplicateKey matches only duplicate-key write failures") {
    def writeFailure(code: Int) =
      new MongoWriteException(
        new WriteError(code, "failure", new BsonDocument()),
        new ServerAddress(),
        java.util.Collections.emptySet[String]()
      )
    val duplicate = writeFailure(11000)
    assertEquals(MongoDuplicateKey.unapply(duplicate), Some(duplicate))
    assertEquals(MongoDuplicateKey.unapply(writeFailure(112)), None)
    assertEquals(MongoDuplicateKey.unapply(new RuntimeException("11000")), None)
    (duplicate: Throwable) match {
      case MongoDuplicateKey(found) => assertEquals(found, duplicate)
      case other                    => fail(s"unexpected $other")
    }
  }
}
