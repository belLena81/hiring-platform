package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.repository.mongo.MongoHiringCodecs.StoredDocumentError
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.shared.Parsing
import org.bson.Document

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Total readers for fields of untrusted stored BSON documents.
  *
  * Every reader returns a value; none throws. A failure names the offending field:
  *   - `MissingField` when the field is absent or an explicit BSON null;
  *   - `InvalidField` when it is present with an unexpected type, value or range.
  *
  * Numeric rules, strictest first. Stored numeric types are part of the persisted shape, so a reader names the
  * tolerance it needs:
  *   - `int32` / `int64`: exactly that BSON type (Java `Integer` / `Long`); anything else is `InvalidField`.
  *   - `int` / `long`: either integer BSON type (Int32 or Int64) with a range check on narrowing to `Int`; Double and
  *     Decimal values are `InvalidField`.
  *   - `numberAsInt` / `numberAsLong`: any `java.lang.Number` (aggregation `$sum`/`$count` results, fields never
  *     constrained by a collection validator) whose value is exactly integral and in range. Fractional, NaN, infinite
  *     and out-of-range numbers are `InvalidField`; the readers they replace truncated or wrapped them silently.
  * Each integer reader takes an inclusive `min`.
  *
  * UUID rule: only the canonical lower-case 8-4-4-4-12 form is accepted, the same form the collection validators
  * enforce. `java.util.UUID.fromString` alone also accepts short and upper-case forms.
  *
  * Use [[toRepository]] at a repository boundary to collapse field detail into `RepositoryError.InvalidStoredData`.
  */
private[mongo] object MongoDocumentFields {
  type Read[A] = Either[StoredDocumentError, A]

  private def present(document: Document, field: String): Option[AnyRef] = Option(document.get(field))

  private def required[A](document: Document, field: String)(extract: PartialFunction[AnyRef, A]): Read[A] =
    present(document, field) match {
      case None        => Left(StoredDocumentError.MissingField(field))
      case Some(value) => extract.lift(value).toRight(StoredDocumentError.InvalidField(field))
    }

  private def optionalOf[A](document: Document, field: String)(extract: PartialFunction[AnyRef, A]): Read[Option[A]] =
    present(document, field) match {
      case None        => Right(None)
      case Some(value) => extract.lift(value).map(Some(_)).toRight(StoredDocumentError.InvalidField(field))
    }

  /** Collapses field detail where the caller only reports an unreadable stored document. */
  def toRepository[A](read: Read[A]): Either[RepositoryError, A] = read.leftMap(_ => RepositoryError.InvalidStoredData)

  def requiredString(document: Document, field: String): Read[String] =
    required(document, field) { case value: String => value }
  def optionalString(document: Document, field: String): Read[Option[String]] =
    optionalOf(document, field) { case value: String => value }

  def requiredBoolean(document: Document, field: String): Read[Boolean] =
    required(document, field) { case value: java.lang.Boolean => value.booleanValue }
  def optionalBoolean(document: Document, field: String): Read[Option[Boolean]] =
    optionalOf(document, field) { case value: java.lang.Boolean => value.booleanValue }

  def requiredDouble(document: Document, field: String): Read[Double] =
    required(document, field) { case value: Number => value.doubleValue }
  def optionalDouble(document: Document, field: String): Read[Option[Double]] =
    optionalOf(document, field) { case value: Number => value.doubleValue }

  def requiredInt32(document: Document, field: String, min: Int = Int.MinValue): Read[Int] =
    required(document, field) { case value: java.lang.Integer => value.intValue }.flatMap(atLeast(_, field, min))
  def optionalInt32(document: Document, field: String, min: Int = Int.MinValue): Read[Option[Int]] =
    optionalOf(document, field) { case value: java.lang.Integer => value.intValue }.flatMap(
      _.traverse(atLeast(_, field, min))
    )

  def requiredInt64(document: Document, field: String, min: Long = Long.MinValue): Read[Long] =
    required(document, field) { case value: java.lang.Long => value.longValue }.flatMap(atLeast(_, field, min))
  def optionalInt64(document: Document, field: String, min: Long = Long.MinValue): Read[Option[Long]] =
    optionalOf(document, field) { case value: java.lang.Long => value.longValue }.flatMap(
      _.traverse(atLeast(_, field, min))
    )

  def requiredLong(document: Document, field: String, min: Long = Long.MinValue): Read[Long] =
    required(document, field) {
      case value: java.lang.Integer => value.longValue
      case value: java.lang.Long    => value.longValue
    }.flatMap(atLeast(_, field, min))
  def optionalLong(document: Document, field: String, min: Long = Long.MinValue): Read[Option[Long]] =
    optionalOf(document, field) {
      case value: java.lang.Integer => value.longValue
      case value: java.lang.Long    => value.longValue
    }.flatMap(_.traverse(atLeast(_, field, min)))

  def requiredInt(document: Document, field: String, min: Int = Int.MinValue): Read[Int] =
    requiredLong(document, field, min.toLong).flatMap(toInt(_, field))
  def optionalInt(document: Document, field: String, min: Int = Int.MinValue): Read[Option[Int]] =
    optionalLong(document, field, min.toLong).flatMap(_.traverse(toInt(_, field)))

  def requiredNumberAsLong(document: Document, field: String, min: Long = Long.MinValue): Read[Long] =
    required(document, field) { case value: Number => value }
      .flatMap(wholeNumber(_, field))
      .flatMap(atLeast(_, field, min))
  def optionalNumberAsLong(document: Document, field: String, min: Long = Long.MinValue): Read[Option[Long]] =
    optionalOf(document, field) { case value: Number => value }.flatMap(
      _.traverse(wholeNumber(_, field).flatMap(atLeast(_, field, min)))
    )

  def requiredNumberAsInt(document: Document, field: String, min: Int = Int.MinValue): Read[Int] =
    requiredNumberAsLong(document, field, min.toLong).flatMap(toInt(_, field))

  def requiredInstant(document: Document, field: String): Read[Instant] =
    required(document, field) { case value: java.util.Date => value.toInstant }
  def optionalInstant(document: Document, field: String): Read[Option[Instant]] =
    optionalOf(document, field) { case value: java.util.Date => value.toInstant }

  def requiredUuid(document: Document, field: String): Read[UUID] =
    requiredString(document, field).flatMap(parseUuid(field, _))
  def optionalUuid(document: Document, field: String): Read[Option[UUID]] =
    optionalString(document, field).flatMap(_.traverse(parseUuid(field, _)))

  /** Canonical-form UUID check for an already extracted string. */
  def parseUuid(field: String, raw: String): Read[UUID] =
    Parsing.parseUuid(raw).toOption.filter(_.toString == raw).toRight(StoredDocumentError.InvalidField(field))

  /** A BSON array whose every element is a string. */
  def requiredStringVector(document: Document, field: String): Read[Vector[String]] =
    required(document, field) { case values: java.util.List[?] => values }.flatMap(strings(_, field))
  def optionalStringVector(document: Document, field: String): Read[Option[Vector[String]]] =
    optionalOf(document, field) { case values: java.util.List[?] => values }.flatMap(_.traverse(strings(_, field)))
  def requiredStringList(document: Document, field: String): Read[List[String]] =
    requiredStringVector(document, field).map(_.toList)

  def requiredDocument(document: Document, field: String): Read[Document] =
    required(document, field) { case value: Document => value }
  def optionalDocument(document: Document, field: String): Read[Option[Document]] =
    optionalOf(document, field) { case value: Document => value }

  /** A BSON array whose every element is a nested document. */
  def requiredDocumentVector(document: Document, field: String): Read[Vector[Document]] =
    required(document, field) { case values: java.util.List[?] => values }.flatMap(
      _.asScala.toVector.traverse {
        case value: Document => Right(value)
        case _               => Left(StoredDocumentError.InvalidField(field))
      }
    )

  /** A string field naming a case through `parse`; an unknown name is `InvalidField`. */
  def requiredEnum[A](document: Document, field: String)(parse: String => Option[A]): Read[A] =
    requiredString(document, field).flatMap(parse(_).toRight(StoredDocumentError.InvalidField(field)))
  def optionalEnum[A](document: Document, field: String)(parse: String => Option[A]): Read[Option[A]] =
    optionalString(document, field).flatMap(_.traverse(parse(_).toRight(StoredDocumentError.InvalidField(field))))

  /** Enum lookup by `toString`, the persisted name of every closed state. */
  def byName[A](values: Array[A])(name: String): Option[A] = values.find(_.toString == name)

  private def strings(values: java.util.List[?], field: String): Read[Vector[String]] =
    values.asScala.toVector.traverse {
      case value: String => Right(value)
      case _             => Left(StoredDocumentError.InvalidField(field))
    }

  private def atLeast[A](value: A, field: String, min: A)(using ordering: Ordering[A]): Read[A] =
    Either.cond(ordering.gteq(value, min), value, StoredDocumentError.InvalidField(field))

  private def wholeNumber(number: Number, field: String): Read[Long] = {
    val integral: Option[Long] = number match {
      case v @ (_: java.lang.Integer | _: java.lang.Long | _: java.lang.Short | _: java.lang.Byte) => Some(v.longValue)
      case v @ (_: java.lang.Double | _: java.lang.Float)                                          =>
        val d = v.doubleValue
        // 2^63 is exactly representable; Long.MaxValue.toDouble rounds up to it, so compare strictly below.
        Option.when(
          !d.isNaN && !d.isInfinite && d == Math.rint(d) && d >= -9.223372036854775808e18 && d < 9.223372036854775808e18
        )(
          d.toLong
        )
      case v: java.math.BigInteger => Option.when(v.bitLength < 64)(v.longValue)
      case v: java.math.BigDecimal =>
        Option
          .when(v.signum == 0 || v.stripTrailingZeros.scale <= 0)(v.toBigInteger)
          .filter(_.bitLength < 64)
          .map(_.longValue)
      case _ => None
    }
    integral.toRight(StoredDocumentError.InvalidField(field))
  }

  private def toInt(value: Long, field: String): Read[Int] =
    Either.cond(value >= Int.MinValue && value <= Int.MaxValue, value.toInt, StoredDocumentError.InvalidField(field))

  /** The same readers collapsed to `InvalidStoredData` for repository-boundary decoding. */
  object Repository {
    def string(document: Document, field: String): Either[RepositoryError, String] =
      toRepository(requiredString(document, field))
    def instant(document: Document, field: String): Either[RepositoryError, Instant] =
      toRepository(requiredInstant(document, field))
    def optionalInstant(document: Document, field: String): Either[RepositoryError, Option[Instant]] =
      toRepository(MongoDocumentFields.optionalInstant(document, field))
    def stringList(document: Document, field: String): Either[RepositoryError, List[String]] =
      toRepository(requiredStringList(document, field))
    def stringVector(document: Document, field: String): Either[RepositoryError, Vector[String]] =
      toRepository(requiredStringVector(document, field))
    def int32(document: Document, field: String): Either[RepositoryError, Int] =
      toRepository(requiredInt32(document, field))
    def int64(document: Document, field: String): Either[RepositoryError, Long] =
      toRepository(requiredInt64(document, field))
  }
}
