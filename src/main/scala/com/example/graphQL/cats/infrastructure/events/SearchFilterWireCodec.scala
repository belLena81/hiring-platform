package com.example.graphQL.cats.infrastructure.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.events.SearchFilterSnapshot
import io.circe.Json
import io.circe.parser.parse
import java.time.Instant
import com.example.graphQL.cats.shared.Parsing.parseUuid

/** Wire form of a stored search filter: exactly the three shapes the search resolvers have written. */
object SearchFilterWireCodec {
  private val CriteriaKeys = Set("city", "skills", "createdAfter")
  private val JobKeys = Set("jobId")

  def encode(value: SearchFilterSnapshot): Json = value match {
    case SearchFilterSnapshot.JobCriteria(city, skills, createdAfter) =>
      Json.obj(
        "city" -> city.fold(Json.Null)(Json.fromString),
        "skills" -> Json.arr(skills.map(Json.fromString)*),
        "createdAfter" -> createdAfter.fold(Json.Null)(instant => Json.fromString(instant.toString))
      )
    case SearchFilterSnapshot.Unfiltered => Json.obj()
    case SearchFilterSnapshot.ForJob(id) => Json.obj("jobId" -> Json.fromString(id.value.toString))
  }

  def encodeString(value: SearchFilterSnapshot): String =
    encode(value).noSpaces

  def decodeString(raw: String): Either[Unit, SearchFilterSnapshot] =
    parse(raw).leftMap(_ => ()).flatMap(decode)

  def decode(json: Json): Either[Unit, SearchFilterSnapshot] =
    json.asObject.toRight(()).flatMap { fields =>
      val keys = fields.keys.toSet
      if (keys.isEmpty) Right(SearchFilterSnapshot.Unfiltered)
      else if (keys == JobKeys)
        fields("jobId")
          .flatMap(_.asString)
          .flatMap(raw => parseUuid(raw).toOption)
          .map(id => SearchFilterSnapshot.ForJob(JobId(id)))
          .toRight(())
      else if (keys == CriteriaKeys) criteria(fields)
      else Left(())
    }

  private def criteria(fields: io.circe.JsonObject): Either[Unit, SearchFilterSnapshot] =
    for {
      city <- optionalString(fields("city"))
      skills <- fields("skills").flatMap(_.asArray).toRight(()).flatMap(_.toList.traverse(_.asString.toRight(())))
      createdAfter <- optionalString(fields("createdAfter")).flatMap(
        _.traverse(raw => Either.catchNonFatal(Instant.parse(raw)).leftMap(_ => ()))
      )
    } yield SearchFilterSnapshot.JobCriteria(city, skills, createdAfter)

  private def optionalString(value: Option[Json]): Either[Unit, Option[String]] =
    value match {
      case Some(json) if json.isNull => Right(None)
      case Some(json)                => json.asString.map(Some(_)).toRight(())
      case None                      => Left(())
    }
}
