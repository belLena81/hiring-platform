package com.example.hiring.analytics

import io.circe.{Json, JsonObject}
import io.circe.parser.parse
import java.nio.charset.StandardCharsets
import java.util.UUID

/** Generates complete current hiring facts from small scenario-specific fixture inputs. */
object AnalyticsOperationalEventFixtures {
  def id(value: String): String =
    if (value.trim.isEmpty) value
    else
      scala.util
        .Try(UUID.fromString(value))
        .fold(
          _ => UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8)).toString,
          _.toString
        )

  private def identifier(value: Json): Json = value.asString.fold(value)(text => Json.fromString(id(text)))
  private def normalizeIdentifiers(value: Json): Json = value.arrayOrObject(
    value,
    values => Json.fromValues(values.map(normalizeIdentifiers)),
    fields =>
      Json.fromJsonObject(JsonObject.fromIterable(fields.toIterable.map { case (key, field) =>
        key -> (if (
                  Set(
                    "eventId",
                    "actorId",
                    "aggregateId",
                    "applicationId",
                    "candidateId",
                    "jobId",
                    "searchId",
                    "resultId"
                  ).contains(key)
                ) identifier(field)
                else normalizeIdentifiers(field))
      }))
  )

  def payload(eventType: String, aggregateId: String, supplied: Json): Json = {
    def field(name: String, fallback: Json): Json = supplied.hcursor.downField(name).focus.getOrElse(fallback)
    def text(value: String): Json = Json.fromString(value)
    val appId = field("applicationId", text(aggregateId))
    val candidate = field("candidateId", text("candidate-1"))
    val job = field("jobId", text("job-1"))
    val result = eventType match {
      case "JOB_CREATED" | "JOB_UPDATED" | "JOB_CLOSED" =>
        val skills = supplied.hcursor
          .downField("job")
          .get[List[String]]("skills")
          .getOrElse(Nil)
          .map(_.trim)
          .filter(_.nonEmpty)
          .distinct
        Json.obj(
          "job" -> Json.obj(
            "jobId" -> text(aggregateId),
            "skills" -> Json.fromValues(skills.map(text)),
            "status" -> text(if (eventType == "JOB_CLOSED") "Closed" else "Open")
          )
        )
      case "APPLICATION_CREATED" | "CANDIDATE_HIRED" =>
        Json.obj(
          "applicationId" -> appId,
          "candidateId" -> candidate,
          "jobId" -> job,
          "status" -> text(if (eventType == "APPLICATION_CREATED") "Created" else "Hired")
        )
      case "APPLICATION_STATUS_CHANGED" =>
        Json.obj(
          "applicationId" -> appId,
          "candidateId" -> candidate,
          "jobId" -> job,
          "previousStatus" -> field("previousStatus", text("Interview")),
          "newStatus" -> field("newStatus", text("Hired"))
        )
      case "SEARCH_PERFORMED" =>
        val results = supplied.hcursor
          .get[List[Json]]("results")
          .getOrElse(Nil)
          .distinctBy(_.hcursor.get[String]("resultId").getOrElse(""))
          .zipWithIndex
          .map { case (result, index) =>
            Json.obj(
              "resultId" -> result.hcursor.downField("resultId").focus.getOrElse(text("job-1")),
              "rank" -> Json.fromInt(index + 1),
              "score" -> result.hcursor.downField("score").focus.getOrElse(Json.fromDoubleOrNull(0.5))
            )
          }
        Json.obj(
          "searchId" -> text(aggregateId),
          "searchKind" -> field("searchKind", text("jobs")),
          "results" -> Json.fromValues(results)
        )
      case "JOB_VIEWED" =>
        Json.obj(
          "searchId" -> Json.Null,
          "resultId" -> text(aggregateId),
          "searchKind" -> Json.Null,
          "rank" -> Json.Null
        )
      case "SEARCH_RESULT_CLICKED" =>
        Json.obj(
          "searchId" -> text(aggregateId),
          "resultId" -> field("resultId", text("job-1")),
          "searchKind" -> field("searchKind", text("jobs")),
          "rank" -> field("rank", Json.fromInt(1))
        )
      case _ => supplied
    }
    normalizeIdentifiers(result)
  }

  def complete(raw: String): String = parse(raw).fold(
    _ => raw,
    json => {
      val cursor = json.hcursor
      val eventType = cursor.get[String]("eventType").getOrElse("")
      val aggregateId = cursor.get[String]("aggregateId").getOrElse("")
      val supplied = cursor.downField("payload").focus.getOrElse(Json.obj())
      val fact = payload(eventType, aggregateId, supplied)
      val identity =
        if (Set("APPLICATION_CREATED", "APPLICATION_STATUS_CHANGED", "CANDIDATE_HIRED").contains(eventType))
          fact.hcursor.downField("applicationId").focus.getOrElse(Json.fromString(aggregateId))
        else Json.fromString(aggregateId)
      val completed = json.mapObject(_.add("payload", fact).add("aggregateId", identity))
      normalizeIdentifiers(completed).noSpaces
    }
  )
}
