package com.example.graphQL.cats.service.search

import cats.effect.{IO, IOApp}
import io.circe.Json
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets

/** Exports fabricated source material for actual human review; it never marks judgments reviewed. */
object SearchEvaluationJudgmentReview extends IOApp.Simple {
  val packet: Json = Json.obj(
    "corpusIdentity" -> Json.fromString(SearchEvaluationFixtures.corpus.identity),
    "corpusDigest" -> Json.fromString(SearchEvaluationFixtures.corpus.digest),
    "reviewStatus" -> Json.fromString("Pending human domain review"),
    "rubric" -> Json.fromString("Binary intended role and all required skills, within eligible IDs"),
    "queries" -> Json.fromValues(SearchEvaluationFixtures.queries.map { query =>
      val intent = SearchEvaluationFixtures.intents.find(_.queryId == query.queryId)
      val entities =
        if (query.useCase == SearchEvaluationUseCase.RecruiterMatching)
          SearchEvaluationFixtures.candidates
        else SearchEvaluationFixtures.jobs
      Json.obj(
        "queryId" -> Json.fromString(query.queryId),
        "useCase" -> Json.fromString(query.useCase.toString),
        "split" -> Json.fromString(query.split.toString),
        "filterGroup" -> Json.fromString(query.filterGroup.toString),
        "filterIdentity" -> Json.fromString(query.filterIdentity),
        "intendedRole" -> intent.fold(Json.Null)(value => Json.fromString(value.intendedRole)),
        "requiredSkills" -> Json.fromValues(intent.toList.flatMap(_.requiredSkills.toList.sorted).map(Json.fromString)),
        "entities" -> Json.fromValues(entities.map { entity =>
          Json.obj(
            "id" -> Json.fromString(entity.id),
            "role" -> Json.fromString(entity.intendedRole),
            "skills" -> Json.fromValues(entity.skills.toList.sorted.map(Json.fromString)),
            "fabricatedSummary" -> Json.fromString(entity.summary),
            "eligible" -> Json.fromBoolean(query.eligibleIds.contains(entity.id)),
            "relevant" -> Json.fromBoolean(query.relevantIds.contains(entity.id)),
            "active" -> Json.fromBoolean(entity.active),
            "deleted" -> Json.fromBoolean(entity.deleted),
            "staleEmbedding" -> Json.fromBoolean(entity.stale),
            "consent" -> entity.optIn.fold(Json.Null)(Json.fromBoolean),
            "privateAttributesPresent" -> Json.fromBoolean(entity.privateAttributesPresent),
            "eligibilityRationale" -> Json.fromString(
              if (entity.deleted) "Deleted source is excluded"
              else if (!entity.active) "Closed job or inactive account is excluded"
              else if (entity.stale) "Stored embedding source hash differs from current source"
              else if (query.filterGroup == SearchEvaluationFilterGroup.Empty)
                "No entity has the requested nonexistent fixture skill"
              else if (
                query.filterGroup == SearchEvaluationFilterGroup.Selective && !query.eligibleIds.contains(entity.id)
              )
                "Required public skills or opted-in private predicates do not match"
              else "Current active source satisfies declared public/private eligibility predicates"
            ),
            "rationale" -> Json.fromString(
              intent
                .flatMap(_.relevanceRationales.get(entity.id))
                .getOrElse("Outside declared eligibility; no relevance label counted")
            )
          )
        })
      )
    })
  )

  override def run: IO[Unit] = IO.blocking {
    val directory = Path.of(".local/data/search-evaluation/judgment-review")
    Files.createDirectories(directory)
    Files.writeString(
      directory.resolve(s"${SearchEvaluationFixtures.corpus.digest}.json"),
      packet.spaces2,
      StandardCharsets.UTF_8
    )
    ()
  } *> IO.println(s"Pending human review packet: ${SearchEvaluationFixtures.corpus.digest}")
}
