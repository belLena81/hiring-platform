package com.example.graphQL.cats.domain.model

import java.time.Instant

enum SearchMode {
  case FILTER, VECTOR, HYBRID
}

final case class EmbeddingMeta(
    model: String,
    version: Int,
    sourceHash: String,
    updatedAt: Instant
)

final case class EntityEmbedding(values: List[Float], meta: EmbeddingMeta)

object SearchableText {
  val QueryMaxChars: Int = 2048
  val DocumentMaxChars: Int = 12000

  def job(job: Job): String =
    List(
      job.title,
      job.description,
      "Requirements:",
      job.requirements.mkString("\n"),
      "Skills:",
      job.skills.toList.sorted.mkString(", ")
    ).mkString("\n").trim

  def candidate(profile: CandidateProfile): String =
    List(
      profile.experienceSummary.getOrElse(""),
      "Skills:",
      profile.skills.toList.sorted.mkString(", ")
    ).filter(_.nonEmpty).mkString("\n").trim
}
