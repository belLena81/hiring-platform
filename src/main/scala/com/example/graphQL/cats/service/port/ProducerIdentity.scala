package com.example.graphQL.cats.service.port

import java.util.UUID

/** The stable operator-chosen name of one producing process slot (`hiring-publisher-<worker id>`). A replacement
  * process of the same slot reuses it, which is what lets the broker fence the previous lifetime on restart.
  */
final case class ProducerWorkerId private (value: String)

object ProducerWorkerId {

  /** Lowercase alphanumerics and hyphens, at most 64 characters, starting alphanumeric. */
  val Pattern = "[a-z0-9][a-z0-9-]{0,63}"

  def parse(raw: String): Either[String, ProducerWorkerId] =
    Either.cond(raw.matches(Pattern), ProducerWorkerId(raw), s"worker id must match $Pattern")
}

/** One initialized producer lifetime. Fenced means this generation is dead forever; the transactional id lives on. */
final case class ProducerGenerationId(value: UUID)

final case class ProducerGeneration(transactionalId: String, generationId: ProducerGenerationId)

/** The one parser of the transactional id format: a known role prefix followed by a worker id. A legacy id of the form
  * prefix plus lowercase UUID still matches the worker id pattern, so migrated registry rows stay valid. A worker id
  * cannot contain a colon, so the registry key subject:transactionalId:generationId is unambiguous.
  */
object ProducerTransactionalId {
  val PublisherPrefix = "hiring-publisher-"
  val Prefixes: List[String] = PublisherPrefix :: InterviewPublisherRole.values.toList.map(_.transactionalIdPrefix)

  def parse(id: String): Either[String, (String, ProducerWorkerId)] =
    Prefixes
      .find(id.startsWith)
      .toRight("unknown transactional id prefix")
      .flatMap(prefix => ProducerWorkerId.parse(id.substring(prefix.length)).map(prefix -> _))
}
