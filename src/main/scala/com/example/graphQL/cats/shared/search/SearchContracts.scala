package com.example.graphQL.cats.shared.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, Job, SearchMode, User}
import com.example.graphQL.cats.shared.pagination.PageSize
import java.time.Instant
import java.util.UUID

final case class JobSearchFilter(
    city: Option[String],
    skills: Set[String],
    createdAfter: Option[Instant]
)

final case class VectorSearchQuery(
    vector: List[Float],
    filter: JobSearchFilter,
    first: PageSize,
    mode: SearchMode,
    model: String,
    version: Int,
    searchId: UUID
)

final case class RankedJob(job: Job, score: Double, mode: SearchMode, meta: EmbeddingMeta, searchId: UUID)
final case class RankedCandidate(candidate: User, score: Double, mode: SearchMode, meta: EmbeddingMeta, searchId: UUID)
