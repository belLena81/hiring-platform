package com.example.graphQL.cats.service.events

import com.example.graphQL.cats.domain.model.Identifiers.JobId
import java.time.Instant

/** The criteria a search session was run with, kept as a closed set of shapes rather than free-form JSON. */
enum SearchFilterSnapshot {

  /** Job discovery criteria; `skills` are in sorted order so equal criteria have one representation. */
  case JobCriteria(city: Option[String], skills: List[String], createdAfter: Option[Instant])

  /** Searches without caller-supplied criteria, such as recommended jobs. */
  case Unfiltered

  /** Candidate matching for one job. */
  case ForJob(jobId: JobId)
}

object SearchFilterSnapshot {
  def jobCriteria(city: Option[String], skills: Set[String], createdAfter: Option[Instant]): SearchFilterSnapshot =
    JobCriteria(city, skills.toList.sorted, createdAfter)
}
