package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{Job, SearchTerm}
import java.time.Instant

/** How the `skills` filter of an open-job listing matches: every skill (the default) or at least one. */
enum SkillMatchMode {
  case AllOf
  case AnyOf
}

/** A filter the recovery of an empty search relaxed, with what it was; a closed set, never a filter name string. */
enum RelaxedFilter {

  /** The lower bound on the creation time was removed for every job; `from` was the inclusive bound the caller sent. */
  case CreatedAfterDropped(from: Instant)

  /** The skills filter changed from all-of to any-of, over the skills the caller sent. */
  case SkillsAllOfToAnyOf(skills: Set[String])
}

/** One page of open jobs and the filters that had to be relaxed to find it; empty when the page is the caller's own. */
final case class OpenJobsPage(jobs: List[Job], relaxedFilters: List[RelaxedFilter])

/** One extra search: the filter to run and the relaxation it stands for. */
final case class RelaxationAttempt(filter: JobSearchFilter, relaxed: RelaxedFilter)

/** The pure decision of what to try when a filtered open-job search finds nothing. The order is fixed: drop
  * `createdAfter`, then widen `skills` to any-of. Each attempt relaxes exactly one filter of the original, a city is
  * never relaxed, and an unfiltered search has nothing to relax.
  */
object ZeroResultRecovery {
  private val MaxAttempts = 2

  def plan(filter: JobSearchFilter): List[RelaxationAttempt] = {
    val dropCreatedAfter = filter.createdAfter.map(from =>
      RelaxationAttempt(filter.copy(createdAfter = None), RelaxedFilter.CreatedAfterDropped(from))
    )
    // One distinct skill (after canonicalisation, so "Scala" and "scala" are one) reads the same all-of and any-of,
    // so the widened search would repeat the original.
    val widenSkills = Option.when(
      filter.skillMode == SkillMatchMode.AllOf && SearchTerm.canonicalSorted(filter.skills).size > 1
    )(
      RelaxationAttempt(
        filter.copy(skillMode = SkillMatchMode.AnyOf),
        RelaxedFilter.SkillsAllOfToAnyOf(filter.skills)
      )
    )
    List(dropCreatedAfter, widenSkills).flatten.take(MaxAttempts)
  }
}
