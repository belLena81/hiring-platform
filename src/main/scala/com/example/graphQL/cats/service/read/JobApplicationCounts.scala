package com.example.graphQL.cats.service.read

import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.JobId

/** How many applications a job holds in each status. Every status is present, so a reader never distinguishes "none"
  * from "not reported".
  */
final case class JobApplicationCounts private (jobId: JobId, byStatus: Map[ApplicationStatus, Int]) {
  def total: Int = byStatus.values.sum
}

object JobApplicationCounts {

  /** Statuses the store reported nothing for count as zero; negative counts cannot occur and are clamped. */
  def of(jobId: JobId, reported: Map[ApplicationStatus, Int]): JobApplicationCounts =
    JobApplicationCounts(
      jobId,
      ApplicationStatus.values.map(status => status -> reported.getOrElse(status, 0).max(0)).toMap
    )
}
