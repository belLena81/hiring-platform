package com.example.graphQL.cats.domain.model

import com.example.graphQL.cats.domain.model.Identifiers.JobId

/** The job facts required to decide and atomically guard an application submission. */
final case class JobSubmissionSnapshot(id: JobId, status: JobStatus, revision: Long)
