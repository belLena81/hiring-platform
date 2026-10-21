package com.example.graphQL.cats.domain.model

import com.example.graphQL.cats.domain.model.Identifiers.JobId

/** The job identity and status required to decide an application submission. */
final case class JobSubmissionSnapshot(id: JobId, status: JobStatus)
