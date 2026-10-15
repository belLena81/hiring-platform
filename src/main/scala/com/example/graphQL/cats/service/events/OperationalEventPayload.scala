package com.example.graphQL.cats.service.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus}
import java.util.UUID
import com.example.graphQL.cats.shared.Parsing.parseUuid

enum OperationalEventContractError {
  case MalformedEnvelope, InvalidPayload, AggregateMismatch, EnvelopeTooLarge
}

/** The payload ADT owns business facts; the seven-field envelope is a wire DTO. */
enum OperationalEventPayload {
  import OperationalEventPayload.*
  case JobFact(kind: OperationalEventPayload.JobKind, job: OperationalEventPayload.JobSnapshot)
  case ApplicationCreated(applicationId: UUID, candidateId: UUID, jobId: UUID, status: ApplicationStatus)
  case StatusChanged(
      applicationId: UUID,
      candidateId: UUID,
      jobId: UUID,
      previousStatus: Option[ApplicationStatus],
      newStatus: ApplicationStatus
  )
  case CandidateHired(applicationId: UUID, candidateId: UUID, jobId: UUID, status: ApplicationStatus)
  case SearchPerformed(
      searchId: UUID,
      searchKind: OperationalEventPayload.SearchKind,
      results: List[SearchSessionResult]
  )
  case JobViewed(
      searchId: Option[UUID],
      resultId: UUID,
      searchKind: Option[OperationalEventPayload.SearchKind],
      rank: Option[Int]
  )
  case SearchResultClicked(searchId: UUID, resultId: UUID, searchKind: OperationalEventPayload.SearchKind, rank: Int)

  def eventType: OperationalEventType = this match {
    case JobFact(kind, _)       => kind.eventType
    case _: ApplicationCreated  => OperationalEventType.APPLICATION_CREATED
    case _: StatusChanged       => OperationalEventType.APPLICATION_STATUS_CHANGED
    case _: CandidateHired      => OperationalEventType.CANDIDATE_HIRED
    case _: SearchPerformed     => OperationalEventType.SEARCH_PERFORMED
    case _: JobViewed           => OperationalEventType.JOB_VIEWED
    case _: SearchResultClicked => OperationalEventType.SEARCH_RESULT_CLICKED
  }

  def aggregateType: OperationalAggregateType = this match {
    case _: JobFact                                                   => OperationalAggregateType.Job
    case _: ApplicationCreated | _: StatusChanged | _: CandidateHired => OperationalAggregateType.Application
    case _: SearchPerformed | _: JobViewed | _: SearchResultClicked   => OperationalAggregateType.Search
  }

  /** The aggregate this fact belongs to; its string form is the envelope's `aggregateId` and partition key. */
  def aggregateId: UUID = this match {
    case JobFact(_, job)                  => job.jobId
    case ApplicationCreated(id, _, _, _)  => id
    case StatusChanged(id, _, _, _, _)    => id
    case CandidateHired(id, _, _, _)      => id
    case SearchPerformed(id, _, _)        => id
    case JobViewed(_, id, _, _)           => id
    case SearchResultClicked(id, _, _, _) => id
  }

  def subjectIds: Either[OperationalEventContractError, List[UUID]] = this match {
    case ApplicationCreated(_, candidate, _, _)                   => Right(List(candidate))
    case StatusChanged(_, candidate, _, _, _)                     => Right(List(candidate))
    case CandidateHired(_, candidate, _, _)                       => Right(List(candidate))
    case SearchPerformed(_, SearchKind.CandidateMatches, results) =>
      results.traverse(result => parseUuid(result.resultId).leftMap(_ => OperationalEventContractError.InvalidPayload))
    case SearchResultClicked(_, candidate, SearchKind.CandidateMatches, _) => Right(List(candidate))
    case _                                                                 => Right(Nil)
  }
}

object OperationalEventPayload {
  enum JobKind(val eventType: OperationalEventType) {
    case Created extends JobKind(OperationalEventType.JOB_CREATED)
    case Updated extends JobKind(OperationalEventType.JOB_UPDATED)
    case Closed extends JobKind(OperationalEventType.JOB_CLOSED)
  }

  final case class JobSnapshot(jobId: UUID, skills: List[String], status: JobStatus)

  enum SearchKind(val wire: String) {
    case Jobs extends SearchKind("jobs")
    case RecommendedJobs extends SearchKind("recommendedJobs")
    case SemanticJobSearch extends SearchKind("semanticJobSearch")
    case CandidateMatches extends SearchKind("candidateMatches")
  }
}
