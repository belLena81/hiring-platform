package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.UserRole

/** Which role may drive which user-facing lifecycle event, as a pure rule shared by the service (to answer Forbidden)
  * and the guarded repository write (as defense in depth). Anything not listed is denied.
  */
object InterviewActorPolicy {

  /** The seven events a person can request; every other event belongs to the system. */
  def isUserEvent(event: InterviewLifecycleEvent): Boolean = event match {
    case InterviewLifecycleEvent.Cancel(_, _) | InterviewLifecycleEvent.RequestReschedule(_) |
        InterviewLifecycleEvent.DismissRescheduleRequest | InterviewLifecycleEvent.Propose(_, _, _, _, _) |
        InterviewLifecycleEvent.AcceptProposal(_) | InterviewLifecycleEvent.DeclineProposal(_) |
        InterviewLifecycleEvent.WithdrawProposal(_) =>
      true
    case _ => false
  }

  /** Whether `actor` with `role` may drive `event`. A proposal always names its trusted proposer. */
  def permits(role: UserRole, actor: UserId, event: InterviewLifecycleEvent): Boolean = event match {
    case InterviewLifecycleEvent.Cancel(initiator, _) =>
      (initiator, role) match {
        case (InterviewCancellationInitiator.Candidate, UserRole.Candidate) => true
        case (InterviewCancellationInitiator.Recruiter, UserRole.Recruiter) => true
        case (InterviewCancellationInitiator.Admin, UserRole.Admin)         => true
        case _                                                              => false
      }
    case InterviewLifecycleEvent.RequestReschedule(_) | InterviewLifecycleEvent.AcceptProposal(_) |
        InterviewLifecycleEvent.DeclineProposal(_) =>
      role == UserRole.Candidate
    case InterviewLifecycleEvent.DismissRescheduleRequest | InterviewLifecycleEvent.WithdrawProposal(_) =>
      role == UserRole.Recruiter || role == UserRole.Admin
    case InterviewLifecycleEvent.Propose(_, _, proposedBy, _, _) =>
      (role == UserRole.Recruiter || role == UserRole.Admin) && proposedBy == actor
    case _ => false
  }
}
