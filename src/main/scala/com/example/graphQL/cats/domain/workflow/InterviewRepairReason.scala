package com.example.graphQL.cats.domain.workflow

/** Closed set of reasons a step gives up. `wire` is the persisted text, safe to store and log as is. */
enum InterviewFailureCode(val wire: String) {
  case Provider extends InterviewFailureCode("provider")
  case UnknownReservation extends InterviewFailureCode("unknown_reservation")
  case ReplayExpired extends InterviewFailureCode("replay_expired")
  case FutureMessage extends InterviewFailureCode("future_message")
  case InvalidMessage extends InterviewFailureCode("invalid_message")
  case ExecutionExhausted extends InterviewFailureCode("execution_exhausted")
  case NotificationRetry extends InterviewFailureCode("notification_retry")
  case NotificationExhausted extends InterviewFailureCode("notification_exhausted")
  case PublicationExhausted extends InterviewFailureCode("publication_exhausted")
  case PublicationObsolete extends InterviewFailureCode("publication_obsolete")
  case PublicationUnavailable extends InterviewFailureCode("publication_unavailable")
  case PublicationCoordinationUnavailable extends InterviewFailureCode("publication_coordination_unavailable")
}

/** Why a workflow needs Admin repair. `text` is the persisted reason. */
enum InterviewRepairReason {
  case CalendarReservationRejected
  case StatusNotCommitted
  case RetryExhausted(step: InterviewFailureCode)
  case CompensationFailed(step: InterviewFailureCode)

  def text: String = this match {
    case CalendarReservationRejected => "calendar reservation rejected"
    case StatusNotCommitted          => "application status was not committed"
    case RetryExhausted(step)        => s"retry exhausted: ${step.wire}"
    case CompensationFailed(step)    => s"compensation failed: ${step.wire}"
  }
}

object InterviewRepairReason {
  private val all: List[InterviewRepairReason] =
    List(CalendarReservationRejected, StatusNotCommitted) ++
      InterviewFailureCode.values.toList.flatMap(code => List(RetryExhausted(code), CompensationFailed(code)))

  def parse(text: String): Option[InterviewRepairReason] = all.find(_.text == text)
}
