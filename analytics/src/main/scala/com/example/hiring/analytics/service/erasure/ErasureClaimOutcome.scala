package com.example.hiring.analytics.service.erasure

/** Result of processing one claimed erasure request. */
enum ErasureClaimOutcome {
  case Completed
  case Deferred
  case ReleasedForOthers
  case Failed(error: Throwable)
}
