package com.example.graphQL.cats.application

import cats.effect.IO

enum LogEvent {
  case ConfigInvalid, MongoUnavailable, MongoAuthFailed, RequestRejected, StartupFailed, RuntimeFailed, Started, Shutdown

  def category: String = this match {
    case ConfigInvalid    => "CONFIG_INVALID"
    case MongoUnavailable => "MONGO_UNAVAILABLE"
    case MongoAuthFailed  => "MONGO_AUTH_FAILED"
    case RequestRejected => "REQUEST_REJECTED"
    case StartupFailed   => "STARTUP_FAILED"
    case RuntimeFailed   => "RUNTIME_FAILED"
    case Started         => "STARTED"
    case Shutdown        => "SHUTDOWN"
  }
}

trait Diagnostics {
  def event(event: LogEvent, requestId: Option[String] = None): IO[Unit]
}

object Diagnostics {
  val noop: Diagnostics = new Diagnostics {
    def event(event: LogEvent, requestId: Option[String]): IO[Unit] = IO.unit
  }
}
