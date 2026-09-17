package com.example.graphQL.cats.service

import cats.effect.IO

enum ProbeResult {
  case Ready, Unavailable, AuthenticationFailed
}

trait DatabaseProbe {
  def check: IO[ProbeResult]
  def check(@scala.annotation.unused requestId: Option[String]): IO[ProbeResult] = check
}
