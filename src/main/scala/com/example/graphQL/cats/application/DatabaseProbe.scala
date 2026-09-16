package com.example.graphQL.cats.application

import cats.effect.IO

enum ProbeResult {
  case Ready, Unavailable, AuthenticationFailed
}

trait DatabaseProbe {
  def check: IO[ProbeResult]
}
