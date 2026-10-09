package com.example.graphQL.cats.service.port

import cats.effect.IO
import com.example.graphQL.cats.service.ProbeResult

trait DatabaseProbe {
  def check(requestId: Option[String]): IO[ProbeResult]
}
