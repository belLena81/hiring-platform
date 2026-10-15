package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.service.ProbeResult

trait DatabaseProbe[F[_]] {
  def check(requestId: Option[String]): F[ProbeResult]
}
