package com.example.graphQL.cats.service.application

import cats.effect.IO
import com.example.graphQL.cats.service.port.RepositoryF

/** Test view of a worker receive: whether the message was durably handled, so its offset could be committed. */
extension (received: RepositoryF[IO, Unit]) def committed: IO[Boolean] = received.value.map(_.isRight)
