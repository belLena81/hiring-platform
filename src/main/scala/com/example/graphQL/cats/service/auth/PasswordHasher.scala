package com.example.graphQL.cats.service.auth

import cats.effect.IO
import com.example.graphQL.cats.domain.model.PasswordHash

trait PasswordHasher {
  def hash(password: String): IO[PasswordHash]
  def verify(encoded: PasswordHash, password: String): IO[Boolean]
  def verifyUnknown(password: String): IO[Unit]
}
