package com.example.graphQL.cats.service.auth

import com.example.graphQL.cats.domain.model.PasswordHash

trait PasswordHasher[F[_]] {
  def hash(password: String): F[PasswordHash]
  def verify(encoded: PasswordHash, password: String): F[Boolean]
  def verifyUnknown(password: String): F[Unit]
}
