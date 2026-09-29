package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*

private[config] object ConfigBounds {
  def bounded[A: Ordering](min: A, max: A, error: ConfigError)(value: A): ValidatedNel[ConfigError, A] =
    Either.cond(Ordering[A].lteq(min, value) && Ordering[A].lteq(value, max), value, error).toValidatedNel
}
