package com.example.graphQL.cats.domain.policy

import cats.data.StateT
import com.example.graphQL.cats.domain.error.DomainError

type LifecycleProgram[S, A] = StateT[[Value] =>> Either[DomainError, Value], S, A]
