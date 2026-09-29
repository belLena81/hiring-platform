package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.service.UseCaseError

private[graphql] enum HiringGraphQLFailure {
  case UseCase(error: UseCaseError)
  case Input(error: HiringGraphQLModel.GraphQLFailure)
  case RateLimited(retryAfterSeconds: Long)
  case RequestClosed
}

private[graphql] type HiringGraphQLResult[A] = EitherT[IO, HiringGraphQLFailure, A]
