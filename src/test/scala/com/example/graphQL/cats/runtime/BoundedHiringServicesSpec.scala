package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO}
import com.example.graphQL.cats.domain.model.{AccountToken, User, UserPageRequest, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{ActorContext, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{AccountProfileInput, AccountUseCases, BootstrapAdminInput, LoginInput, SignUpInput}
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class BoundedHiringServicesSpec extends CatsEffectSuite {
  test("typed resolver timeout cancels its effect and returns sanitized unavailable") {
    for {
      cancelled <- Deferred[IO, Unit]
      service = BoundedHiringServices.accounts(new AccountUseCases[IO] {
        def signUp(input: SignUpInput, now: Instant, id: UserId): IO[Either[UseCaseError, (User, AccountToken)]] = IO.never
        def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, id: UserId): IO[Either[UseCaseError, (User, AccountToken)]] = IO.never
        def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] = IO.never
        def me(actor: ActorContext): IO[Either[UseCaseError, User]] = IO.never.onCancel(cancelled.complete(()).void)
        def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant): IO[Either[UseCaseError, User]] = IO.never
        def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]] = IO.never
        def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]] = IO.never
      }, 20.millis)
      result <- service.me(ActorContext(UserId(UUID.randomUUID()), UserRole.Candidate))
      _ <- cancelled.get.timeout(1.second)
    } yield assertEquals(result, Left(UseCaseError.repository(RepositoryError.Unavailable)))
  }
}
