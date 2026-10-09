package com.example.graphQL.cats.repository.mongo

import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.config.AdminSeedConfig
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.auth.PasswordHasher
import com.example.graphQL.cats.service.port.{MutationWriteContext, RepositoryError, UserAccountRepository}
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase
import java.text.Normalizer
import java.util.UUID

/** Trusted startup-only provisioning. First run creates the singleton; later runs only require a coherent registry. */
object MongoAdminSeed {
  private def rejected: IO[Unit] =
    IO.raiseError(new IllegalStateException("Admin seed conflicts with account registry"))

  def run(
      database: MongoDatabase[IO],
      accounts: UserAccountRepository,
      hasher: PasswordHasher,
      config: AdminSeedConfig,
      clock: Clock[IO] = Clock[IO],
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): IO[Unit] =
    if (!config.enabled) IO.unit
    else
      (config.name, config.password) match {
        case (Some(name), Some(password)) =>
          val canonical = AccountName.canonical(name)
          def registryState: IO[Option[org.bson.Document]] =
            Mongo4catsCollections
              .documents(database, MongoCollections.AccountRegistry)
              .flatMap(_.find(Filters.eq(MongoFields.Id, "user-account-registry")).first)
          // After first provisioning the seed is a no-op: later renames, status or password changes of the
          // admin are account operations, not startup conflicts. Only an inconsistent registry is fatal.
          def existing: IO[Unit] = registryState.flatMap {
            case Some(document)
                if Option(document.get(MongoFields.State)).contains("Initialized") &&
                  MongoDocumentFields.requiredUuid(document, MongoFields.AdminId).isRight =>
              IO.unit
            case _ => rejected
          }
          def reconcile: IO[Unit] = for {
            state <- registryState
            account <- accounts.findByCanonicalName(canonical).value
            _ <- (state, account) match {
              case (Some(document), Right(Some(credentials)))
                  if document.getString(MongoFields.State) == "Initialized" &&
                    document.getString(MongoFields.AdminId) == credentials.user.id.value.toString &&
                    credentials.user.role == UserRole.Admin && credentials.user.adminSingleton &&
                    credentials.user.accountStatus == AccountStatus.Active =>
                IO.unit
              case _ => rejected
            }
          } yield ()
          accounts.initialized.value.flatMap {
            case Right(true)  => existing
            case Right(false) =>
              for {
                now <- clock.realTimeInstant
                id <- uuidGen.randomUUID.map(UserId.apply)
                hash <- hasher.hash(password)
                user = User(id, None, Normalizer.normalize(name.trim, Normalizer.Form.NFKC), UserRole.Admin, None, now)
                  .copy(adminSingleton = true)
                result <- accounts.bootstrap(user, hash, MutationWriteContext.directWrite).value
                _ <- result match {
                  case Right(_)                       => reconcile
                  case Left(RepositoryError.Conflict) => reconcile
                  case Left(_) => IO.raiseError(new IllegalStateException("Admin seed persistence unavailable"))
                }
              } yield ()
            case Left(_) => IO.raiseError(new IllegalStateException("Admin seed registry unavailable"))
          }
        case _ => IO.raiseError(new IllegalStateException("Admin seed credentials are required"))
      }
}
