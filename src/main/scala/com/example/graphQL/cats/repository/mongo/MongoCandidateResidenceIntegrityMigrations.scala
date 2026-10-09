package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.FieldLimits
import com.mongodb.client.model.{Filters, Projections, Sorts}
import org.bson.Document
import java.util.Locale

/** Pure validation of the indexed residence values; no stored values are repaired implicitly. */
private[mongo] object CandidateResidenceIntegrity {
  enum Error { case MalformedResidence }

  private final case class Residence(
      country: String,
      countryCanonical: String,
      city: Option[String],
      cityCanonical: Option[String]
  )

  def validate(user: Document): Either[Error, Unit] = {
    def invalid[A]: Either[Error, A] = Left(Error.MalformedResidence)
    def text(document: Document, field: String): Either[Error, String] =
      Option(document.get(field)).collect { case value: String => value }.toRight(Error.MalformedResidence)
    def optionalText(document: Document, field: String): Either[Error, Option[String]] =
      if (!document.containsKey(field)) Right(None) else text(document, field).map(Some(_))
    def canonical(value: String): String = value.trim.toLowerCase(Locale.ROOT)
    def validText(value: String): Boolean = value.trim.nonEmpty && value.length <= FieldLimits.ShortTextMaxChars
    def validateResidence(document: Document): Either[Error, Unit] =
      for {
        country <- text(document, MongoFields.Country)
        countryCanonical <- text(document, MongoFields.CountryCanonical)
        city <- optionalText(document, MongoFields.City)
        cityCanonical <- optionalText(document, MongoFields.CityCanonical)
        residence = Residence(country, countryCanonical, city, cityCanonical)
        valid = validText(residence.country) && validText(residence.countryCanonical) &&
          residence.countryCanonical == canonical(residence.country) &&
          residence.city.forall(validText) && residence.cityCanonical.forall(validText) &&
          residence.cityCanonical == residence.city.map(canonical)
        _ <- Either.cond(valid, (), Error.MalformedResidence)
      } yield ()

    if (!user.containsKey(MongoFields.Profile)) Right(())
    else
      user.get(MongoFields.Profile) match {
        case profile: Document if !profile.containsKey(MongoFields.CurrentResidence) => Right(())
        case profile: Document                                                       =>
          profile.get(MongoFields.CurrentResidence) match {
            case residence: Document => validateResidence(residence)
            case _                   => invalid
          }
        case _ => invalid
      }
  }
}

/** Audits existing indexed residences once, then trusts supported canonicalizing writers and the paired validator. Stop
  * incompatible writers before cutover. This migration changes only its own ledger, never user revisions/data.
  */
private[mongo] object MongoCandidateResidenceIntegrityMigrations {
  private val Id: MigrationId = MigrationIds.CandidateResidenceIntegrity

  /** Ledger literal kept as `String` for existing callers; the typed identity is
    * `MigrationIds.CandidateResidenceIntegrity`.
    */
  val MigrationId: String = Id.value
  private val BatchSize = 500

  private def requireValidator(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    MongoHiringValidators.assertStrictValidators(
      database,
      List(MongoCollections.Users -> MongoHiringValidators.userValidator)
    )

  /** Run before installing the user validator so completed-proof drift cannot be silently overwritten. */
  def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    MongoMigrationLedger.read(database, Id).flatMap {
      case MigrationLedgerState.Complete => requireValidator(database)
      case _                             => IO.unit
    }

  private def audit(run: MigrationRun): IO[Unit] = {
    val users = run.database.getCollection(MongoCollections.Users)
    def scan(after: Option[String], checkpoint: Boolean): IO[Unit] = {
      val selection = after.fold(Filters.empty())(value => Filters.gt(MongoFields.Id, value))
      users
        .find(selection)
        .projection(Projections.include(MongoFields.Id, s"${MongoFields.Profile}.${MongoFields.CurrentResidence}"))
        .sort(Sorts.ascending(MongoFields.Id))
        .hint("_id_")
        .limit(BatchSize)
        .boundedStream(32)
        .take(BatchSize.toLong + 1L)
        .compile
        .toList
        .flatMap { rows =>
          IO.raiseWhen(rows.size > BatchSize)(MigrationError.StepFailed(run.id, "audit exceeded batch limit")) *>
            rows.traverse_(row =>
              IO.fromEither(
                CandidateResidenceIntegrity
                  .validate(row)
                  .leftMap(_ => MigrationError.StepFailed(run.id, "audit rejected a stored residence"))
              )
            ) *> rows.lastOption.traverse_ { last =>
              val identity = Option(last.get(MongoFields.Id)).collect { case value: String if value.nonEmpty => value }
              IO.fromEither(identity.toRight(MigrationError.StepFailed(run.id, "audit identity is invalid")))
                .flatMap { id =>
                  (if (checkpoint) run.advance(id) else IO.unit) *>
                    (if (rows.size == BatchSize) IO.defer(scan(Some(id), checkpoint)) else IO.unit)
                }
            }
        }
    }
    requireValidator(run.database) *> run.textCheckpoint.flatMap(after => scan(after, checkpoint = true)) *>
      // The paired validator prevents unpaired new writes. Recheck the prefix before freezing its proof.
      scan(None, checkpoint = false) *> requireValidator(run.database)
  }

  val step: MongoMigrationStep =
    MongoMigrationStep(Id, audit, requireValidator(_).as(CompletedProof.Trusted))

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = MongoMigrationRunner.run(database, step)
}
