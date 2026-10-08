package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.FieldLimits
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Projections, Sorts, UpdateOptions, Updates}
import com.mongodb.client.result.UpdateResult
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
  val MigrationId = "016_candidate_residence_integrity"
  private val BatchSize = 500

  private enum State {
    case Running(after: Option[String])
    case Complete
  }

  private def readState(row: Document): Either[String, State] = {
    val version = Option(row.get("version"))
    val checkpoint: Either[String, Option[String]] =
      if (!row.containsKey("lastId")) Right(None)
      else
        Option(row.get("lastId"))
          .collect { case value: String if value.nonEmpty => value }
          .toRight("Invalid candidate residence integrity checkpoint")
          .map(Some(_))
    if (!version.contains(Long.box(1L)) || !version.exists(_.isInstanceOf[java.lang.Long]))
      Left("Unsupported candidate residence integrity proof")
    else
      Option(row.get("state")) match {
        case Some("Running")                                => checkpoint.map(State.Running.apply)
        case Some("Complete") if !row.containsKey("lastId") => Right(State.Complete)
        case _                                              => Left("Unsupported candidate residence integrity proof")
      }
  }

  private def storedState(row: Document): IO[State] =
    IO.fromEither(readState(row).leftMap(new IllegalStateException(_)))

  private def validator(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    MongoHiringValidators
      .userValidatorMatches(database.underlying)
      .flatMap(valid =>
        IO.raiseUnless(valid)(
          new IllegalStateException("Candidate residence validator changed; explicit maintenance repair required")
        )
      )

  /** Run before installing the user validator so completed-proof drift cannot be silently overwritten. */
  def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    database
      .getCollection(MongoCollections.HiringMigrationLedger)
      .find(Filters.eq("_id", MigrationId))
      .first
      .flatMap {
        case None      => IO.unit
        case Some(row) =>
          storedState(row).flatMap {
            case State.Complete   => validator(database)
            case State.Running(_) => IO.unit
          }
      }

  def initialize(database: MongoHiringSetup.SetupDatabase): IO[Unit] = {
    val users = database.getCollection(MongoCollections.Users)
    val ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
    val marker = Filters.eq("_id", MigrationId)
    val running = Filters.and(marker, Filters.eq("version", Long.box(1L)), Filters.eq("state", "Running"))

    def acknowledged(result: UpdateResult): IO[Unit] =
      IO.raiseUnless(result.wasAcknowledged())(
        new IllegalStateException("Candidate residence integrity proof write was not acknowledged")
      )

    def complete: IO[Unit] = ledger.find(marker).first.flatMap {
      case Some(row) =>
        storedState(row).flatMap {
          case State.Complete => validator(database)
          case _ => IO.raiseError(new IllegalStateException("Candidate residence integrity audit did not complete"))
        }
      case None => IO.raiseError(new IllegalStateException("Candidate residence integrity proof is absent"))
    }

    def audit(after: Option[String], checkpoint: Boolean): IO[Unit] = {
      val selection = after.fold(Filters.empty())(value => Filters.gt("_id", value))
      users
        .find(selection)
        .projection(Projections.include("_id", "profile.currentResidence"))
        .sort(Sorts.ascending("_id"))
        .hint("_id_")
        .limit(BatchSize)
        .boundedStream(32)
        .take(BatchSize.toLong + 1L)
        .compile
        .toList
        .flatMap { rows =>
          IO.raiseWhen(rows.size > BatchSize)(
            new IllegalStateException("Candidate residence audit exceeded batch limit")
          ) *>
            rows.traverse_(row =>
              IO.fromEither(
                CandidateResidenceIntegrity
                  .validate(row)
                  .leftMap(_ => new IllegalStateException("Candidate residence audit rejected stored residence"))
              )
            ) *> rows.lastOption.traverse_ { last =>
              val identity = Option(last.get("_id")).collect { case value: String if value.nonEmpty => value }
              IO.fromEither(
                identity.toRight(new IllegalStateException("Candidate residence audit identity is invalid"))
              ).flatMap { id =>
                val advance =
                  if (!checkpoint) IO.unit
                  else
                    ledger.updateOne(running, Updates.max("lastId", id)).flatMap { result =>
                      acknowledged(result) *> (if (result.getMatchedCount == 1L) IO.unit else complete)
                    }
                advance *> (if (rows.size == BatchSize) IO.defer(audit(Some(id), checkpoint)) else IO.unit)
              }
            }
        }
    }

    def resume: IO[Unit] = ledger.find(marker).first.flatMap {
      case Some(row) =>
        storedState(row).flatMap {
          case State.Complete       => validator(database)
          case State.Running(after) =>
            audit(after, checkpoint = true) *>
              // The paired validator prevents unpaired new writes. Recheck the prefix before freezing its proof.
              audit(None, checkpoint = false) *>
              ledger
                .updateOne(running, Updates.combine(Updates.set("state", "Complete"), Updates.unset("lastId")))
                .flatMap(acknowledged) *> complete
        }
      case None => IO.raiseError(new IllegalStateException("Candidate residence integrity proof is absent"))
    }

    validator(database) *> ledger.find(marker).first.flatMap {
      case Some(_) => resume
      case None    =>
        ledger
          .updateOne(
            marker,
            Updates.combine(Updates.setOnInsert("version", Long.box(1L)), Updates.setOnInsert("state", "Running")),
            new UpdateOptions().upsert(true)
          )
          .flatMap(acknowledged)
          .handleErrorWith {
            case error: MongoWriteException if error.getError.getCode == 11000 => IO.unit
            case error                                                         => IO.raiseError(error)
          } *> resume
    }
  }
}
