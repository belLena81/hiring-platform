package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.SearchTerm
import com.mongodb.MongoCommandException
import com.mongodb.client.model.{Filters, Indexes, Projections, Updates}
import org.bson.Document
import org.bson.conversions.Bson
import scala.jdk.CollectionConverters.*

/** `027_job_canonical_filters`: job `skills` and `location.city` filters compare canonical values.
  *
  * Candidates already filter on stored canonical sidecars; jobs compared the display values, so a filter for `scala`
  * missed `Scala`. Job writers now store `skillsCanonical` (one canonical value per skill, in the order of `skills`)
  * and `location.cityCanonical`, both produced by `SearchTerm.canonical` (trim, lower-case with the root locale, the
  * same function as the candidate sidecars), and every filter path (list, discovery, facets, vector-search filter,
  * lexical filter, in-application eligibility recheck) compares them. Filter inputs are normalized the same way.
  *
  * Steps, in this order:
  *   1. Backfill. Every job is scanned in bounded `_id` batches with the ledger checkpoint `<last _id>`. A job whose
  *      stored canonical values differ from the values derived from its display fields gets both written by one
  *      `updateOne` guarded by `_id` and by the display values that were read, so a concurrent content edit (which
  *      writes the canonical values itself) is never overwritten with stale values. A restart resumes after the
  *      checkpoint; two processes converge because the derivation is deterministic. After the first full scan the audit
  *      below runs and, if it finds a job, one further scan from the start runs without moving the checkpoint before
  *      the step fails.
  *   1. Audit with `$nor` against the successor validator, install the validator
  *      ([[MongoHiringValidators.jobValidatorV3]]: canonical fields required and paired with their display fields),
  *      audit again to close the window between the first audit and the install.
  *   1. Create the new-named indexes (`jobs_open_city_canonical_created_id`, `jobs_embedding_meta_canonical_filters`)
  *      from `MongoHiringIndexSetup`, then drop the two superseded indexes that keyed `location.city`.
  *
  * The completed proof is read-only and does not scan: the exact successor validator (V3, or V4 from `028`) is
  * installed unchanged, which keeps unconforming jobs out, and the superseded indexes are absent; a reappearing
  * superseded index reopens the step. Validator drift fails closed and is never overwritten.
  *
  * Atlas Search definitions are provisioned separately under new index names (`MongoAtlasSearchSetup`); an existing
  * same-name definition with other paths fails startup and is never replaced. The old-named Atlas indexes are left in
  * place for the operator to delete after the gates pass.
  *
  * Recovery is forward-only. Stop every writer running an earlier binary before upgrading: it does not write the
  * canonical fields, which the successor validator rejects. Application rollback does not remove the fields or restore
  * the dropped indexes.
  */
private[mongo] object MongoJobCanonicalFilterMigrations {
  private val Id: MigrationId = MigrationIds.JobCanonicalFilters
  val MigrationId: String = Id.value

  /** The superseded index names, kept here only so this step can find and remove them. */
  val LegacyIndexNames: List[String] = List("jobs_open_city_created_id", "jobs_embedding_meta_filters")

  private val City = MongoFields.LocationCity
  private val CityCanonical = MongoFields.LocationCityCanonical

  private def jobs(database: MongoHiringSetup.SetupDatabase) = database.getCollection(MongoCollections.Jobs)

  private def textList(document: Document, field: String): Option[List[String]] =
    Option(document.get(field)).collect { case values: java.util.List[?] => values.asScala.toList }.flatMap { values =>
      values.traverse { case value: String => Some(value); case _ => None }
    }

  private def location(document: Document): Option[Document] =
    Option(document.get(MongoFields.Location)).collect { case value: Document => value }

  private def key(run: MigrationRun, row: Document): IO[String] =
    Option(row.get(MongoFields.Id)).collect { case value: String if value.nonEmpty => value } match {
      case Some(value) => IO.pure(value)
      case None        => run.fail[String]("found an invalid document key")
    }

  private def page(run: MigrationRun, after: Option[String]): IO[List[Document]] =
    MongoBoundedResults.collect(
      jobs(run.database)
        .find(after.fold(Filters.empty())(id => Filters.gt(MongoFields.Id, id)))
        .projection(
          Projections.include(MongoFields.Id, MongoFields.Skills, MongoFields.SkillsCanonical, City, CityCanonical)
        )
        .sort(Indexes.ascending(MongoFields.Id))
        .limit(MongoBatchScan.BatchSize)
        .boundedStream(32),
      MongoBatchScan.BatchSize,
      MigrationError.StepFailed(Id, s"setup read exceeded its maximum of ${MongoBatchScan.BatchSize} elements")
    )

  /** The repair of one job, or `None` when its stored canonical values already equal the derived ones. A job whose
    * display fields are not text is left for the audit, which fails the step naming only the collection.
    */
  private def repair(run: MigrationRun, row: Document): IO[Unit] =
    for {
      id <- key(run, row)
      _ <- (textList(row, MongoFields.Skills), location(row)) match {
        case (Some(skills), Some(place)) =>
          val city = Option(place.get(MongoFields.City)).collect { case value: String => value }
          val expectedSkills = skills.map(SearchTerm.canonical)
          val expectedCity = city.map(SearchTerm.canonical)
          val storedSkills = textList(row, MongoFields.SkillsCanonical)
          val storedCity = Option(place.get(MongoFields.CityCanonical)).collect { case value: String => value }
          val current = storedSkills.contains(expectedSkills) && storedCity == expectedCity
          IO.unlessA(current) {
            val unchanged: Bson = Filters.and(
              Filters.eq(MongoFields.Id, id),
              Filters.eq(MongoFields.Skills, skills.asJava),
              city.fold(Filters.exists(City, false))(Filters.eq(City, _))
            )
            val update = Updates.combine(
              Updates.set(MongoFields.SkillsCanonical, expectedSkills.asJava),
              expectedCity.fold(Updates.unset(CityCanonical))(Updates.set(CityCanonical, _))
            )
            jobs(run.database).updateOne(unchanged, update).void
          }
        case _ => IO.unit
      }
    } yield ()

  private def scan(run: MigrationRun, from: Option[String], checkpoint: Boolean): IO[Unit] =
    MongoBatchScan.byId(MongoBatchScan.BatchSize, from)(page(run, _))(key(run, _)) { rows =>
      rows.traverse_(repair(run, _)) *>
        IO.whenA(checkpoint)(rows.lastOption.traverse_(key(run, _).flatMap(run.advance)))
    }

  /** First job the successor validator rejects, if any; only the collection is ever named in an error. */
  private def auditFinds(database: MongoHiringSetup.SetupDatabase): IO[Boolean] =
    jobs(database)
      .find(new Document("$nor", List(MongoHiringValidators.jobCanonicalAudit).asJava))
      .limit(1)
      .first
      .map(_.nonEmpty)

  private def audit(run: MigrationRun): IO[Unit] =
    auditFinds(run.database).flatMap(invalid =>
      IO.whenA(invalid)(run.fail[Unit](s"found invalid stored data in ${MongoCollections.Jobs}"))
    )

  private def backfill(run: MigrationRun): IO[Unit] =
    run.textCheckpoint.flatMap(scan(run, _, checkpoint = true)) *>
      auditFinds(run.database).flatMap { invalid =>
        // A writer may have stored a job after the scan passed it: rescan once from the start, then fail closed.
        IO.whenA(invalid)(scan(run, None, checkpoint = false))
      }

  private def recoverMissing[A](default: A)(effect: IO[A]): IO[A] =
    effect.recoverWith {
      case error: MongoCommandException if Set(26, 27).contains(error.getErrorCode) => IO.pure(default)
    }

  private def legacyIndexesPresent(database: MongoHiringSetup.SetupDatabase): IO[Boolean] =
    recoverMissing(false)(
      jobs(database).listIndexes[Document].map(_.exists(index => LegacyIndexNames.contains(index.getString("name"))))
    )

  private def dropLegacyIndexes(database: MongoHiringSetup.SetupDatabase): IO[Unit] =
    LegacyIndexNames.traverse_ { name =>
      recoverMissing(())(
        database.runCommand(new Document("dropIndexes", MongoCollections.Jobs).append("index", name)).void
      )
    }

  private def cutover(run: MigrationRun): IO[Unit] =
    for {
      _ <- MongoMigrationLedger.requireComplete(run.database, MigrationIds.OutboxEnvelopeSource)
      _ <- backfill(run)
      _ <- audit(run)
      // A rerun of this step (reopened after an index regression) must not replace the successor `028` installed.
      successor <- MongoHiringValidators.validatorMatchesAny(
        run.database,
        MongoCollections.Jobs,
        MongoHiringValidators.jobValidatorHistory.drop(1)
      )
      _ <- IO.unlessA(successor)(
        MongoHiringValidators.install(run.database, MongoCollections.Jobs, MongoHiringValidators.jobValidatorV3)
      )
      _ <- audit(run)
      // Create the replacements first so the job queries are never left without an index, then drop the old ones.
      _ <- MongoHiringIndexSetup.ensure(run.database.underlying, MongoHiringIndexSetup.jobCanonicalFilterIndexes)
      _ <- dropLegacyIndexes(run.database)
      remaining <- legacyIndexesPresent(run.database)
      _ <- IO.whenA(remaining)(run.fail[Unit]("superseded index remains"))
    } yield ()

  private def verifyCompleted(database: MongoHiringSetup.SetupDatabase): IO[CompletedProof] =
    MongoHiringValidators
      .assertAnyStrictValidator(
        database,
        MongoCollections.Jobs,
        MongoHiringValidators.jobValidatorHistory.drop(1)
      ) *>
      legacyIndexesPresent(database).map(present => if (present) CompletedProof.Reopen else CompletedProof.Trusted)

  val step: MongoMigrationStep = MongoMigrationStep(Id, cutover, verifyCompleted)
}
