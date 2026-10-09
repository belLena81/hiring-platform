package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{UserId, parse as parseIdentifier}
import com.example.graphQL.cats.service.port.RepositoryError
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.repository.mongo.MongoHiringCodecs.StoredDocumentError
import org.bson.Document

/** Projected BSON is untrusted; malformed present values fail closed at this adapter. */
private[mongo] object MongoSearchEligibilityCodecs {
  val jobFields: List[String] = List(
    MongoFields.Id,
    MongoFields.Version,
    MongoFields.RecruiterId,
    MongoFields.Title,
    MongoFields.Description,
    MongoFields.Requirements,
    MongoFields.Skills,
    MongoFields.Location,
    MongoFields.Status,
    MongoFields.CreatedAt,
    MongoFields.UpdatedAt,
    MongoFields.ClosedAt,
    MongoFields.EmbeddingMeta
  )
  val candidateFields: List[String] = List(
    MongoFields.Id,
    MongoFields.Name,
    MongoFields.Role,
    MongoFields.AccountStatus,
    MongoFields.ProfileSkills,
    MongoFields.ProfileExperienceSummary,
    s"${MongoFields.Profile}.${MongoFields.CurrentResidence}",
    MongoFields.ProfileAvailabilityStatus,
    MongoFields.ProfileRecruiterSearchOptIn,
    MongoFields.EmbeddingMeta
  )

  def projection(fields: List[String]): Document = fields.foldLeft(new Document())((doc, field) => doc.append(field, 1))

  def metadata(document: Document): Either[RepositoryError, Option[EmbeddingMeta]] =
    MongoDocumentFields.toRepository(
      MongoDocumentFields
        .optionalDocument(document, MongoFields.EmbeddingMeta)
        .flatMap(_.traverse { value =>
          (
            MongoDocumentFields.requiredString(value, MongoFields.Model),
            MongoDocumentFields.requiredString(value, MongoFields.SourceHash),
            MongoDocumentFields.requiredInstant(value, MongoFields.UpdatedAt)
          ).mapN(EmbeddingMeta.apply)
        })
    )

  def job(document: Document): Either[RepositoryError, JobSearchEligibility] = {
    val source = new Document(document)
    source.remove(MongoFields.EmbeddingMeta)
    (MongoHiringCodecs.readJob(source).toEither.leftMap(_ => RepositoryError.InvalidStoredData), metadata(document))
      .mapN(JobSearchEligibility.apply)
  }

  def candidate(document: Document): Either[RepositoryError, CandidateSearchEligibility] =
    for {
      source <- MongoDocumentFields.toRepository(candidateSource(document))
      _ <- Either.cond(
        source.role != UserRole.Candidate || source.accountStatus != AccountStatus.Active ||
          source.name.exists(_.nonEmpty),
        (),
        RepositoryError.InvalidStoredData
      )
      meta <- metadata(document)
    } yield source.copy(metadata = meta)

  private def candidateSource(document: Document): MongoDocumentFields.Read[CandidateSearchEligibility] =
    for {
      rawId <- MongoDocumentFields.requiredString(document, MongoFields.Id)
      id <- parseIdentifier(rawId)(UserId.apply).leftMap(_ => StoredDocumentError.InvalidField(MongoFields.Id))
      role <- MongoDocumentFields.requiredEnum(document, MongoFields.Role)(MongoDocumentFields.byName(UserRole.values))
      status <- MongoDocumentFields.requiredEnum(document, MongoFields.AccountStatus)(
        MongoDocumentFields.byName(AccountStatus.values)
      )
      profile <-
        if (role == UserRole.Candidate && status == AccountStatus.Active)
          MongoDocumentFields.optionalDocument(document, MongoFields.Profile).flatMap(_.traverse(candidateProfile))
        else Right(None)
      name <- MongoDocumentFields.optionalString(document, MongoFields.Name)
    } yield CandidateSearchEligibility(id, role, status, profile, None, name)

  private def candidateProfile(value: Document): MongoDocumentFields.Read[CandidateProfile] =
    for {
      skills <- MongoDocumentFields.requiredStringVector(value, MongoFields.Skills)
      summary <- MongoDocumentFields.optionalString(value, MongoFields.ExperienceSummary)
      residence <- MongoDocumentFields
        .optionalDocument(value, MongoFields.CurrentResidence)
        .flatMap(_.traverse { r =>
          (
            MongoDocumentFields.requiredString(r, MongoFields.Country),
            MongoDocumentFields.optionalString(r, MongoFields.City)
          ).mapN(CandidateResidence.apply)
        })
      availability <- MongoDocumentFields.optionalEnum(value, MongoFields.AvailabilityStatus)(
        MongoDocumentFields.byName(CandidateAvailabilityStatus.values)
      )
      optIn <- MongoDocumentFields.optionalBoolean(value, MongoFields.RecruiterSearchOptIn)
    } yield CandidateProfile(skills.toSet, summary, None, residence, availability, optIn.getOrElse(false))
}
