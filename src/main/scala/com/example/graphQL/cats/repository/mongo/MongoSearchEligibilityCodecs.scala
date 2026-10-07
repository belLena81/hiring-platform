package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{UserId, parse as parseIdentifier}
import com.example.graphQL.cats.service.port.RepositoryError
import com.example.graphQL.cats.service.search.*
import org.bson.Document
import scala.jdk.CollectionConverters.*

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
    Either
      .catchNonFatal(Option(document.get(MongoFields.EmbeddingMeta, classOf[Document])))
      .leftMap(_ => RepositoryError.InvalidStoredData)
      .flatMap(_.traverse { value =>
        def text(field: String): Either[RepositoryError, String] =
          Either
            .catchNonFatal(Option(value.getString(field)))
            .leftMap(_ => RepositoryError.InvalidStoredData)
            .flatMap(_.toRight(RepositoryError.InvalidStoredData))
        for {
          model <- text(MongoFields.Model)
          hash <- text(MongoFields.SourceHash)
          timestamp <- Either
            .catchNonFatal(Option(value.getDate(MongoFields.UpdatedAt)))
            .leftMap(_ => RepositoryError.InvalidStoredData)
            .flatMap(_.toRight(RepositoryError.InvalidStoredData))
        } yield EmbeddingMeta(model, hash, timestamp.toInstant)
      })

  def job(document: Document): Either[RepositoryError, JobSearchEligibility] = {
    val source = new Document(document)
    source.remove(MongoFields.EmbeddingMeta)
    (MongoHiringCodecs.readJob(source).toEither.leftMap(_ => RepositoryError.InvalidStoredData), metadata(document))
      .mapN(JobSearchEligibility.apply)
  }

  def candidate(document: Document): Either[RepositoryError, CandidateSearchEligibility] =
    for {
      rawId <- Either.catchNonFatal(document.getString(MongoFields.Id)).leftMap(_ => RepositoryError.InvalidStoredData)
      id <- parseIdentifier(rawId)(UserId.apply).leftMap(_ => RepositoryError.InvalidStoredData)
      source <- Either
        .catchNonFatal {
          val role = UserRole.valueOf(document.getString(MongoFields.Role))
          val status = AccountStatus.valueOf(document.getString(MongoFields.AccountStatus))
          val profile = Option
            .when(role == UserRole.Candidate && status == AccountStatus.Active)(
              document.get(MongoFields.Profile, classOf[Document])
            )
            .flatMap(Option(_))
            .map { value =>
              val skills = value.get(MongoFields.Skills, classOf[java.util.List[String]]).asScala.toSet
              val summary = Option(value.getString(MongoFields.ExperienceSummary))
              val residence = Option(value.get(MongoFields.CurrentResidence, classOf[Document])).map { residence =>
                val country = residence.getString(MongoFields.Country)
                require(country != null)
                CandidateResidence(country, Option(residence.getString(MongoFields.City)))
              }
              val availability =
                Option(value.getString(MongoFields.AvailabilityStatus)).map(CandidateAvailabilityStatus.valueOf)
              val optIn =
                Option(value.get(MongoFields.RecruiterSearchOptIn, classOf[java.lang.Boolean])).exists(_.booleanValue())
              CandidateProfile(skills, summary, None, residence, availability, optIn)
            }
          CandidateSearchEligibility(id, role, status, profile, None, Option(document.getString(MongoFields.Name)))
        }
        .leftMap(_ => RepositoryError.InvalidStoredData)
      _ <- Either.cond(
        source.role != UserRole.Candidate || source.accountStatus != AccountStatus.Active ||
          source.name.exists(_.nonEmpty),
        (),
        RepositoryError.InvalidStoredData
      )
      meta <- metadata(document)
    } yield source.copy(metadata = meta)
}
