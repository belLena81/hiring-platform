package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.{Job, SearchTerm}
import org.bson.Document

import scala.jdk.CollectionConverters.*

/** The targeted write of a job's content. It `$set`s every content field, `$unset`s the optional content fields that
  * are now absent, and advances the content revision once. It never touches `embedding`, `embeddingMeta` or
  * `admissionFence`: derived data and the admission fence have their own writers, and rewriting the whole document
  * would move the stored vector for every edit. The caller supplies the `version == observed` predicate (plus the Open
  * status for closure).
  */
private[mongo] object MongoJobContentUpdate {
  private val City = MongoFields.LocationCity
  private val CityCanonical = MongoFields.LocationCityCanonical

  def apply(job: Job): MongoUpdate = {
    val location = job.location
    val point = location.coordinates.map(value =>
      new Document("type", "Point").append(
        MongoFields.Coordinates,
        List[java.lang.Double](value.longitude, value.latitude).asJava
      )
    )
    def optional[A](field: String, value: Option[A]): MongoUpdate =
      value.fold(MongoUpdate.unset(field))(MongoUpdate.set(field, _))
    MongoUpdate.combine(
      MongoUpdate.set(MongoFields.RecruiterId, job.recruiterId.value.toString),
      MongoUpdate.set(MongoFields.Title, job.title),
      MongoUpdate.set(MongoFields.Description, job.description),
      MongoUpdate.set(MongoFields.Requirements, job.requirements.asJava),
      MongoUpdate.set(MongoFields.Skills, job.skills.toList.sorted.asJava),
      MongoUpdate.set(MongoFields.SkillsCanonical, job.skills.toList.sorted.map(SearchTerm.canonical).asJava),
      MongoUpdate.set(s"${MongoFields.Location}.${MongoFields.Country}", location.country),
      MongoUpdate.set(s"${MongoFields.Location}.${MongoFields.Remote}", location.remote),
      optional(City, location.city),
      optional(CityCanonical, location.city.map(SearchTerm.canonical)),
      optional(MongoFields.LocationPoint, point),
      MongoUpdate.set(MongoFields.Status, job.status.toString),
      MongoUpdate.set(MongoFields.UpdatedAt, java.util.Date.from(job.updatedAt)),
      optional(MongoFields.ClosedAt, job.closedAt.map(java.util.Date.from)),
      MongoUpdate.inc(MongoFields.Version, 1L)
    )
  }
}
