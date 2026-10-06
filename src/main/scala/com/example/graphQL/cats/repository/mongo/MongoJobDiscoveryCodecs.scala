package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.service.search.{JobDiscoveryFacets, JobFacetBucket}
import org.bson.Document
import scala.jdk.CollectionConverters.*
import MongoHiringCodecs.StoredDocumentError
import MongoHiringCodecs.StoredDocumentError.{InvalidField, MissingField}

/** A malformed aggregation result cannot be represented as successful exact counts. */
private[mongo] object MongoJobDiscoveryCodecs {
  def facets(result: Option[Document]): ValidatedNel[StoredDocumentError, JobDiscoveryFacets] =
    result.toValidNel(MissingField("facets")).andThen { document =>
      (
        buckets(document, "skills", remote = false),
        buckets(document, "countries", remote = false),
        buckets(document, "cities", remote = false),
        buckets(document, "remote", remote = true)
      ).mapN { (skills, countries, cities, remote) =>
        val limit = JobDiscoveryFacets.MaxBucketsPerDimension
        JobDiscoveryFacets(
          skills.take(limit),
          countries.take(limit),
          cities.take(limit),
          remote.take(limit),
          List(skills, countries, cities, remote).exists(_.size > limit)
        )
      }
    }

  private def buckets(
      document: Document,
      field: String,
      remote: Boolean
  ): ValidatedNel[StoredDocumentError, List[JobFacetBucket]] =
    Option(document.get(field)).toValidNel(MissingField(field)).andThen {
      case values: java.util.List[?] =>
        values.asScala.toList.traverse {
          case bucket: Document =>
            (
              identity(bucket, field, remote),
              count(bucket, field)
            ).mapN(JobFacetBucket.apply)
          case _ => InvalidField(field).invalidNel
        }
      case _ => InvalidField(field).invalidNel
    }

  private def identity(
      bucket: Document,
      field: String,
      remote: Boolean
  ): ValidatedNel[StoredDocumentError, String] =
    Option(bucket.get("_id")).toValidNel(MissingField(s"$field._id")).andThen {
      case value: java.lang.Boolean if remote              => value.toString.validNel
      case value: String if !remote && value.trim.nonEmpty => value.validNel
      case _                                               => InvalidField(s"$field._id").invalidNel
    }

  private def count(bucket: Document, field: String): ValidatedNel[StoredDocumentError, Long] =
    Option(bucket.get("count")).toValidNel(MissingField(s"$field.count")).andThen {
      case value: java.lang.Integer if value.longValue() > 0L => value.longValue().validNel
      case value: java.lang.Long if value.longValue() > 0L    => value.longValue().validNel
      case _                                                  => InvalidField(s"$field.count").invalidNel
    }
}
