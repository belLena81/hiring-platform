package com.example.graphQL.cats.api.http

import org.http4s.{MediaRange, MediaType, QValue}
import org.http4s.headers.Accept
import org.http4s.MediaType.application

object MediaTypeNegotiation {
  val graphqlResponse: MediaType = MediaType.unsafeParse("application/graphql-response+json")
  val supported: List[MediaType] = List(graphqlResponse, application.json)
  val supportedMessage: String = supported.map(mediaType => s"${mediaType.mainType}/${mediaType.subType}").mkString(" or ")

  def selectResponseMediaType(accept: Option[Accept]): Option[MediaType] =
    accept.fold(Option(supported.head)) { header =>
      val entries = header.values.toList.zipWithIndex

      def qualityFor(mediaType: MediaType): Option[QValue] =
        entries
          .filter { case (entry, _) => entry.mediaRange.satisfiedBy(mediaType) }
          .maxByOption { case (entry, headerIndex) => (specificity(entry.mediaRange), -headerIndex) }
          .collect { case (entry, _) if entry.qValue > QValue.Zero => entry.qValue }

      (qualityFor(graphqlResponse), qualityFor(application.json)) match {
        case (Some(graphqlQuality), Some(jsonQuality)) if graphqlQuality >= jsonQuality => Some(graphqlResponse)
        case (Some(_), Some(_)) => Some(application.json)
        case (Some(_), None) => Some(graphqlResponse)
        case (None, Some(_)) => Some(application.json)
        case _ => None
      }
    }

  private def specificity(mediaRange: MediaRange): Int = mediaRange match {
    case _: MediaType => 2
    case range if range.mainType == "*" => 0
    case _ => 1
  }
}
