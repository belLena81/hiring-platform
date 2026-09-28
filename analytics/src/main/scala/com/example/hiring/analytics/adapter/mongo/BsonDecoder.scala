package com.example.hiring.analytics.adapter.mongo
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.errors.AnalyticsError

import org.bson.Document

import java.util.Date

/** Validated decoding for analytics records stored as BSON documents. */
private[analytics] trait BsonDecoder[A] {
  def decode(document: Document): Either[AnalyticsError, A]
}

private[analytics] object BsonDecoder {
  def apply[A](using decoder: BsonDecoder[A]): BsonDecoder[A] = decoder

  def instance[A](read: Document => Either[AnalyticsError, A]): BsonDecoder[A] =
    new BsonDecoder[A] {
      override def decode(document: Document): Either[AnalyticsError, A] = read(document)
    }

  def required[A](document: Document, field: String, malformed: AnalyticsError)(using
      decoder: BsonValueDecoder[A]
  ): Either[AnalyticsError, A] =
    Option(document.get(field)).flatMap(decoder.decode).toRight(malformed)

  /** A missing or BSON-null optional field is absent; a present value of the wrong BSON type is malformed. */
  def optional[A](document: Document, field: String, malformed: AnalyticsError)(using
      decoder: BsonValueDecoder[A]
  ): Either[AnalyticsError, Option[A]] =
    Option(document.get(field)) match {
      case None        => Right(None)
      case Some(value) => decoder.decode(value).map(Some(_)).toRight(malformed)
    }
}

private[analytics] trait BsonValueDecoder[A] {
  def decode(value: Any): Option[A]
}

private[analytics] object BsonValueDecoder {
  given BsonValueDecoder[String] with {
    override def decode(value: Any): Option[String] = value match {
      case text: String => Some(text)
      case _            => None
    }
  }

  given BsonValueDecoder[Int] with {
    override def decode(value: Any): Option[Int] = value match {
      case number: java.lang.Integer => Some(number.intValue())
      case _                         => None
    }
  }

  given BsonValueDecoder[Long] with {
    override def decode(value: Any): Option[Long] = value match {
      case number: java.lang.Long => Some(number.longValue())
      case _                      => None
    }
  }

  given BsonValueDecoder[Boolean] with {
    override def decode(value: Any): Option[Boolean] = value match {
      case flag: java.lang.Boolean => Some(flag.booleanValue())
      case _                       => None
    }
  }

  given BsonValueDecoder[Date] with {
    override def decode(value: Any): Option[Date] = value match {
      case date: Date => Some(date)
      case _          => None
    }
  }

  given BsonValueDecoder[Document] with {
    override def decode(value: Any): Option[Document] = value match {
      case document: Document => Some(document)
      case _                  => None
    }
  }

  given BsonValueDecoder[ErasureRequestState] with {
    override def decode(value: Any): Option[ErasureRequestState] = value match {
      case name: String => ErasureRequestState.fromString(name)
      case _            => None
    }
  }

  given BsonValueDecoder[Vector[Any]] with {
    override def decode(value: Any): Option[Vector[Any]] = value match {
      case values: java.util.List[?] => Some(values.toArray.toVector)
      case _                         => None
    }
  }
}
