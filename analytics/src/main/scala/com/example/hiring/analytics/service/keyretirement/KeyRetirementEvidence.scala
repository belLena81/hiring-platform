package com.example.hiring.analytics.service.keyretirement

import com.example.hiring.analytics.domain.{RetentionEvidence, WriterDisposition, WriterInventory}

import cats.data.{Chain, NonEmptyChain, ValidatedNec}
import cats.syntax.all.*

import java.time.Instant

/** Pure key-retirement evidence checks and the shared scan-result shape. */
private[analytics] object KeyRetirementEvidence {
  final case class ScanResult(count: Long, blockers: Chain[String] = Chain.empty)

  def validateRetention(evidence: RetentionEvidence, now: Instant): ValidatedNec[String, Unit] = {
    val kafkaReasons =
      if (evidence.kafka.partitions.nonEmpty) {
        val parts = evidence.kafka.partitions
        if (
          parts.map(_._1).distinct.size == parts.size &&
          parts.forall { case (number, barrier, earliest) => number >= 0 && barrier >= 0L && earliest >= barrier }
        )
          Chain.empty[String]
        else Chain.one("Kafka retention partition barrier is missing, invalid, or not yet passed")
      } else
        (evidence.kafka.barrierOffset, evidence.kafka.earliestAvailableOffset) match {
          case (Some(barrier), Some(earliest)) if barrier >= 0L && earliest >= barrier => Chain.empty[String]
          case _ => Chain.one("Kafka retention barrier is missing, invalid, or not yet passed")
        }
    val horizonReasons = Vector(
      "Delta data-file" -> evidence.deltaData,
      "Delta transaction-log" -> evidence.deltaLogs,
      "report" -> evidence.reports
    ).foldLeft(Chain.empty[String]) {
      case (reasons, (_, horizon))
          if horizon.retainedUntil.exists(deadline => !now.isBefore(deadline)) &&
            horizon.evidenceReference.trim.nonEmpty =>
        reasons
      case (reasons, (name, _)) =>
        reasons.append(s"$name retention horizon or its evidence reference is missing or has not elapsed")
    }
    val evidenceReasons =
      if (evidence.kafka.evidenceReference.trim.isEmpty)
        Chain.one("Kafka retention barrier evidence reference is missing")
      else Chain.empty[String]
    accumulate(kafkaReasons ++ horizonReasons ++ evidenceReasons)
  }

  def validateWriters(inventory: WriterInventory, now: Instant): ValidatedNec[String, Unit] = {
    val freshnessReasons = inventory.observedAt match {
      case Some(observedAt)
          if !observedAt.isAfter(now) && !observedAt.isBefore(now.minus(MaximumWriterEvidenceAge)) &&
            inventory.coverageReference.trim.nonEmpty =>
        Chain.empty[String]
      case _ =>
        Chain.one("managed and unmanaged Delta writer inventory lacks fresh operator-attested coverage evidence")
    }
    val all = inventory.managed ++ inventory.unmanaged
    val identityReasons =
      if (all.isEmpty) Chain.one("writer inventory contains no individually accounted writer identities")
      else Chain.empty[String]
    val writerReasons = all.foldLeft(Chain.empty[String]) { (reasons, writer) =>
      val missing =
        if (writer.identity.trim.isEmpty || writer.evidenceReference.trim.isEmpty)
          Chain.one("a Delta writer identity or evidence reference is missing")
        else Chain.empty[String]
      val disposition = writer.disposition match {
        case WriterDisposition.Stopped | WriterDisposition.AccessRevoked => Chain.empty[String]
        case WriterDisposition.Active                                    => Chain.one("a Delta writer remains active")
        case WriterDisposition.Unknown => Chain.one("a Delta writer is not accounted for as stopped or access-revoked")
      }
      reasons ++ missing ++ disposition
    }
    accumulate(freshnessReasons ++ identityReasons ++ writerReasons)
  }

  private def accumulate(reasons: Chain[String]): ValidatedNec[String, Unit] =
    NonEmptyChain.fromSeq(reasons.toList) match {
      case Some(errors) => errors.invalid
      case None         => ().validNec[String]
    }

  private val MaximumWriterEvidenceAge = java.time.Duration.ofHours(1)
}
