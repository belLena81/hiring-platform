package com.example.graphQL.cats.service.search

/** Agreed ANN retrieval limits per filter bucket and concurrency: Recall@7 against exact search is at least
  * `minRecall`, and candidate p95 latency is at most baseline p95 times `maxP95Ratio`.
  */
final case class RetrievalLimits(minRecall: Double, maxP95Ratio: Double)

object RetrievalLimits {
  val Agreed: RetrievalLimits = RetrievalLimits(0.95, 1.10)

  /** One bucket measurement. `recallVsExact` is `None` when the exact search returned no neighbors (undefined recall);
    * `annReturnedHits` says whether ANN returned any hit for the same query set.
    */
  final case class BucketObservation(
      recallVsExact: Option[Double],
      annReturnedHits: Boolean,
      candidateP95Millis: Double,
      baselineP95Millis: Double
  )

  enum Violation {
    case RecallBelowMinimum(actual: Double)
    case RecallUndefinedButAnnReturnedHits
    case P95AboveLimit(candidate: Double, allowed: Double)
    case InvalidMeasurement
  }

  enum Verdict {
    case Pass
    case Fail(violations: List[Violation])
  }

  /** Empty-filter buckets have no exact neighbors: recall is vacuously satisfied only when ANN also returns nothing. */
  def check(limits: RetrievalLimits, observation: BucketObservation): Verdict = {
    val valid =
      List(observation.candidateP95Millis, observation.baselineP95Millis).forall(v => v.isFinite && v >= 0.0) &&
        observation.baselineP95Millis > 0.0 &&
        observation.recallVsExact.forall(r => r.isFinite && r >= 0.0 && r <= 1.0)
    if (!valid) Verdict.Fail(List(Violation.InvalidMeasurement))
    else {
      val recall = observation.recallVsExact match {
        case Some(value) if value < limits.minRecall => List(Violation.RecallBelowMinimum(value))
        case Some(_)                                 => Nil
        case None if observation.annReturnedHits     => List(Violation.RecallUndefinedButAnnReturnedHits)
        case None                                    => Nil
      }
      val allowed = observation.baselineP95Millis * limits.maxP95Ratio
      val latency =
        if (observation.candidateP95Millis <= allowed) Nil
        else List(Violation.P95AboveLimit(observation.candidateP95Millis, allowed))
      recall ++ latency match {
        case Nil        => Verdict.Pass
        case violations => Verdict.Fail(violations)
      }
    }
  }

  def checkAll(
      limits: RetrievalLimits,
      observations: Map[(SearchEvaluationFilterGroup, Int), BucketObservation]
  ): Map[(SearchEvaluationFilterGroup, Int), Verdict] =
    observations.view.mapValues(check(limits, _)).toMap
}
