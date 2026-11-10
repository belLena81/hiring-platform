package com.example.graphQL.cats.domain.search

import cats.data.{NonEmptyChain, ValidatedNec}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.SearchTerm

/** How many of a job's distinct canonical skills a candidate covers. Built only through [[SkillEvidence.of]]. */
final case class SkillEvidence private (covered: Int, required: Int)

object SkillEvidence {

  /** None when the job lists no skill or no job skill is covered: no evidence, never an invented zero. Candidate skills
    * outside the job's set add nothing, so listing more skills cannot raise coverage.
    */
  def of(candidateSkills: Set[String], jobSkills: Set[String]): Option[SkillEvidence] = {
    val required = jobSkills.map(SearchTerm.canonical).filter(_.nonEmpty)
    val covered = required.intersect(candidateSkills.map(SearchTerm.canonical))
    Option.when(required.nonEmpty && covered.nonEmpty)(new SkillEvidence(covered.size, required.size))
  }
}

/** One authorized, already-retrieved hit. The only ranking inputs are the 1-based retrieval rank and skill evidence; no
  * field can carry a private attribute (residence, availability, consent, name).
  */
final case class RankingHit[Id](id: Id, retrievalRank: Int, evidence: Option[SkillEvidence])

enum RankingBasis {
  case RetrievalOrder, SkillEvidence
}

/** A hit in ranked position with its internal fused value. The fused value is internal and maps to no API field; list
  * order is authoritative.
  */
final case class RankedHit[Id](id: Id, retrievalRank: Int, evidence: Option[SkillEvidence], fusedValue: Double)

final case class RankingOutcome[Id](basis: RankingBasis, hits: List[RankedHit[Id]])

enum RankingInputError {
  case InvalidLimit(limit: Int)
  case TooManyHits(limit: Int)
  case NonPositiveRetrievalRank(rank: Int)
  case DuplicateRetrievalRank(rank: Int)
  case BlankKey
  case DuplicateHit(key: String)
}

/** Pure weight-free reciprocal rank fusion of retrieval rank and skill-evidence rank. A caller that receives a Left
  * keeps the retrieval order it already holds; the ranker never truncates or reorders on failure.
  */
object SkillEvidenceRanking {

  /** The HybridRankFusion constant, reused and not tuned. */
  val RankConstant: Int = 60

  def rank[Id](hits: List[RankingHit[Id]], maxHits: Int)(
      key: Id => String
  ): Either[NonEmptyChain[RankingInputError], RankingOutcome[Id]] =
    validate(hits, maxHits, key).toEither.map(_ => order(hits))

  private def validate[Id](
      hits: List[RankingHit[Id]],
      maxHits: Int,
      key: Id => String
  ): ValidatedNec[RankingInputError, Unit] = {
    val limit: ValidatedNec[RankingInputError, Unit] =
      if (maxHits < 1) RankingInputError.InvalidLimit(maxHits).invalidNec
      else if (hits.size > maxHits) RankingInputError.TooManyHits(maxHits).invalidNec
      else ().validNec
    val ranks = hits.map(_.retrievalRank)
    val nonPositive = ranks.filter(_ < 1).distinct.map(r => RankingInputError.NonPositiveRetrievalRank(r))
    val duplicateRanks =
      ranks.groupBy(identity).collect { case (r, occurrences) if occurrences.size > 1 => r }.toList.sorted
    val keys = hits.map(h => key(h.id))
    // Asymmetry by design: a key that is blank after trim is rejected, but duplicates are compared on the exact key,
    // so "a" and " a" are distinct keys. Keys are caller identifiers, not user text, and are not canonicalized here.
    val blank = Option.when(keys.exists(_.trim.isEmpty))(RankingInputError.BlankKey).toList
    val duplicateKeys =
      keys.filter(_.trim.nonEmpty).groupBy(identity).collect { case (k, o) if o.size > 1 => k }.toList.sorted
    val others: List[RankingInputError] =
      nonPositive ++ duplicateRanks.map(RankingInputError.DuplicateRetrievalRank(_)) ++
        blank ++ duplicateKeys.map(RankingInputError.DuplicateHit(_))
    limit.combine(others.traverse_(e => e.invalidNec[Unit]))
  }

  private def order[Id](hits: List[RankingHit[Id]]): RankingOutcome[Id] = {
    val withEvidence = hits.flatMap(h => h.evidence.map(e => (h, e)))
    if (withEvidence.isEmpty)
      RankingOutcome(
        RankingBasis.RetrievalOrder,
        hits.sortBy(_.retrievalRank).map(h => ranked(h, reciprocal(h.retrievalRank)))
      )
    else {
      val evidenceRank: Map[Int, Int] = withEvidence.map { case (h, e) =>
        // Competition rank: 1 plus the number of hits with strictly greater coverage, compared exactly.
        h.retrievalRank -> (1 + withEvidence.count { case (_, o) =>
          o.covered.toLong * e.required > e.covered.toLong * o.required
        })
      }.toMap
      val result = hits
        .map { h =>
          ranked(h, reciprocal(h.retrievalRank) + evidenceRank.get(h.retrievalRank).fold(0.0)(reciprocal))
        }
        .sortBy(r => (-r.fusedValue, r.retrievalRank))
      RankingOutcome(RankingBasis.SkillEvidence, result)
    }
  }

  private def ranked[Id](h: RankingHit[Id], value: Double): RankedHit[Id] =
    RankedHit(h.id, h.retrievalRank, h.evidence, value)

  // Computed in Double so a valid rank near Int.MaxValue cannot overflow: contract formula 1 / (60 + rank).
  private def reciprocal(rank: Int): Double = 1.0 / (RankConstant.toDouble + rank.toDouble)
}
