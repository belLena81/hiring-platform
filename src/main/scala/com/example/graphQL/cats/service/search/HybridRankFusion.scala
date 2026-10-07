package com.example.graphQL.cats.service.search

/** Deterministic reciprocal-rank fusion for the two retrieval views used by job search. */
object HybridRankFusion {
  val RankConstant: Int = 60

  def retrieval[Id](branches: List[List[SearchRetrievalHit[Id]]], limit: Int)(
      identifier: Id => String
  ): List[SearchRetrievalHit[Id]] = {
    final case class Entry(value: SearchRetrievalHit[Id], ranks: Vector[Option[Int]]) {
      val score: Double = ranks.flatten.map(rank => 1d / (RankConstant + rank)).sum
    }
    val entries = branches.zipWithIndex.foldLeft(Map.empty[Id, Entry]) { case (all, (branch, branchIndex)) =>
      branch.distinctBy(_.id).zipWithIndex.foldLeft(all) { case (current, (value, index)) =>
        val entry = current.getOrElse(value.id, Entry(value, Vector.fill(branches.size)(None)))
        current.updated(value.id, entry.copy(ranks = entry.ranks.updated(branchIndex, Some(index + 1))))
      }
    }
    def before(left: Entry, right: Entry): Boolean = {
      if (left.score != right.score) left.score > right.score
      else
        left.ranks
          .zip(right.ranks)
          .collectFirst {
            case (a, b) if a != b => a.getOrElse(Int.MaxValue) < b.getOrElse(Int.MaxValue)
          }
          .getOrElse(identifier(left.value.id) < identifier(right.value.id))
    }
    entries.values.toList.sortWith(before).take(limit).map(entry => entry.value.copy(score = entry.score))
  }

  def candidates(
      jobVector: List[RankedCandidate],
      queryVector: List[RankedCandidate],
      lexical: List[RankedCandidate],
      limit: Int
  ): List[RankedCandidate] = {
    val values = (jobVector ++ queryVector ++ lexical)
      .distinctBy(_.candidate.id)
      .map(value => value.candidate.id -> value)
      .toMap
    retrieval(List(jobVector.map(_.retrieval), queryVector.map(_.retrieval), lexical.map(_.retrieval)), limit)(
      _.value.toString
    )
      .flatMap(hit => values.get(hit.id).map(_.copy(score = hit.score)))
  }

  def jobs(vector: List[RankedJob], lexical: List[RankedJob], limit: Int): List[RankedJob] = {
    val values = (vector ++ lexical).distinctBy(_.job.id).map(value => value.job.id -> value).toMap
    retrieval(List(vector.map(_.retrieval), lexical.map(_.retrieval)), limit)(_.value.toString)
      .flatMap(hit => values.get(hit.id).map(_.copy(score = hit.score)))
  }
}
