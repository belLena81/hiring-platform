package com.example.graphQL.cats.shared.search

import scala.collection.mutable

/** Deterministic reciprocal-rank fusion for the two retrieval views used by job search. */
object HybridRankFusion {
  val RankConstant: Int = 60

  def jobs(vector: List[RankedJob], lexical: List[RankedJob], limit: Int): List[RankedJob] = {
    final case class Entry(job: RankedJob, vectorRank: Option[Int], lexicalRank: Option[Int]) {
      val score: Double = vectorRank.fold(0.0)(rank => 1.0 / (RankConstant + rank)) +
        lexicalRank.fold(0.0)(rank => 1.0 / (RankConstant + rank))
    }

    val entries = mutable.LinkedHashMap.empty[String, Entry]
    vector.zipWithIndex.foreach { case (job, index) =>
      val key = job.job.id.value.toString
      entries.update(key, Entry(job, Some(index + 1), entries.get(key).flatMap(_.lexicalRank)))
    }
    lexical.zipWithIndex.foreach { case (job, index) =>
      val key = job.job.id.value.toString
      entries.get(key) match {
        case Some(existing) => entries.update(key, existing.copy(lexicalRank = Some(index + 1)))
        case None => entries.update(key, Entry(job, None, Some(index + 1)))
      }
    }

    entries.values.toList.sortWith { (left, right) =>
      if (left.score != right.score) left.score > right.score
      else if (left.vectorRank != right.vectorRank) compareRanks(left.vectorRank, right.vectorRank)
      else if (left.lexicalRank != right.lexicalRank) compareRanks(left.lexicalRank, right.lexicalRank)
      else left.job.job.id.value.toString < right.job.job.id.value.toString
    }.take(limit).map(entry => entry.job.copy(score = entry.score, mode = entry.job.mode))
  }

  private def compareRanks(left: Option[Int], right: Option[Int]): Boolean =
    left.getOrElse(Int.MaxValue) < right.getOrElse(Int.MaxValue)
}
