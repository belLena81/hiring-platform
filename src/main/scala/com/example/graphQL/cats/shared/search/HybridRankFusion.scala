package com.example.graphQL.cats.shared.search

/** Deterministic reciprocal-rank fusion for the two retrieval views used by job search. */
object HybridRankFusion {
  val RankConstant: Int = 60

  def jobs(vector: List[RankedJob], lexical: List[RankedJob], limit: Int): List[RankedJob] = {
    final case class Entry(job: RankedJob, vectorRank: Option[Int], lexicalRank: Option[Int]) {
      val score: Double = vectorRank.fold(0.0)(rank => 1.0 / (RankConstant + rank)) +
        lexicalRank.fold(0.0)(rank => 1.0 / (RankConstant + rank))
    }

    val vectorEntries = vector.zipWithIndex.foldLeft(Map.empty[String, Entry]) { case (entries, (job, index)) =>
      val key = job.job.id.value.toString
      entries.updated(key, Entry(job, Some(index + 1), entries.get(key).flatMap(_.lexicalRank)))
    }
    val entries = lexical.zipWithIndex.foldLeft(vectorEntries) { case (current, (job, index)) =>
      val key = job.job.id.value.toString
      current.updated(key, current.get(key).fold(Entry(job, None, Some(index + 1)))(_.copy(lexicalRank = Some(index + 1))))
    }

    entries.valuesIterator.toList.sortWith { (left, right) =>
      if (left.score != right.score) left.score > right.score
      else if (left.vectorRank != right.vectorRank) compareRanks(left.vectorRank, right.vectorRank)
      else if (left.lexicalRank != right.lexicalRank) compareRanks(left.lexicalRank, right.lexicalRank)
      else left.job.job.id.value.toString < right.job.job.id.value.toString
    }.take(limit).map(entry => entry.job.copy(score = entry.score, mode = entry.job.mode))
  }

  private def compareRanks(left: Option[Int], right: Option[Int]): Boolean =
    left.getOrElse(Int.MaxValue) < right.getOrElse(Int.MaxValue)
}
