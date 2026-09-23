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
      current.updated(
        key,
        current.get(key).fold(Entry(job, None, Some(index + 1)))(_.copy(lexicalRank = Some(index + 1)))
      )
    }

    entries.valuesIterator.toList
      .sortBy(entry =>
        (
          -entry.score,
          entry.vectorRank.getOrElse(Int.MaxValue),
          entry.lexicalRank.getOrElse(Int.MaxValue),
          entry.job.job.id.value.toString
        )
      )
      .take(limit)
      .map(entry => entry.job.copy(score = entry.score, mode = entry.job.mode))
  }
}
