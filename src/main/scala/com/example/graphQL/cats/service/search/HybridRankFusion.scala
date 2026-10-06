package com.example.graphQL.cats.service.search

/** Deterministic reciprocal-rank fusion for the two retrieval views used by job search. */
object HybridRankFusion {
  val RankConstant: Int = 60

  def candidates(
      jobVector: List[RankedCandidate],
      queryVector: List[RankedCandidate],
      lexical: List[RankedCandidate],
      limit: Int
  ): List[RankedCandidate] = {
    final case class Entry(
        value: RankedCandidate,
        jobVectorRank: Option[Int],
        queryVectorRank: Option[Int],
        lexicalRank: Option[Int]
    ) {
      val score: Double = List(jobVectorRank, queryVectorRank, lexicalRank).flatten
        .map(rank => 1.0 / (RankConstant + rank))
        .sum
    }
    def addBranch(entries: Map[String, Entry], branch: List[RankedCandidate])(
        withRank: (Entry, Int) => Entry
    ): Map[String, Entry] =
      branch.zipWithIndex.foldLeft(entries) { case (current, (candidate, index)) =>
        val id = candidate.candidate.id.value.toString
        val entry = current.getOrElse(id, Entry(candidate, None, None, None))
        current.updated(id, withRank(entry, index + 1))
      }
    val jobVectorEntries =
      addBranch(Map.empty[String, Entry], jobVector)((entry, rank) => entry.copy(jobVectorRank = Some(rank)))
    val queryVectorEntries =
      addBranch(jobVectorEntries, queryVector)((entry, rank) => entry.copy(queryVectorRank = Some(rank)))
    val entries = addBranch(queryVectorEntries, lexical)((entry, rank) => entry.copy(lexicalRank = Some(rank)))
    entries.values.toList
      .sortBy(entry =>
        (
          -entry.score,
          entry.jobVectorRank.getOrElse(Int.MaxValue),
          entry.queryVectorRank.getOrElse(Int.MaxValue),
          entry.lexicalRank.getOrElse(Int.MaxValue),
          entry.value.candidate.id.value.toString
        )
      )
      .take(limit)
      .map(entry => entry.value.copy(score = entry.score))
  }

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
