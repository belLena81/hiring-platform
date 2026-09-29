package com.example.graphQL.cats.service.search

/** Offline ranking measures for comparing bounded search runs against judged synthetic queries. */
object SearchEvaluationMetrics {
  def recallAtK(rankedIds: List[String], relevantIds: Set[String], k: Int): Double =
    if (relevantIds.isEmpty || k <= 0) 0.0
    else rankedIds.take(k).toSet.intersect(relevantIds).size.toDouble / relevantIds.size.toDouble

  def ndcgAtK(rankedIds: List[String], relevantIds: Set[String], k: Int): Double = {
    if (k <= 0) 0.0
    else {
      def gain(rank: Int): Double = 1.0 / (math.log(rank + 1.0) / math.log(2.0))
      val dcg = rankedIds
        .take(k)
        .zipWithIndex
        .collect {
          case (id, index) if relevantIds.contains(id) => gain(index + 1)
        }
        .sum
      val ideal = (1 to math.min(k, relevantIds.size)).map(gain).sum
      if (ideal == 0.0) 0.0 else dcg / ideal
    }
  }
}
