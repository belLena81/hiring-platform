package com.example.graphQL.cats.service.search

import cats.data.NonEmptyChain
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.{RankingBasis, RankingHit, SkillEvidence}
import com.example.graphQL.cats.infrastructure.search.SearchEvaluationReportJson
import io.circe.Json

/** Applies the skill-evidence ranker on top of a captured baseline run, for the Recommendations and RecruiterMatching
  * use cases. Embeddings, retrieval and provider requests are the baseline's by construction, so any relevance change
  * comes from the ranker alone. JobSearch and unavailable or failed queries pass through unchanged. Test sources only.
  */
object SkillEvidenceRankedEvaluation {

  /** Skills the ranker may read: the entities' listed skills, and the skills of the query's own profile or owned job.
    * No residence, availability, consent or name is available here.
    */
  final case class Skills(entities: Map[String, Set[String]], queries: Map[String, Set[String]])

  /** The authored fixtures: entity skills, and each query intent's required skills as the caller's profile skills
    * (Recommendations) or the owned job's skills (RecruiterMatching).
    */
  val authoredFixtures: Skills = Skills(
    (SearchEvaluationFixtures.jobs ++ SearchEvaluationFixtures.candidates).map(e => e.id -> e.skills).toMap,
    SearchEvaluationFixtures.intents.map(i => i.queryId -> i.requiredSkills).toMap
  )

  /** Measures the ranker's own elapsed time per query, in milliseconds. Injected so tests supply fixed durations. */
  trait RankerTimer {
    def time[A](work: => A): (A, Double)
  }

  object RankerTimer {

    /** Monotonic wall time of the ranker call itself. */
    val live: RankerTimer = new RankerTimer {
      def time[A](work: => A): (A, Double) = {
        val start = System.nanoTime()
        val result = work
        (result, (System.nanoTime() - start) / 1000000.0)
      }
    }

    def fixed(millis: Double): RankerTimer = new RankerTimer {
      def time[A](work: => A): (A, Double) = (work, millis)
    }
  }

  enum RankerOutcome {

    /** JobSearch, or the baseline query had no ranking to reorder. */
    case OutOfScope

    /** The ranker refused the input and the retrieval order was kept. */
    case Fallback
    case Applied(basis: RankingBasis)
  }

  final case class Observed(query: SearchEvaluationQuery, outcome: RankerOutcome)
  final case class RankedRun(run: SearchEvaluationRun, outcomes: Map[String, RankerOutcome])

  /** The ranked observation for one fixture query. `arrange` permutes the list handed to the ranker (ranks are already
    * assigned), which must not change the result.
    */
  def observation(
      fixture: SearchEvaluationFixtureQuery,
      baseline: SearchEvaluationQuery,
      skills: Skills = authoredFixtures,
      arrange: List[RankingHit[String]] => List[RankingHit[String]] = identity,
      timer: RankerTimer = RankerTimer.live
  ): Observed = {
    val querySkills = skills.queries.getOrElse(fixture.queryId, Set.empty)
    def listed(id: String): Set[String] = skills.entities.getOrElse(id, Set.empty)
    val evidence: Option[String => Option[SkillEvidence]] = fixture.useCase match {
      case SearchEvaluationUseCase.Recommendations   => Some(id => SkillEvidence.of(querySkills, listed(id)))
      case SearchEvaluationUseCase.RecruiterMatching => Some(id => SkillEvidence.of(listed(id), querySkills))
      case SearchEvaluationUseCase.JobSearch         => None
    }
    (evidence, baseline.ranking) match {
      case (Some(evidenceOf), SearchEvaluationRanking.Succeeded(ids, latency)) =>
        val (selection, rankerMillis) =
          timer.time(SkillEvidenceRankedComposition.select[String](ids, identity, evidenceOf, arrange))
        // Observed latency plus the measured ranker time; an absent baseline latency stays absent (never invented).
        Observed(
          baseline.copy(ranking = SearchEvaluationRanking.Succeeded(selection.items, latency.map(_ + rankerMillis))),
          selection.basis.fold[RankerOutcome](RankerOutcome.Fallback)(RankerOutcome.Applied(_))
        )
      case _ => Observed(baseline, RankerOutcome.OutOfScope)
    }
  }

  /** Truncation guard. The composition asks the service for PageSize.Max, ranks, then takes K, but a capture holds at
    * most `captureBound` results (K today), so a hit retrieved beyond that bound could never be promoted. Rule: a
    * ranked query is accepted only if its captured list is strictly shorter than the declared `captureBound` (a capture
    * that stopped short of its bound is TAKEN AS EVIDENCE that the whole eligible set was captured, assuming the
    * service returns every eligible hit whenever it returns fewer than `first`; ANN numCandidates and post-retrieval
    * drops could break that assumption, see "Not evidenced" in the discovery search quality spec) or the bound already
    * reaches the ranker's own bound PageSize.Max (the composition's bound). Otherwise the evaluation fails with a typed
    * error, never silently ranking a possibly cut list. Checked for every query the ranker would reorder.
    */
  def truncationErrors(
      baseline: SearchEvaluationRun,
      fixtures: List[SearchEvaluationFixtureQuery],
      captureBound: Int
  ): List[SearchEvaluationError] = {
    val ranked = fixtures.filter(_.useCase != SearchEvaluationUseCase.JobSearch).map(_.queryId).toSet
    baseline.queries.collect {
      case SearchEvaluationQuery(id, SearchEvaluationRanking.Succeeded(ids, _), _)
          if ranked(id) && captureBound < PageSize.Max && ids.size >= captureBound =>
        SearchEvaluationError.PossiblyTruncatedCapture(id)
    }
  }

  /** `captureBound` is the largest result count the baseline capture could hold (the K the capture was taken with). */
  def rankedRun(
      baseline: SearchEvaluationRun,
      captureBound: Int,
      fixtures: List[SearchEvaluationFixtureQuery] = SearchEvaluationFixtures.queries,
      skills: Skills = authoredFixtures,
      arrange: List[RankingHit[String]] => List[RankingHit[String]] = identity,
      timer: RankerTimer = RankerTimer.live
  ): Either[NonEmptyChain[SearchEvaluationError], RankedRun] =
    NonEmptyChain.fromSeq(truncationErrors(baseline, fixtures, captureBound)).toLeft(()).map { _ =>
      unguardedRankedRun(baseline, fixtures, skills, arrange, timer)
    }

  private def unguardedRankedRun(
      baseline: SearchEvaluationRun,
      fixtures: List[SearchEvaluationFixtureQuery],
      skills: Skills,
      arrange: List[RankingHit[String]] => List[RankingHit[String]],
      timer: RankerTimer
  ): RankedRun = {
    val byId = fixtures.map(f => f.queryId -> f).toMap
    val observed =
      baseline.queries.map(q => q.queryId -> byId.get(q.queryId).map(observation(_, q, skills, arrange, timer)))
    RankedRun(
      baseline.copy(
        strategy = SearchEvaluationStrategy.SkillEvidenceRanked(baseline.strategy),
        coordinates = baseline.coordinates.copy(tieRule = SkillEvidenceRankedComposition.TieRule),
        queries = baseline.queries.zip(observed).map { case (q, (_, o)) => o.fold(q)(_.query) }
      ),
      observed.collect { case (id, Some(o)) => id -> o.outcome }.toMap
    )
  }

  /** What a replay or capture record names about the ranker (HDQ-07.11). */
  final case class Record(
      ranker: String,
      tieRule: String,
      underlyingRetrieval: SearchEvaluationStrategy,
      retrievalOrderQueries: Int,
      skillEvidenceQueries: Int,
      fallbackQueries: Int,
      outOfScopeQueries: Int
  )

  def record(ranked: RankedRun): Record = {
    val outcomes = ranked.outcomes.values.toList
    Record(
      SkillEvidenceRankedComposition.RankerName,
      ranked.run.coordinates.tieRule,
      ranked.run.strategy.retrieval,
      outcomes.count(_ == RankerOutcome.Applied(RankingBasis.RetrievalOrder)),
      outcomes.count(_ == RankerOutcome.Applied(RankingBasis.SkillEvidence)),
      outcomes.count(_ == RankerOutcome.Fallback),
      outcomes.count(_ == RankerOutcome.OutOfScope)
    )
  }

  def renderRecord(value: Record): Json = Json.obj(
    "ranker" -> Json.fromString(value.ranker),
    "tieRule" -> Json.fromString(value.tieRule),
    "underlyingRetrieval" -> Json.fromString(value.underlyingRetrieval.toString),
    "retrievalOrderQueries" -> Json.fromInt(value.retrievalOrderQueries),
    "skillEvidenceQueries" -> Json.fromInt(value.skillEvidenceQueries),
    "fallbackQueries" -> Json.fromInt(value.fallbackQueries),
    "outOfScopeQueries" -> Json.fromInt(value.outOfScopeQueries)
  )

  def renderReport(report: SearchEvaluationReport, value: Record): Json =
    SearchEvaluationReportJson.render(report).deepMerge(Json.obj("skillEvidenceRanker" -> renderRecord(value)))
}
