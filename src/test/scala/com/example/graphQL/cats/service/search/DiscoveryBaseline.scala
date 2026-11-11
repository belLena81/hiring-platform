package com.example.graphQL.cats.service.search

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all.*

import java.time.{Duration, Instant}
import java.util.Locale

/** Local baseline measures for job discovery and recruiter follow-up (GDF-07). Pure functions over synthetic
  * observations: no clock, no randomness, no I/O, no personal data. The result is a LOCAL SYNTHETIC measure; it sets no
  * target and proves no production result.
  */
object DiscoveryBaseline {

  /** One search and the first result a person clicked afterwards, if any. `firstClickRank` is 1-based. */
  final case class SearchObservation private (resultCount: Int, firstClickRank: Option[Int])

  object SearchObservation {
    enum Invalid { case NegativeResultCount, ClickRankOutsideResults }

    def validated(resultCount: Int, firstClickRank: Option[Int]): ValidatedNel[Invalid, SearchObservation] =
      (
        Validated.condNel(resultCount >= 0, resultCount, Invalid.NegativeResultCount),
        Validated.condNel(
          firstClickRank.forall(rank => rank >= 1 && rank <= resultCount),
          firstClickRank,
          Invalid.ClickRankOutsideResults
        )
      ).mapN(SearchObservation(_, _))
  }

  /** One application and the time of its first status change, if a recruiter has acted. */
  final case class FollowUpObservation private (appliedAt: Instant, firstStatusChangeAt: Option[Instant])

  object FollowUpObservation {
    enum Invalid { case StatusChangeBeforeApplication }

    def validated(
        appliedAt: Instant,
        firstStatusChangeAt: Option[Instant]
    ): ValidatedNel[Invalid, FollowUpObservation] =
      Validated.condNel(
        firstStatusChangeAt.forall(!_.isBefore(appliedAt)),
        FollowUpObservation(appliedAt, firstStatusChangeAt),
        Invalid.StatusChangeBeforeApplication
      )
  }

  final case class RankSummary(clickedSearches: Int, meanRank: Double, medianRank: Double)
  final case class FollowUpSummary(reviewedApplications: Int, meanSeconds: Double, medianSeconds: Double)

  /** Each summary is `None` when its population is empty: absence is not zero. */
  final case class Report(
      searches: Int,
      zeroResultRate: Option[Double],
      firstClickRank: Option[RankSummary],
      clickedSearchShare: Option[Double],
      applications: Int,
      unreviewedApplications: Int,
      applicationToFirstStatusChange: Option[FollowUpSummary]
  ) {
    val label: String = "LOCAL SYNTHETIC BASELINE - no target set, no production claim"

    /** Aggregates only; contains no per-search or per-application data. */
    def render: String = {
      def pct(value: Option[Double]) = value.fold("n/a")(v => "%.1f%%".formatLocal(Locale.ROOT, v * 100))
      List(
        label,
        s"searches=$searches zeroResultRate=${pct(zeroResultRate)} clickedSearchShare=${pct(clickedSearchShare)}",
        firstClickRank.fold("firstClickRank=n/a")(r =>
          "firstClickRank clicked=%d mean=%.2f median=%.1f"
            .formatLocal(Locale.ROOT, r.clickedSearches, r.meanRank, r.medianRank)
        ),
        s"applications=$applications unreviewed=$unreviewedApplications",
        applicationToFirstStatusChange.fold("applicationToFirstStatusChange=n/a")(s =>
          "applicationToFirstStatusChange reviewed=%d meanSeconds=%.1f medianSeconds=%.1f"
            .formatLocal(Locale.ROOT, s.reviewedApplications, s.meanSeconds, s.medianSeconds)
        )
      ).mkString("\n")
    }
  }

  def measure(searches: List[SearchObservation], followUps: List[FollowUpObservation]): Report = {
    val total = searches.size
    val ranks = searches.flatMap(_.firstClickRank)
    val seconds =
      followUps.flatMap(f => f.firstStatusChangeAt.map(changed => Duration.between(f.appliedAt, changed).getSeconds))
    Report(
      searches = total,
      zeroResultRate = ratio(searches.count(_.resultCount == 0), total),
      firstClickRank = Option.when(ranks.nonEmpty)(
        RankSummary(ranks.size, ranks.sum.toDouble / ranks.size, median(ranks.map(_.toDouble)))
      ),
      clickedSearchShare = ratio(ranks.size, total),
      applications = followUps.size,
      unreviewedApplications = followUps.size - seconds.size,
      applicationToFirstStatusChange = Option.when(seconds.nonEmpty)(
        FollowUpSummary(seconds.size, seconds.sum.toDouble / seconds.size, median(seconds.map(_.toDouble)))
      )
    )
  }

  private def ratio(part: Int, whole: Int): Option[Double] = Option.when(whole > 0)(part.toDouble / whole)

  private def median(values: List[Double]): Double = {
    val sorted = values.sorted
    val n = sorted.size
    if (n % 2 == 1) sorted(n / 2) else (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0
  }
}

/** Deterministic synthetic corpus for the baseline: a fixed seed always yields the same observations. */
object DiscoveryBaselineCorpus {
  import DiscoveryBaseline.*

  final case class Config(
      seed: Long,
      searches: Int,
      applications: Int,
      zeroResultRate: Double,
      clickRate: Double,
      maxResults: Int,
      reviewedRate: Double,
      maxReviewHours: Int,
      epoch: Instant
  )

  object Config {
    enum Invalid {
      case NoSearches, NoApplications, RateOutsideUnitInterval, NoResultCapacity, NoReviewWindow,
        ResultCapacityTooLarge, ReviewWindowTooLong
    }

    /** Upper bounds keep `maxReviewHours * 60` and result counts far from Int overflow. */
    val MaxResultCapacity: Int = 10000
    val MaxReviewHours: Int = 24 * 365

    /** A configuration that passed `validated`; the only input `generate` accepts. */
    final case class Valid private[Config] (config: Config)

    /** Local fixed default; not an operational setting and not a target. */
    val Local: Config = Config(
      seed = 11L,
      searches = 400,
      applications = 200,
      zeroResultRate = 0.15,
      clickRate = 0.45,
      maxResults = 20,
      reviewedRate = 0.8,
      maxReviewHours = 72,
      epoch = Instant.parse("2026-01-01T00:00:00Z")
    )

    def validated(config: Config): ValidatedNel[Invalid, Valid] = {
      val rates = List(config.zeroResultRate, config.clickRate, config.reviewedRate)
      List(
        Validated.condNel(config.searches > 0, (), Invalid.NoSearches),
        Validated.condNel(config.applications > 0, (), Invalid.NoApplications),
        Validated.condNel(rates.forall(r => r >= 0.0 && r <= 1.0), (), Invalid.RateOutsideUnitInterval),
        Validated.condNel(config.maxResults >= 1, (), Invalid.NoResultCapacity),
        Validated.condNel(config.maxResults <= MaxResultCapacity, (), Invalid.ResultCapacityTooLarge),
        Validated.condNel(config.maxReviewHours >= 1, (), Invalid.NoReviewWindow),
        Validated.condNel(config.maxReviewHours <= MaxReviewHours, (), Invalid.ReviewWindowTooLong)
      ).sequence_.as(Valid(config))
    }
  }

  /** SplitMix64: a pure step from state to (next state, value in [0, 1)). */
  private def next(state: Long): (Long, Double) = {
    val s = state + 0x9e3779b97f4a7c15L
    val z1 = (s ^ (s >>> 30)) * 0xbf58476d1ce4e5b9L
    val z2 = (z1 ^ (z1 >>> 27)) * 0x94d049bb133111ebL
    val z3 = z2 ^ (z2 >>> 31)
    (s, (z3 >>> 11).toDouble / (1L << 53).toDouble)
  }

  private def below(state: Long, bound: Int): (Long, Int) = {
    val (s, u) = next(state)
    (s, math.min((u * bound).toInt, bound - 1))
  }

  def generate(valid: Config.Valid): (List[SearchObservation], List[FollowUpObservation]) = {
    val config = valid.config
    val (afterSearches, searches) = (1 to config.searches).foldLeft((config.seed, Vector.empty[SearchObservation])) {
      case ((s0, acc), _) =>
        val (s1, zero) = next(s0)
        if (zero < config.zeroResultRate) (s1, acc :+ observation(0, None))
        else {
          val (s2, extra) = below(s1, config.maxResults)
          val count = extra + 1
          val (s3, click) = next(s2)
          val (s4, rank) = below(s3, count)
          (s4, acc :+ observation(count, Option.when(click < config.clickRate)(rank + 1)))
        }
    }
    val (_, followUps) = (1 to config.applications).foldLeft((afterSearches, Vector.empty[FollowUpObservation])) {
      case ((s0, acc), index) =>
        val appliedAt = config.epoch.plusSeconds(index.toLong * 3600L)
        val (s1, reviewed) = next(s0)
        val (s2, minutes) = below(s1, config.maxReviewHours * 60)
        val changed = Option.when(reviewed < config.reviewedRate)(appliedAt.plus(Duration.ofMinutes(minutes + 1L)))
        (s2, acc :+ followUp(appliedAt, changed))
    }
    (searches.toList, followUps.toList)
  }

  private def followUp(appliedAt: Instant, changed: Option[Instant]): FollowUpObservation =
    FollowUpObservation
      .validated(appliedAt, changed)
      .valueOr(errors => throw new IllegalStateException(s"synthetic corpus invariant broken: ${errors.size}"))

  private def observation(count: Int, rank: Option[Int]): SearchObservation =
    SearchObservation
      .validated(count, rank)
      .valueOr(errors => throw new IllegalStateException(s"synthetic corpus invariant broken: ${errors.size}"))
}
