package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, JobStatus, SearchMode}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.ServiceFixtures
import com.example.graphQL.cats.shared.crypto.SourceHash
import java.time.Instant
import java.util.UUID

/** Intentionally authored fabricated inputs, not captured retrieval or reviewed human labels. */
object SearchEvaluationFixtures {
  final case class Entity(
      id: String,
      intendedRole: String,
      skills: Set[String],
      summary: String,
      active: Boolean = true,
      deleted: Boolean = false,
      stale: Boolean = false,
      optIn: Option[Boolean] = None,
      privateAttributesPresent: Boolean = false
  )
  def jobId(number: Int): String = f"00000000-0000-0000-0000-${number}%012d"
  def candidateId(number: Int): String = f"00000000-0000-0000-0001-${number}%012d"
  val K: Int = 7
  val jobs: List[Entity] = List(
    Entity(jobId(1), "Scala backend", Set("scala", "cats"), "Build functional distributed APIs"),
    Entity(jobId(2), "Java backend", Set("java"), "Maintain JVM payment APIs"),
    Entity(jobId(3), "Frontend", Set("typescript", "react"), "Build accessible browser interfaces"),
    Entity(jobId(4), "Data engineering", Set("scala", "spark"), "Build bounded batch ingestion"),
    Entity(jobId(5), "Scala backend", Set("scala", "cats"), "Develop typed hiring services"),
    Entity(jobId(6), "Scala backend", Set("scala"), "Closed role", active = false),
    Entity(jobId(7), "Scala backend", Set("scala"), "Deleted role", deleted = true),
    Entity(jobId(8), "Scala backend", Set("scala"), "Changed source with stale vector", stale = true)
  )
  val candidates: List[Entity] = List(
    Entity(
      candidateId(1),
      "Scala backend",
      Set("scala", "cats"),
      "Built functional service APIs",
      optIn = Some(true),
      privateAttributesPresent = true
    ),
    Entity(
      candidateId(2),
      "Java backend",
      Set("java"),
      "Built JVM service APIs",
      optIn = Some(true),
      privateAttributesPresent = true
    ),
    Entity(
      candidateId(3),
      "Scala backend",
      Set("scala", "cats"),
      "Built typed APIs",
      optIn = Some(false),
      privateAttributesPresent = true
    ),
    Entity(candidateId(4), "Data engineering", Set("scala", "spark"), "Built batch data pipelines"),
    Entity(candidateId(5), "Frontend", Set("react"), "Built accessible interfaces", optIn = Some(true)),
    Entity(candidateId(6), "Scala backend", Set("scala"), "Inactive account", active = false),
    Entity(candidateId(7), "Scala backend", Set("scala"), "Deleted account", deleted = true),
    Entity(candidateId(8), "Scala backend", Set("scala"), "Changed source with stale vector", stale = true)
  )
  private val eligibleJobs = jobs.filter(value => value.active && !value.deleted && !value.stale).map(_.id)
  private val eligibleCandidates = candidates.filter(value => value.active && !value.deleted && !value.stale).map(_.id)
  val rubric = SearchEvaluationRubric(
    "binary-intended-role-required-skills",
    SearchEvaluationJudgmentOrigin.ProvisionalFabricated,
    approved = true,
    "user-approved rubric; per-query domain review pending"
  )
  val queries: List[SearchEvaluationFixtureQuery] = SearchEvaluationUseCase.values.toList.flatMap { useCase =>
    (0 until 4).toList.map { index =>
      val candidateSearch = useCase == SearchEvaluationUseCase.RecruiterMatching
      val eligible = index match {
        case 1                    => Nil
        case 3 if candidateSearch => List(candidateId(1), candidateId(3), candidateId(4))
        case 3                    => List(jobId(1), jobId(5))
        case _ if candidateSearch => eligibleCandidates
        case _                    => eligibleJobs
      }
      val relevant = index match {
        case 1                    => Set.empty[String]
        case 2 if candidateSearch => Set(candidateId(4))
        case 2                    => Set(jobId(4))
        case _ if candidateSearch => Set(candidateId(1), candidateId(3))
        case _                    => Set(jobId(1), jobId(5))
      }
      SearchEvaluationFixtureQuery(
        s"${useCase.toString}-$index",
        useCase,
        if (index == 1) SearchEvaluationFilterGroup.Empty
        else if (index == 3) SearchEvaluationFilterGroup.Selective
        else SearchEvaluationFilterGroup.Broad,
        if (index < 2) SearchEvaluationSplit.Tuning else SearchEvaluationSplit.HeldOut,
        s"fabricated-${useCase.toString}-filter-$index",
        eligible,
        relevant,
        SearchEvaluationLabelReview.Pending
      )
    }
  }
  final case class QueryIntent(
      queryId: String,
      intendedRole: String,
      requiredSkills: Set[String],
      relevanceRationales: Map[String, String]
  )
  val intents: List[QueryIntent] = queries.map { query =>
    val dataRole = query.queryId.endsWith("-2")
    val role = if (dataRole) "Data engineering" else "Scala backend"
    val required = if (dataRole) Set("scala", "spark") else Set("scala", "cats")
    val entities = if (query.useCase == SearchEvaluationUseCase.RecruiterMatching) candidates else jobs
    val rationales = query.eligibleIds
      .flatMap(id =>
        entities.find(_.id == id).map { entity =>
          val reason =
            if (entity.intendedRole != role) "Intended role differs"
            else if (!required.subsetOf(entity.skills)) "Required skills are missing"
            else "Intended role and every required skill match"
          id -> reason
        }
      )
      .toMap
    QueryIntent(query.queryId, role, required, rationales)
  }
  private val canonical = (jobs ++ candidates).map(value =>
    s"${value.id}|${value.intendedRole}|${value.skills.toList.sorted.mkString(",")}|${value.summary}|${value.active}|${value.deleted}|${value.stale}|${value.optIn}|${value.privateAttributesPresent}"
  ) ++
    queries.map(value =>
      s"${value.queryId}|${value.useCase}|${value.filterGroup}|${value.split}|${value.filterIdentity}|${value.eligibleIds.mkString(",")}|${value.relevantIds.toList.sorted.mkString(",")}|${value.labelReview}"
    ) ++
    intents.map(value =>
      s"${value.queryId}|${value.intendedRole}|${value.requiredSkills.toList.sorted.mkString(",")}|${value.relevanceRationales.toList.sortBy(_._1).mkString(",")}"
    )
  val corpus: SearchEvaluationCorpus = SearchEvaluationCorpus(
    "fabricated-hiring-role-skills",
    SourceHash.sha256((rubric.toString :: canonical).mkString("\n")),
    20261006L,
    (jobs ++ candidates).map(_.id),
    rubric,
    queries
  )
  val environment: SearchEvaluationEnvironment = SearchEvaluationEnvironment(
    "offline-authored-ranking-replay",
    None,
    None,
    None,
    None,
    None,
    Some(0L),
    Map(
      SearchEvaluationTelemetry.Cpu -> SearchEvaluationTelemetryReason.NotCaptured,
      SearchEvaluationTelemetry.Memory -> SearchEvaluationTelemetryReason.NotCaptured,
      SearchEvaluationTelemetry.CollectionIndex -> SearchEvaluationTelemetryReason.RequiresAtlas,
      SearchEvaluationTelemetry.VectorIndex -> SearchEvaluationTelemetryReason.RequiresAtlas,
      SearchEvaluationTelemetry.Billing -> SearchEvaluationTelemetryReason.NoProviderUsed,
      SearchEvaluationTelemetry.AtlasQueryMetrics -> SearchEvaluationTelemetryReason.RequiresAtlas,
      SearchEvaluationTelemetry.Latency -> SearchEvaluationTelemetryReason.NotCaptured,
      SearchEvaluationTelemetry.AtlasVersion -> SearchEvaluationTelemetryReason.RequiresAtlas
    )
  )

  def emptyRun(
      strategy: SearchEvaluationStrategy,
      revision: String,
      fingerprint: String,
      timestamp: Instant
  ): SearchEvaluationRun = {
    val coordinates = SearchEvaluationCoordinates(
      revision,
      fingerprint,
      corpus.identity,
      corpus.digest,
      rubric.identity,
      SearchEvaluationRankingOrigin.AuthoredFixture,
      "authored-branch-inputs",
      17,
      "no-live-index-authored-fixture",
      8,
      8,
      K,
      "RRF score descending, branch ranks ascending, UUID ascending"
    )
    SearchEvaluationRun(strategy, coordinates, 1, 0, false, timestamp, None, environment, Nil)
  }

  def run(
      strategy: SearchEvaluationStrategy,
      revision: String,
      fingerprint: String,
      timestamp: Instant
  ): SearchEvaluationRun =
    emptyRun(strategy, revision, fingerprint, timestamp).copy(queries = queries.map(observation(_, strategy)))

  def observation(fixture: SearchEvaluationFixtureQuery, strategy: SearchEvaluationStrategy): SearchEvaluationQuery = {

    val vector = fixture.eligibleIds.reverse
    val lexical = fixture.eligibleIds
    val ids = strategy match {
      case SearchEvaluationStrategy.Lexical        => lexical.take(K)
      case SearchEvaluationStrategy.Vector         => vector.take(K)
      case SearchEvaluationStrategy.ApplicationRrf => fuse(fixture, vector, lexical)
      case SearchEvaluationStrategy.AtlasAnn       => Nil
    }
    val unavailable = strategy == SearchEvaluationStrategy.AtlasAnn ||
      (fixture.useCase == SearchEvaluationUseCase.Recommendations && strategy != SearchEvaluationStrategy.Vector)
    val ranking =
      if (unavailable)
        SearchEvaluationRanking.Unavailable(
          if (strategy == SearchEvaluationStrategy.AtlasAnn) SearchEvaluationUnavailable.RequiresAtlas
          else SearchEvaluationUnavailable.UnsupportedUseCase
        )
      else SearchEvaluationRanking.Succeeded(ids, None)
    SearchEvaluationQuery(
      fixture.queryId,
      ranking,
      SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.NotCaptured)
    )
  }

  private def fuse(fixture: SearchEvaluationFixtureQuery, vector: List[String], lexical: List[String]): List[String] = {
    val meta = EmbeddingMeta("authored-branch-inputs", "authored-fixture-source", ServiceFixtures.now)
    val searchId = UUID.fromString("00000000-0000-0000-0002-000000000001")
    if (fixture.useCase == SearchEvaluationUseCase.RecruiterMatching) {
      def branch(ids: List[String]): List[RankedCandidate] = ids.flatMap(id =>
        candidates
          .find(_.id == id)
          .map(value =>
            RankedCandidate(
              CandidateSearchHit(
                UserId(UUID.fromString(id)),
                "Fabricated candidate",
                value.skills,
                Some(value.summary)
              ),
              1.0,
              SearchMode.HYBRID,
              meta,
              searchId
            )
          )
      )
      HybridRankFusion.candidates(branch(vector), Nil, branch(lexical), K).map(_.candidate.id.value.toString)
    } else {
      def branch(ids: List[String]): List[RankedJob] = ids.flatMap(id =>
        jobs
          .find(_.id == id)
          .map(value =>
            RankedJob(
              ServiceFixtures.openJob.copy(
                id = JobId(UUID.fromString(id)),
                title = value.intendedRole,
                description = value.summary,
                skills = value.skills,
                status = JobStatus.Open
              ),
              1.0,
              SearchMode.HYBRID,
              meta,
              searchId
            )
          )
      )
      HybridRankFusion.jobs(branch(vector), branch(lexical), K).map(_.job.id.value.toString)
    }
  }
}
