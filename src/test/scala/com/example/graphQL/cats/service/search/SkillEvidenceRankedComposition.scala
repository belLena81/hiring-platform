package com.example.graphQL.cats.service.search

import cats.data.NonEmptyChain
import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.{
  RankingBasis,
  RankingHit,
  RankingInputError,
  RankingOutcome,
  SkillEvidence,
  SkillEvidenceRanking
}
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.service.protocol.UseCaseF
import java.util.UUID

/** Test-source composition of the skill-evidence ranker around the public service output. It mirrors the adoption
  * placement of the design (after the authoritative eligible list, before `take(first)`) without any production caller:
  * the service is asked for the bound, the ranker orders the result, and the caller takes K. A ranker error keeps the
  * retrieval order (owner decision Q8).
  */
object SkillEvidenceRankedComposition {

  /** Names the ranker in replay and capture records (HDQ-07.11). */
  val TieRule: String =
    "skill-evidence RRF k=60 over retrieval rank and coverage competition rank; then retrieval rank ascending"
  val RankerName: String = "skill-evidence-rrf-k60"

  /** `basis` is None when the ranker refused the input and the retrieval order was kept. */
  final case class Selection[A](basis: Option[RankingBasis], items: List[A])

  type Ranked = Either[NonEmptyChain[RankingInputError], RankingOutcome[String]]

  /** The ranker boundary a test can observe: it sees exactly what the ranker would receive. */
  trait EvidenceRanker {
    def rank(hits: List[RankingHit[String]], maxHits: Int): IO[Ranked]
  }

  object EvidenceRanker {
    val live: EvidenceRanker = (hits, maxHits) => IO.pure(SkillEvidenceRanking.rank(hits, maxHits)(identity))
  }

  /** Records every invocation, then delegates. */
  final class RecordingEvidenceRanker(
      calls: Ref[IO, Vector[List[RankingHit[String]]]],
      delegate: EvidenceRanker
  ) extends EvidenceRanker {
    def rank(hits: List[RankingHit[String]], maxHits: Int): IO[Ranked] =
      calls.update(_ :+ hits) *> delegate.rank(hits, maxHits)
    def invocations: IO[Vector[List[RankingHit[String]]]] = calls.get
  }

  object RecordingEvidenceRanker {
    def create(delegate: EvidenceRanker = EvidenceRanker.live): IO[RecordingEvidenceRanker] =
      Ref.of[IO, Vector[List[RankingHit[String]]]](Vector.empty).map(new RecordingEvidenceRanker(_, delegate))
  }

  /** Retrieval rank is the 1-based position in the given (authoritative, already eligible) list. */
  def inputs[A](items: List[A], key: A => String, evidence: A => Option[SkillEvidence]): List[RankingHit[String]] =
    items.zipWithIndex.map { case (item, index) => RankingHit(key(item), index + 1, evidence(item)) }

  def settle[A](items: List[A], key: A => String, result: Ranked): Selection[A] = result match {
    case Right(outcome) =>
      val byKey = items.map(item => key(item) -> item).toMap
      Selection(Some(outcome.basis), outcome.hits.flatMap(hit => byKey.get(hit.id)))
    case Left(_) => Selection(None, items)
  }

  /** Pure form, used by replay. `arrange` permutes the list handed to the ranker; ranks are already assigned. */
  def select[A](
      items: List[A],
      key: A => String,
      evidence: A => Option[SkillEvidence],
      arrange: List[RankingHit[String]] => List[RankingHit[String]] = identity
  ): Selection[A] =
    settle(items, key, SkillEvidenceRanking.rank(arrange(inputs(items, key, evidence)), PageSize.Max)(identity))

  def selectVia[A](
      ranker: EvidenceRanker,
      items: List[A],
      key: A => String,
      evidence: A => Option[SkillEvidence]
  ): IO[Selection[A]] =
    ranker.rank(inputs(items, key, evidence), PageSize.Max).map(settle(items, key, _))

  /** `recommendedJobs` asked for the bound, ranked against the caller's own profile skills, then cut to `first`. */
  def recommendedJobs(
      service: SemanticSearchService[IO],
      ranker: EvidenceRanker,
      actor: ActorContext,
      profileSkills: Set[String],
      first: PageSize,
      searchId: UUID
  ): UseCaseF[IO, Selection[RankedJob]] =
    service
      .recommendedJobs(actor, bound, searchId)
      .semiflatMap(hits =>
        selectVia[RankedJob](
          ranker,
          hits,
          _.job.id.value.toString,
          hit => SkillEvidence.of(profileSkills, hit.job.skills)
        )
      )
      .map(cut(first))

  /** `candidateMatches` asked for the bound, ranked against the owned job's skills, then cut to `first`. */
  def candidateMatches(
      service: SemanticSearchService[IO],
      ranker: EvidenceRanker,
      actor: ActorContext,
      jobId: JobId,
      jobSkills: Set[String],
      first: PageSize,
      searchId: UUID,
      filters: CandidateMatchFilters = CandidateMatchFilters.empty
  ): UseCaseF[IO, Selection[SearchedCandidate]] =
    service
      .candidateMatches(actor, jobId, None, filters, bound, searchId)
      .semiflatMap(results =>
        selectVia[SearchedCandidate](
          ranker,
          results.hits,
          _.candidate.id.value.toString,
          hit => SkillEvidence.of(hit.candidate.skills, jobSkills)
        )
      )
      .map(cut(first))

  private def cut[A](first: PageSize): Selection[A] => Selection[A] = selection =>
    selection.copy(items = selection.items.take(first.value))

  private val bound: PageSize =
    PageSize.fromInt(PageSize.Max).toOption.getOrElse(throw new IllegalStateException("PageSize.Max is a valid size"))
}
