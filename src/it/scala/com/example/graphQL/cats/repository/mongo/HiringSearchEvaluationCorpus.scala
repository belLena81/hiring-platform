package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.ServiceFixtures
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import java.util.UUID
import org.bson.Document

/** Exact curated fabricated sources, with explicitly synthetic (nonsemantic) embeddings. */
private[mongo] object HiringSearchEvaluationCorpus {
  val Model = "curated-synthetic-fixture"
  val Dimensions = 17
  val recruiter = ServiceFixtures.recruiter.copy(id = UserId(UUID.fromString("00000000-0000-0000-0002-000000000001")))
  def vector(skills: Set[String]): List[Float] = {
    val vocabulary = List("scala", "cats", "java", "typescript", "react", "spark")
    val values = vocabulary.map(skill => if (skills.contains(skill)) 1.0f else 0.0f) ++ List.fill(11)(0.1f)
    val norm = math.sqrt(values.map(value => value.toDouble * value.toDouble).sum).toFloat
    values.map(_ / norm)
  }
  def embedding(text: String, skills: Set[String], stale: Boolean): EntityEmbedding =
    EntityEmbedding(
      vector(skills),
      EmbeddingMeta(Model, SourceHash.sha256(if (stale) "genuinely outdated source" else text), ServiceFixtures.now)
    )
  val jobs: List[Job] = SearchEvaluationFixtures.jobs.map { entity =>
    val job = ServiceFixtures.openJob.copy(
      id = JobId(UUID.fromString(entity.id)),
      recruiterId = recruiter.id,
      title = entity.intendedRole,
      description = entity.summary,
      requirements = entity.skills.toList.sorted,
      skills = entity.skills,
      status = if (entity.active && !entity.deleted) JobStatus.Open else JobStatus.Closed
    )
    job.copy(embedding = Some(embedding(SearchableText.job(job), job.skills, entity.stale)))
  }
  val candidates: List[User] = SearchEvaluationFixtures.candidates.map { entity =>
    val profile = CandidateProfile(
      entity.skills,
      Some(entity.summary),
      None,
      Option.when(entity.privateAttributesPresent)(CandidateResidence("Canada", Some("Toronto"))),
      Option.when(entity.privateAttributesPresent)(CandidateAvailabilityStatus.AVAILABLE_NOW),
      entity.optIn.contains(true)
    )
    ServiceFixtures.candidate.copy(
      id = UserId(UUID.fromString(entity.id)),
      email = None,
      name = "Fabricated candidate",
      profile = Some(UserProfile.Candidate(profile)),
      embedding = Some(embedding(SearchableText.candidate(profile), entity.skills, entity.stale))
    )
  }
  def jobDocument(job: Job): Document = MongoHiringCodecs.job(job)
  def candidateDocument(user: User): Document = {
    val document = MongoHiringCodecs.user(user)
    val entity = SearchEvaluationFixtures.candidates.find(_.id == user.id.value.toString)
    entity.foreach { value =>
      if (!value.active) {
        val _ = document.put(MongoFields.AccountStatus, "Inactive")
      } // defensive unknown-status fixture
      if (value.deleted) { val _ = document.put(MongoFields.AccountStatus, AccountStatus.Deleted.toString) }
      if (value.optIn.isEmpty) {
        val _ = document.get(MongoFields.Profile, classOf[Document]).remove("recruiterSearchOptIn")
      }
    }
    document
  }
  def jobFilter(query: SearchEvaluationFixtureQuery): JobSearchFilter =
    JobSearchFilter(
      None,
      query.filterGroup match {
        case SearchEvaluationFilterGroup.Empty     => Set("nonexistent-fixture-skill")
        case SearchEvaluationFilterGroup.Selective => Set("scala", "cats")
        case SearchEvaluationFilterGroup.Broad     => Set.empty
      },
      None
    )
  def candidateFilter(query: SearchEvaluationFixtureQuery): CandidateMatchFilters = query.filterGroup match {
    case SearchEvaluationFilterGroup.Empty => CandidateMatchFilters(List("nonexistent-fixture-skill"), None, None, None)
    case SearchEvaluationFilterGroup.Selective =>
      CandidateMatchFilters(List("scala"), Some("canada"), Some("toronto"), Some("AVAILABLE_NOW"))
    case SearchEvaluationFilterGroup.Broad => CandidateMatchFilters.empty
  }
  def querySkills(query: SearchEvaluationFixtureQuery): Set[String] =
    if (query.queryId.endsWith("-2")) Set("scala", "spark") else Set("scala", "cats")
  def text(query: SearchEvaluationFixtureQuery): String = querySkills(query).toList.sorted.mkString(" ")
}
