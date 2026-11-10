package com.example.graphQL.cats.service.search

import cats.data.NonEmptyChain
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.{RankingBasis, RankingHit, RankingInputError}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.search.SkillEvidenceRankedComposition.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.{ActorContext, RepositoryError}
import com.example.graphQL.cats.shared.crypto.SourceHash
import munit.CatsEffectSuite
import java.util.UUID

/** Service-level evidence for the skill-evidence ranker (HDQ-07.8, 07.9). The ranker is composed in test code around
  * the real `SemanticSearchService` output; nothing in production calls it.
  */
final class SkillEvidenceRankedDiscoveryAccessSpec extends CatsEffectSuite {
  private val model = "voyage-4-lite"
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000099")
  private val first = PageSize.fromInt(7).toOption.getOrElse(fail("page size"))
  private val candidateActor = ActorContext(candidateId, UserRole.Candidate)
  private val recruiterActor = ActorContext(recruiterId, UserRole.Recruiter)

  private def jobIdOf(n: Int): JobId = JobId(UUID.fromString(f"00000000-0000-0000-0003-${n}%012d"))
  private def userIdOf(n: Int): UserId = UserId(UUID.fromString(f"00000000-0000-0000-0001-${n}%012d"))
  private def embeddingOf(source: String): EntityEmbedding =
    EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta(model, SourceHash.sha256(source), now))
  private def jobOf(n: Int, skills: Set[String], state: JobState = JobState.Open, owner: UserId = recruiterId): Job = {
    val bare = openJob.copy(
      id = jobIdOf(n),
      recruiterId = owner,
      title = s"Job $n",
      requirements = skills.toList.sorted,
      skills = skills,
      state = state
    )
    bare.copy(embedding = Some(embeddingOf(SearchableText.job(bare))))
  }
  private def jobHit(job: Job, meta: Option[EmbeddingMeta] = None): RankedJob =
    RankedJob(job, 0.9, SearchMode.VECTOR, meta.orElse(job.embedding.map(_.meta)).get, searchId)
  private def profileOf(
      skills: Set[String],
      residence: Option[CandidateResidence] = None,
      availability: Option[CandidateAvailabilityStatus] = None,
      optIn: Boolean = false
  ): CandidateProfile = CandidateProfile(skills, Some("Backend engineer"), None, residence, availability, optIn)
  private def candidateOf(
      id: UserId,
      name: String,
      profile: CandidateProfile,
      status: AccountStatus = AccountStatus.Active
  ): User =
    candidate.copy(
      id = id,
      email = Some(email(s"${id.value}@example.com")),
      name = name,
      profile = Some(UserProfile.Candidate(profile)),
      embedding = Some(embeddingOf(SearchableText.candidate(profile))),
      state = accountStateOf(status)
    )
  private def candidateHit(user: User, meta: Option[EmbeddingMeta] = None): RankedCandidate = {
    val profile = user.candidateProfile.getOrElse(fail("candidate fixture needs a profile"))
    RankedCandidate(
      CandidateSearchHit(user.id, user.name, profile.skills, profile.experienceSummary),
      0.9,
      SearchMode.VECTOR,
      meta.orElse(user.embedding.map(_.meta)).get,
      searchId
    )
  }

  private final case class World(
      users: List[User],
      jobs: List[Job],
      jobHits: List[RankedJob],
      candidates: List[RankedCandidate]
  )

  private def serviceOf(world: World): IO[SemanticSearchService[IO]] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](world.users.map(u => u.id -> u).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](world.jobs.map(j => j.id -> j).toMap)
    } yield {
      val users = new InMemoryUsers(usersRef)
      val jobs = new InMemoryJobs(jobsRef)
      SemanticSearchService(
        users,
        jobs,
        new EmbeddingService[IO] {
          def embed(input: EmbeddingInput) = IO.pure(Left(EmbeddingError.ProviderUnavailable))
        },
        new SemanticSearchRepository[IO] {
          private def persistedActor(scope: HiringReadScope): RepositoryF[IO, Option[User]] =
            users.find(scope.userId).map(_.filter(u => u.role == scope.role && u.accountStatus == AccountStatus.Active))
          def authorizedJobEligibility(
              scope: HiringReadScope,
              ids: List[JobId],
              expected: Option[CandidateSearchEligibility]
          ): RepositoryF[IO, List[JobSearchEligibility]] =
            persistedActor(scope).flatMap {
              case Some(user)
                  if user.role == UserRole.Candidate &&
                    expected.forall(_ == CandidateSearchEligibility.fromUser(user)) =>
                ids.traverse(jobs.find).map(_.flatten.map(JobSearchEligibility.fromJob))
              case _ => RepositoryF.at[IO].pure(Nil)
            }
          def authorizedCandidateEligibility(
              scope: HiringReadScope,
              expected: JobSearchEligibility,
              ids: List[UserId]
          ): RepositoryF[IO, List[CandidateSearchEligibility]] =
            (persistedActor(scope), jobs.find(expected.job.id))
              .mapN { (actor, job) =>
                actor.exists(u =>
                  u.role == UserRole.Admin || u.role == UserRole.Recruiter && job.exists(_.recruiterId == u.id)
                ) &&
                job.exists(j => j.status == JobStatus.Open && JobSearchEligibility.fromJob(j) == expected)
              }
              .flatMap { allowed =>
                if (allowed) ids.traverse(users.find).map(_.flatten.map(CandidateSearchEligibility.fromUser))
                else RepositoryF.at[IO].pure(Nil)
              }
          def searchJobs(query: VectorSearchQuery) = RepositoryF.at[IO].pure(world.jobHits.map(_.retrieval))
          def recommendedJobs(query: VectorSearchQuery) = RepositoryF.at[IO].pure(world.jobHits.map(_.retrieval))
          def candidateMatches(query: VectorSearchQuery) =
            RepositoryF.at[IO].pure(world.candidates.map(_.retrieval))
          def lexicalJobs(query: LexicalSearchQuery) = RepositoryF.at[IO].raise(RepositoryError.Unavailable)
          def lexicalCandidates(query: LexicalSearchQuery) = RepositoryF.at[IO].raise(RepositoryError.Unavailable)
        },
        embeddingModel = model
      )
    }

  private val callerSkills = Set("Scala", "cats")
  private val caller = candidateOf(candidateId, "Caller", profileOf(callerSkills))
  private val ownedJob = jobOf(50, Set("scala", "cats", "kafka"))
  private val otherRecruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000088"))
  private val otherRecruiter = recruiter.copy(id = otherRecruiterId, email = Some(email("other@example.com")))
  private def baseUsers: List[User] = List(caller, recruiter, otherRecruiter, admin)

  private def ids(values: List[RankedJob]): List[JobId] = values.map(_.job.id)

  test("HDQ-07.9 closed, deleted and stale jobs never reach the ranker or its output on recommendations") {
    val open1 = jobOf(1, Set("java"))
    val closed = jobOf(2, Set("scala", "cats"), JobState.Closed(now))
    val deletedFromStore = jobOf(3, Set("scala", "cats"))
    val staleSource = jobOf(4, Set("scala", "cats"))
    val open5 = jobOf(5, Set("scala", "cats"))
    val open6 = jobOf(6, Set("scala"))
    val hits = List(open1, closed, deletedFromStore, staleSource, open5, open6).map(jobHit(_)) // retrieval order
    // The stored stale job has since been re-embedded: the retrieved vector no longer matches its source.
    val storedStale = staleSource.copy(embedding = Some(embeddingOf("changed source")))
    val world = World(baseUsers, List(open1, closed, storedStale, open5, open6), hits, Nil)
    for {
      service <- serviceOf(world)
      plain <- service.recommendedJobs(candidateActor, first, searchId).value
      ranker <- RecordingEvidenceRanker.create()
      ranked <- recommendedJobs(service, ranker, candidateActor, callerSkills, first, searchId).value
      seen <- ranker.invocations
    } yield {
      val eligible = List(open1.id, open5.id, open6.id)
      assertEquals(plain.map(ids), Right(eligible))
      assertEquals(seen.map(_.map(_.id)), Vector(eligible.map(_.value.toString)))
      assertEquals(seen.flatten.map(_.retrievalRank), Vector(1, 2, 3))
      // open5 covers 2 of 2 of its skills, open6 1 of 1, open1 none; a 2/2 and a 1/1 tie on coverage.
      assertEquals(ranked.map(_.items.map(_.job.id)), Right(List(open5.id, open6.id, open1.id)))
      assertEquals(ranked.map(_.basis), Right(Some(RankingBasis.SkillEvidence)))
      val excluded = Set(closed.id, deletedFromStore.id, staleSource.id).map(_.value.toString)
      assert(ranked.toOption.get.items.map(_.job.id.value.toString).forall(!excluded(_)))
    }
  }

  test("HDQ-07.9 inactive, stale and private-filter failing candidates never reach the ranker on candidate matching") {
    val cyprus = Some(CandidateResidence("Cyprus", Some("Nicosia")))
    val optedIn = candidateOf(userIdOf(1), "Opted in", profileOf(Set("scala"), cyprus, None, optIn = true))
    val wrongCountry = candidateOf(
      userIdOf(2),
      "Wrong country",
      profileOf(Set("scala", "cats", "kafka"), Some(CandidateResidence("Canada", None)), None, optIn = true)
    )
    val deleted = candidateOf(userIdOf(3), "Deleted", profileOf(Set("scala", "cats")), AccountStatus.Deleted)
    val staleProfile = candidateOf(userIdOf(4), "Stale", profileOf(Set("scala", "cats", "kafka")))
    // Lives in Canada yet still passes the Cyprus filter: an opted-out candidate's private attributes are not used for
    // filtering, whereas the opted-in Canada candidate above is excluded.
    val optedOut = candidateOf(
      userIdOf(5),
      "Opted out",
      profileOf(Set("scala", "cats"), Some(CandidateResidence("Canada", None)), None, optIn = false)
    )
    val users = List(optedIn, wrongCountry, deleted, staleProfile, optedOut)
    val storedStale = staleProfile.copy(embedding = Some(embeddingOf("changed profile source")))
    val world = World(
      baseUsers ++ users.map(u => if (u.id == staleProfile.id) storedStale else u),
      List(ownedJob),
      Nil,
      users.map(candidateHit(_))
    )
    val filters = CandidateMatchFilters(Nil, Some("Cyprus"), None, None)
    for {
      service <- serviceOf(world)
      plain <- service.candidateMatches(recruiterActor, ownedJob.id, None, filters, first, searchId).value
      ranker <- RecordingEvidenceRanker.create()
      ranked <- candidateMatches(
        service,
        ranker,
        recruiterActor,
        ownedJob.id,
        ownedJob.skills,
        first,
        searchId,
        filters
      ).value
      seen <- ranker.invocations
    } yield {
      val reached = plain.map(_.hits.map(_.candidate.id)).toOption.get
      // Premises, read from the real service: deletion, a stale source and a failing private filter drop a hit.
      assertEquals(reached, List(optedIn.id, optedOut.id))
      assertEquals(seen.map(_.map(_.id)), Vector(reached.map(_.value.toString)))
      assertEquals(ranked.map(_.items.map(_.candidate.id).toSet), Right(reached.toSet))
    }
  }

  test("HDQ-07.9 a forbidden or ineligible actor gets today's typed error and zero ranker invocations") {
    val closedOwned = ownedJob.copy(state = JobState.Closed(now))
    val unowned = ownedJob.copy(recruiterId = otherRecruiterId)
    val deletedRecruiter = recruiter.copy(state = accountStateOf(AccountStatus.Deleted))
    val deletedCaller = caller.copy(state = accountStateOf(AccountStatus.Deleted))
    val someCandidate = candidateOf(userIdOf(1), "Visible", profileOf(Set("scala")))
    def world(users: List[User], job: Job) =
      World(users, List(job), List(jobHit(jobOf(1, Set("scala")))), List(candidateHit(someCandidate)))
    val matching: List[(String, World, ActorContext)] = List(
      ("a candidate calling candidateMatches", world(baseUsers :+ someCandidate, ownedJob), candidateActor),
      ("a non-owner recruiter", world(baseUsers :+ someCandidate, unowned), recruiterActor),
      ("a closed owned job", world(baseUsers :+ someCandidate, closedOwned), recruiterActor),
      (
        "a deleted recruiter",
        world(baseUsers.filterNot(_.id == recruiterId) :+ deletedRecruiter :+ someCandidate, ownedJob),
        recruiterActor
      )
    )
    val matchingChecks = matching.traverse_ { case (label, w, actor) =>
      for {
        service <- serviceOf(w)
        plain <- service.candidateMatches(actor, ownedJob.id, None, CandidateMatchFilters.empty, first, searchId).value
        ranker <- RecordingEvidenceRanker.create()
        ranked <- candidateMatches(service, ranker, actor, ownedJob.id, ownedJob.skills, first, searchId).value
        seen <- ranker.invocations
      } yield {
        assert(plain.isLeft, clue(label))
        assertEquals(ranked.map(_.items.size), plain.map(_ => 0), clue(label))
        assertEquals(ranked.left.toOption, plain.left.toOption, clue(label))
        assertEquals(seen, Vector.empty, clue(label))
      }
    }
    val recommendationChecks = List(
      ("a recruiter calling recommendedJobs", recruiterActor, world(baseUsers, ownedJob)),
      (
        "a deleted candidate",
        candidateActor,
        world(baseUsers.filterNot(_.id == candidateId) :+ deletedCaller, ownedJob)
      )
    ).traverse_ { case (label, actor, w) =>
      for {
        service <- serviceOf(w)
        plain <- service.recommendedJobs(actor, first, searchId).value
        ranker <- RecordingEvidenceRanker.create()
        ranked <- recommendedJobs(service, ranker, actor, callerSkills, first, searchId).value
        seen <- ranker.invocations
      } yield {
        assert(plain.isLeft, clue(label))
        assertEquals(ranked.left.toOption, plain.left.toOption, clue(label))
        assertEquals(seen, Vector.empty, clue(label))
      }
    }
    matchingChecks *> recommendationChecks
  }

  test("HDQ-07.9 admin matching is unchanged: the ranker only reorders what the service already returns") {
    val strong = candidateOf(userIdOf(1), "Strong", profileOf(Set("scala", "cats", "kafka")))
    val weak = candidateOf(userIdOf(2), "Weak", profileOf(Set("java")))
    val world = World(baseUsers ++ List(strong, weak), List(ownedJob), Nil, List(weak, strong).map(candidateHit(_)))
    val adminActor = ActorContext(adminId, UserRole.Admin)
    for {
      service <- serviceOf(world)
      plain <- service
        .candidateMatches(adminActor, ownedJob.id, None, CandidateMatchFilters.empty, first, searchId)
        .value
      ranker <- RecordingEvidenceRanker.create()
      ranked <- candidateMatches(service, ranker, adminActor, ownedJob.id, ownedJob.skills, first, searchId).value
      seen <- ranker.invocations
    } yield {
      assertEquals(plain.map(_.hits.map(_.candidate.id)), Right(List(weak.id, strong.id)))
      assertEquals(seen.size, 1)
      assertEquals(ranked.map(_.items.map(_.candidate.id)), Right(List(strong.id, weak.id)))
      assertEquals(ranked.map(_.items.toSet), plain.map(_.hits.toSet))
    }
  }

  test("HDQ-07.9 Q8 a ranker error keeps the retrieval order of the service result") {
    val a = candidateOf(userIdOf(1), "A", profileOf(Set("java")))
    val b = candidateOf(userIdOf(2), "B", profileOf(Set("scala", "cats", "kafka")))
    val world = World(baseUsers ++ List(a, b), List(ownedJob), Nil, List(a, b).map(candidateHit(_)))
    val refusing: EvidenceRanker =
      (_, maxHits) => IO.pure(Left(NonEmptyChain.one(RankingInputError.TooManyHits(maxHits))))
    for {
      service <- serviceOf(world)
      ranker <- RecordingEvidenceRanker.create(refusing)
      ranked <- candidateMatches(service, ranker, recruiterActor, ownedJob.id, ownedJob.skills, first, searchId).value
    } yield assertEquals(ranked.map(s => (s.basis, s.items.map(_.candidate.id))), Right((None, List(a.id, b.id))))
  }

  /** What the ranker received, without the identifiers that differ between two otherwise identical worlds. */
  private def shape(
      calls: Vector[List[RankingHit[String]]]
  ): Vector[List[(Int, Option[com.example.graphQL.cats.domain.search.SkillEvidence])]] =
    calls.map(_.map(h => (h.retrievalRank, h.evidence)))

  test(
    "HDQ-07.8 changing only a matched candidate's consent, residence, availability and name leaves ranker input and output unchanged"
  ) {
    val skills = List(Set("scala", "cats", "kafka"), Set("scala"), Set("java"))
    def candidates(private_ : Boolean): List[User] = skills.zipWithIndex.map { case (s, i) =>
      val profile =
        if (private_)
          profileOf(
            s,
            Some(CandidateResidence("Cyprus", Some("Nicosia"))),
            Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
            optIn = true
          )
        else profileOf(s)
      candidateOf(userIdOf(i + 1), if (private_) s"Name ${i + 1}" else s"Other ${i + 1}", profile)
    }
    def run(private_ : Boolean) = {
      val users = candidates(private_)
      for {
        service <- serviceOf(World(baseUsers ++ users, List(ownedJob), Nil, users.map(candidateHit(_))))
        ranker <- RecordingEvidenceRanker.create()
        ranked <- candidateMatches(service, ranker, recruiterActor, ownedJob.id, ownedJob.skills, first, searchId).value
        seen <- ranker.invocations
      } yield (shape(seen), ranked.toOption.get.items.map(_.candidate.id), ranked.toOption.get.basis)
    }
    for {
      a <- run(private_ = true)
      b <- run(private_ = false)
    } yield {
      assertEquals(a._1, b._1)
      assertEquals(a._3, b._3)
      // Same positions in the output, whatever the private attributes: the identifiers differ only by index.
      assertEquals(
        a._2.map(id => candidates(true).indexWhere(_.id == id)),
        b._2.map(id => candidates(false).indexWhere(_.id == id))
      )
      assertEquals(a._2.map(id => candidates(true).indexWhere(_.id == id)), List(0, 1, 2))
    }
  }

  test(
    "HDQ-07.8 changing only the caller's consent, residence, availability and name leaves recommendation ranking unchanged"
  ) {
    val jobs = List(jobOf(1, Set("java")), jobOf(2, Set("scala", "cats")), jobOf(3, Set("scala", "kafka")))
    def run(profile: CandidateProfile, name: String) = {
      val actor = candidateOf(candidateId, name, profile)
      for {
        service <- serviceOf(World(List(actor, recruiter, admin), jobs, jobs.map(jobHit(_)), Nil))
        ranker <- RecordingEvidenceRanker.create()
        ranked <- recommendedJobs(service, ranker, candidateActor, profile.skills, first, searchId).value
        seen <- ranker.invocations
      } yield (shape(seen), ranked.map(_.items.map(_.job.id)))
    }
    for {
      withPrivate <- run(
        profileOf(
          callerSkills,
          Some(CandidateResidence("Cyprus", Some("Nicosia"))),
          Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
          optIn = true
        ),
        "Alice"
      )
      withoutPrivate <- run(profileOf(callerSkills), "Zed")
    } yield {
      assertEquals(withPrivate, withoutPrivate)
      assertEquals(withPrivate._2, Right(List(jobs(1).id, jobs(2).id, jobs(0).id)))
    }
  }
}
