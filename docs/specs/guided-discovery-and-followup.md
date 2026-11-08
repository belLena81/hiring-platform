# Guided discovery and follow-up assistance

## Identity and scope

- Roadmap: Phase 11A in [development milestones](../development-milestones.md), after the current analytics refactoring and before Phase 12. The LLM-assisted part is the post-MVP Phase 16 at the end of this document.
- Status: **draft, not started.** Sequenced after the MVP (local) claim (decided 2026-10-29). The pre-MVP contract policy still applies to API and analytics shape changes until the MVP is complete and the first cloud release ships (owner decision 2026-11-08); see D4. Created 2026-10-29 from the codebase read below. No criterion has run.
- Coordinator: Product Manager. Architect owns the GraphQL contracts (GDF-01..06). Scala Developer owns services and resolvers. Data Engineer owns the similar-job query, the saved-search collection, indexes and migration. Independent Code Reviewer and QA review every slice; Security Engineer reviews slices 1 (authorization of similar jobs), 2 (message drafts) and 3 (saved-search storage, erasure).
- User outcome: a candidate finds a fitting job with fewer dead ends and sees why a job fits; a recruiter sees why a candidate fits and starts follow-up messages from a draft instead of a blank field. None of it calls a language model.
- Actors: Candidate, Recruiter (owner), singleton Admin.
- Non-goals: any LLM or text generation (see Phase 16), resume parsing, sending messages automatically, a real email or S3 provider, geocoding, ranking changes or personalization (HDQ-07/08 stay deferred), automatic hiring decisions, frontend assets, new roles or tenants.
- Dependencies: the Phase 8 authoritative eligibility recheck and bounded short results; Phase 9 evaluation (this spec changes no ranking); Phase 10 filters, facets and `nearbyJobs`; the existing notification port and fake provider; [AI search resilience](ai-search-resilience.md) for the `SearchUnavailable` union and cost budget.

## Source context and decisions

Source facts below were read on 2026-10-29 at branch `refactor`, HEAD `321c2eb`. They describe the code, not behavior of this spec.

| Boundary | Fact |
|---|---|
| AI today | Embeddings and retrieval only. `semanticJobSearch`, `recommendedJobs`, `candidateMatches`, `jobDiscoveryFacets`, `nearbyJobs`, `embeddingCoverage` in `src/test/resources/graphql/hiring.graphql`. No LLM client exists in `src/main`. |
| Explanations | Ranked hits carry `matchedSkills` (`SkillMatching.matched`, `service/search/SearchContracts.scala:261`). There is no "missing skills" value. |
| Filters | `JobFilter` has `city`, `skills`, `createdAfter` only (`hiring.graphql:805`). |
| Zero results | Not handled. The behavior of a search that returns no hits, and whether such a session is recorded, is **unverified** (slice 0 checks it). |
| Notifications | A port exists only for interviews (`InterviewNotificationProvider`, `service/port/InterviewProviders.scala:162`) and a job post-commit hook (`JobService.notifyAfterCommit`). Application status changes carry `feedback`/`reason` in history, but no general recruiter-to-candidate message port exists. |
| Search feedback | `recordJobView` and `recordSearchResultClick` mutations exist and reference a stored search session. |
| Cost control | AI roots are priced `100 + first × (1 + child)`, capped per request and per caller budget; unavailable AI returns `SearchUnavailable`; keyword fallback exists for the two search roots. |

Decisions (proposed; revisable by the user):

- D1 Every feature is deterministic or embedding-only. Added value must not depend on a paid text-generation call.
- D2 Nothing is silently applied. Recovery states which filter was relaxed; query understanding returns suggestions the caller applies.
- D3 Drafts are text returned to the user. The existing mutations with their required `feedback`/`reason` remain the only way a status changes.
- D4 The pre-MVP contract policy still applies to API and analytics shape changes until the MVP is complete and the first cloud release ships (owner decision 2026-11-08): GraphQL changes are implemented directly, with no additive-with-deprecation or compatibility code. The saved-search collection is an operational Mongo change and uses the versioned migration ledger. No versioned endpoints. The owner decides whether this D4 wording changes.
- D5 Authorization and eligibility run before any embedding or vector call. A forbidden request costs nothing and makes no provider call.
- Open questions: Q1 relaxation order and limits (default below). Q2 draft language and tone (default: English, neutral). Q3 saved-search caps and cadence (default: at most 5 per candidate, a bounded scheduled run). Each blocks only its own slice.

## Behavior and contracts

### Slice 1 — Dead-end recovery, reasons, similar jobs

**GDF-01 Zero-result recovery.** When a candidate job search with filters returns no eligible job, the service re-runs it with one filter relaxed at a time and returns the best non-empty page with `relaxedFilters` (which filter, from what, to what). Default order: drop `createdAfter`, then turn `skills` from all-of to any-of. City is not relaxed by text. Distance relaxation applies only when the caller supplied coordinates, because geocoding is out of scope. At most two relaxation attempts per request; each attempt is priced like one search. No relaxation when the original page is non-empty. Unfiltered empty results stay empty.

**GDF-02 Visible reasons.** Hits gain `missingSkills` next to `matchedSkills`: for a candidate viewing jobs, the job's required skills not in the caller's own profile; for a recruiter viewing candidates of an owned job, the job's required skills not in the candidate's skills. Both derive from data the caller can already see, so no new private candidate data is exposed. The function stays pure and shares the existing normalization (case, spaces).

**GDF-03 Similar jobs.** `similarJobs(jobId, first)` returns open, visible jobs nearest to the given job's stored embedding, excluding the job itself, with the standard eligibility recheck. It makes no embedding provider call. A missing or stale job embedding returns `SearchUnavailable`. It is an expensive root with the same price formula and budget. The job must itself be visible to the caller; otherwise the result is as for an unknown job, so existence is not disclosed.

### Slice 2 — Follow-up message drafts

**GDF-04 Message drafts.** A query returns a draft for a given application and action:

- Recruiter or Admin owner: Accept, Interview, Reject.
- Candidate (own application): Decline reason, interview reschedule request.

Each draft has `body` (plain text), `basis` (the facts it used: job title, matched/missing skills, interview slot) and `editable: true`. It is built from templates plus the GDF-02 facts. Rules: it never includes a score, rank or similarity; it never includes private residence, availability or consent; a rejection draft lists skills only as a suggestion for the recruiter to confirm. A draft changes nothing and sends nothing. The recruiter submits the final text through the existing action mutation, which keeps its required `feedback`/`reason` validation. Draft access requires the same ownership as that mutation, enforced in the service.

### Slice 3 — Query understanding and alerts

**GDF-05 Query understanding.** `semanticJobSearch` returns `suggestedFilters` derived from the query text. Skills come from the canonical skills of the eligible open jobs (the same population as `jobDiscoveryFacets`); a city is recognized only when it exists in that set. Matching is deterministic and case-insensitive. Suggestions are never applied automatically (D2). Empty or unrecognized text yields none.

**GDF-06 Saved searches and alerts.** A candidate can save a query and filter (at most the configured cap). A bounded scheduled worker finds jobs created after the last run that match, through the same eligibility rules, and hands one notification per (saved search, job) to the notification port with a deduplication key. The default provider stays the fake/local one. The saved search is deleted with the account, is never copied to events or analytics, and its query text is not logged. A new Mongo collection needs a versioned migration with recovery and verification (Flyway is not used for Mongo).

### Cross-cutting

**GDF-07 Baseline measures.** Before slice 1 ships, record on a bounded synthetic corpus: zero-result rate, rank of the first click, share of searches followed by a click, and recruiter time from application to first status change. No target is set until a baseline exists. Local measures are labelled as such and prove no production result.

**GDF-08 Authorization, cost and failure.** Every new root checks role and ownership before any paid or vector call (provider call count is asserted 0 on a forbidden request). Cost budget and the `SearchUnavailable` union apply to GDF-01 and GDF-03. Failure of an optional step (relaxation, suggestions) never blanks the base result.

## Domain language and examples

Terms added to [domain language](../domain-language.md) with the change: *relaxed filter*, *fit reason* (matched and missing skills), *message draft*, *saved search*.

| Rule | Tag | Scenario |
|---|---|---|
| Relaxation is explicit | GDF-01.1 | Given Ana searches skills Scala and Kafka in a window that returns nothing, when the service relaxes `createdAfter`, then the page returns jobs and `relaxedFilters` names `createdAfter` |
| Nothing relaxes a non-empty page | GDF-01.2 | Given 3 matching jobs, when Ana searches, then `relaxedFilters` is empty |
| Bounded attempts | GDF-01.3 | Given a query that is still empty after both relaxations, when it runs, then the page is empty, at most two extra searches ran and the cost reflects them |
| Missing skills from visible data only | GDF-02.1 | Given a job requires Scala and Kafka and Ana's profile lists Scala, then `matchedSkills=[Scala]` and `missingSkills=[Kafka]` |
| Recruiter sees no new private data | GDF-02.2 | Given a recruiter lists matches for an owned job, then the new field uses only the job's skills and the candidate's skills already returned |
| Similar jobs exclude self and closed | GDF-03.1 | Given job J is open and a neighbour is closed, then the result excludes J and the closed job |
| Hidden source job | GDF-03.2 | Given J is closed, when a candidate asks for jobs similar to J, then the result is the same as for an unknown ID |
| Draft needs ownership | GDF-04.1 | Given a recruiter who does not own the job, when requesting a Reject draft, then the result is forbidden and no draft is built |
| Draft is not a decision | GDF-04.2 | Given a Reject draft was returned, when no mutation is called, then the application status and history are unchanged |
| Draft leaks no private data | GDF-04.3 | Given a candidate has residence and consent set, then neither appears in `body` or `basis` |
| Suggestions not applied | GDF-05.1 | Given the query "senior scala berlin" and Berlin jobs with skill Scala exist, then `suggestedFilters` lists skill Scala and city Berlin and the result set equals the unfiltered search |
| Alert once per job | GDF-06.1 | Given a saved search and one new matching job, when the worker runs twice, then one notification receipt exists |
| Alert cap and erasure | GDF-06.2 | Given the cap is reached, when saving another, then it is rejected; when the account is deleted, then its saved searches are gone |

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| GDF-01 | Empty filtered search returns a labelled, bounded recovery page (tags .1-.3) | Not implemented | Unit, service, GraphQL access | Not run |
| GDF-02 | Hits expose matched and missing skills from caller-visible data (tags .1-.2) | Not implemented | Unit, GraphQL contract | Not run |
| GDF-03 | Similar jobs use the stored embedding, hide closed and unknown jobs (tags .1-.2) | Not implemented | Unit, service, Mongo integration; `.L` on Atlas Local | Not run |
| GDF-04 | Drafts respect ownership, privacy and send nothing (tags .1-.3) | Not implemented | Service, GraphQL forbidden-access | Not run |
| GDF-05 | Suggestions come from the eligible population and are never applied (tag .1) | Not implemented | Unit, service | Not run |
| GDF-06 | Saved searches are capped, deduplicated, erasable (tags .1-.2) | Not implemented | Service, Mongo integration, migration recovery, erasure | Not run |
| GDF-07 | A local baseline of the four measures exists before slice 1 ships | Not implemented | Reproducible script on synthetic data | Not run |
| GDF-08 | Forbidden calls make no provider call; failed optional steps keep the base result | Not implemented | Fake provider call counters, failure injection | Not run |

Live vector behavior (GDF-03) closes only on Atlas Local (`.L`, $0) and later cloud (`.C`); fake and Community tests cannot close it.

## Performance and cost

- Slices 1 and 2 add no provider call. GDF-01 adds at most two extra searches on an empty first page; GDF-03 reads a stored vector.
- GDF-06 costs one vector search per saved search per scheduled run, plus one query embedding at save time. Bound it with the per-candidate cap, a run cadence and a per-run limit. Compare a simpler option first: alerting only on a keyword match, which needs no embedding.
- No latency or recall claim is made. Workload, dataset size, concurrency and percentiles are recorded only by a measured run.

## Implementation handoff

| Slice | Content | Owners and write paths | Gates |
|---|---|---|---|
| 0 | Verify the zero-result and session facts; GDF-07 baseline | Scala Developer: `src/test`, a measurement script under `scripts/` | QA |
| 1 | GDF-01, 02, 03 | Architect: contract; Scala Developer: `service/search`, `api/graphql`, tests; Data Engineer: similar-job query in `repository/mongo` | Code, Security, QA |
| 2 | GDF-04 (uses GDF-02) | Scala Developer: draft policy (pure), service, resolver | Code, Security, QA |
| 3 | GDF-05, 06 | Data Engineer: collection, migration, indexes; Scala Developer: worker, service | Code, Security, QA |

SDL, `docs/api.md`, operation fixtures and `docs/domain-language.md` change with the slice that adds the field. Risks: Atlas is needed to verify GDF-03 live; the draft wording is a product choice (Q2); GDF-06 adds a new stored data category to retention and erasure.

## Post-MVP — Phase 16: LLM-assisted drafting and triage

Not authorized and not scheduled. It needs its own scoped task, provider decision and monthly budget.

- **Suggested feature (4): optional draft polish.** A recruiter or candidate presses a button to make a GDF-04 draft warmer or shorter. The deterministic draft remains the default and the fallback.
- **Application triage (advisory).** Orders the applications to one job by stored-vector similarity. It never hides, rejects or accepts. Needs a Security review of using applicant profiles for the recruiter's ordering, and HDQ-08-style evidence before adoption.
- Preconditions: MVP complete; a named provider, price and monthly ceiling; minimized prompt contents (no resume, no private residence or consent, no unnecessary personal data); hiring content treated as untrusted text so it cannot instruct the model or any tool; the existing circuit-breaker and cost-budget pattern; a fixed evaluation set for draft quality; opt-in per user. Regulation of automated recruitment tools (for example the EU AI Act) needs a separate legal review; this document is not legal advice.
- Completion: independent Code, Security and QA verdicts; outage drill showing the deterministic draft is still returned.

## Checkpoint and review

- Completed criteria and changed files: none; this document and the roadmap entries only.
- Latest commands/results: none run. Source facts above were read, not executed.
- Blockers and next action: confirm D1–D5 and Q1–Q3; then run slice 0.
- Code Reviewer verdict: pending
- Security Engineer verdict: pending
- Final independent QA verdict: pending
