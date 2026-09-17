# Vector And Hybrid Search

Status: in progress

## Identity and scope

- Task / business capability: Vector and hybrid search for UC02 Semantic / Hybrid Job Search, UC06 Recommended Jobs, and UC10 Candidate Matching.
- Coordinator / implementation owners: Product Manager coordinates. Software Architect owns API and boundary review. Scala Developer owns service, GraphQL, Voyage adapter, and focused tests. Data Engineer owns MongoDB embedding fields, Atlas Vector Search query/index setup, and integration evidence. Security Engineer reviews RBAC, PII, secrets, provider failure handling, and resource exhaustion.
- User outcome: candidates can find and discover relevant open jobs through semantic retrieval, and recruiters can find candidate matches for owned jobs, with observable model/version metadata and authorization-safe filtering.
- Authorized scope: dev-only real Voyage AI embeddings and MongoDB Atlas Vector Search using the current Scala 3, Cats Effect, Sangria, http4s, and MongoDB stack. Default model is `voyage-4-lite` with 1024-dimensional float embeddings.
- Non-goals: frontend/UI, Kafka/Spark/Delta analytics, production-scale backfill, production SLO claims, production deployment, unauthenticated served hiring workflows, login/token issuance, external identity-provider integration, and changing existing structured `jobs` behavior.
- Dependencies: Phase 3 GraphQL/hiring services are the baseline. Served HTTP hiring operations can authenticate configured HS256 bearer JWTs; production identity-provider integration remains separately scoped.

## Source context and decisions

- Verified implementation: current source has `ActorContext`, `JobService`, `ApplicationService`, MongoDB repositories, request-scoped GraphQL batching, keyset pagination, candidate profiles embedded in `User`, and jobs without embedding fields.
- Target requirements: `docs/development-milestones.md` Phase 4 requires `EmbeddingService[F[_]]`, `SemanticSearch[F[_]]`, embeddings for jobs and candidate profiles, `model/version/sourceHash/updatedAt`, asynchronous generation, search modes `FILTER`, `VECTOR`, `HYBRID`, metadata filtering, observable model/version, and integration tests for vector-search behavior.
- Provider decision: use Voyage REST `POST https://api.voyageai.com/v1/embeddings` with `voyage-4-lite`, `input_type` set to `document` for persisted entity embeddings and `query` for search text, `output_dimension = 1024`, and float output.
- Vector decision: use MongoDB `$vectorSearch`; do not use deprecated `knnBeta`. `limit` must not exceed `numCandidates`; vector fields stay on their owning job/user document.
- Cost decision: use dev-only credentials and bounded fixtures. No full production backfill or benchmark may run without a separate scope and budget.

## Behavior and contracts

- GraphQL evolves additively with `semanticJobSearch(query, filter, first)`, `recommendedJobs(first)`, and `candidateMatches(jobId, first)`. Each ranked result exposes the entity, score, search mode, model, version, and search ID. Existing `jobs`, mutations, health, and readiness remain compatible.
- Services own authorization. Candidates can use semantic job search and recommended jobs. Recruiters can request candidate matches only for jobs they own; Admin remains fully authorized where existing service policy allows it. Closed jobs never appear in candidate search or recommendations.
- Normal job/profile writes enqueue embedding regeneration and do not call Voyage synchronously. Missing or stale embeddings are typed search errors, not silent fallbacks that widen visibility. Regeneration skips only when `sourceHash`, model, and version all match the current runtime configuration; job embedding persistence is guarded by the observed job version so stale in-flight work cannot overwrite a newer job update.
- MongoDB document changes are additive: missing legacy `embedding` and `embeddingMeta` decode as `None`. Atlas vector indexes include metadata filter fields for status, location, recruiterId, role, and embedding model/version.
- Secrets stay in ignored local config or environment variables. Logs and GraphQL errors must not expose API keys, resumes, profile summaries, job descriptions, raw provider responses, or raw Atlas errors.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification command or method | Actual outcome |
|---|---|---|---|---|
| VHS-AC01 | Given the schema is exported, when Phase 4 fields are added, then existing GraphQL operations remain compatible and new ranked search fields are present | `HiringGraphQLSchema`; SDL fixture; contract tests | `sbt test` | PASS: SDL fixture and representative existing operations pass |
| VHS-AC02 | Given a Candidate executes semantic job search, when vector results are returned, then only visible Open jobs matching structured filters appear with score, search mode, model, version, and search ID | `HiringGraphQLAccessSpec`; `SemanticSearchServiceSpec` | `sbt test` | PASS for service and injected GraphQL behavior with deterministic fakes; live Atlas vector ranking not exercised |
| VHS-AC03 | Given a Candidate requests recommendations, when the candidate profile embedding is missing or stale, then a typed sanitized error is returned rather than widening results | `SemanticSearchServiceSpec` | `sbt test` | PASS for missing and same-version stale-hash candidate embedding errors |
| VHS-AC04 | Given a Recruiter requests candidate matches, when the job is not owned by that recruiter, then no candidate profile data is exposed | `SemanticSearchServiceSpec` | `sbt test` | PASS for owner recruiter success and non-owner recruiter forbidden path |
| VHS-AC05 | Given job/profile content changes, when the write succeeds, then embedding regeneration is queued asynchronously and mutation latency is independent from Voyage latency | `JobServiceSpec`; `EmbeddingPipelineSpec`; `MongoHiringRepositoriesIntegrationSpec` | `sbt test`; `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` | PASS/PARTIAL: job create/update enqueue after successful writes, enabled runtime wires the embedding pipeline, saturated scheduling is non-blocking and coalesces pending work, stale in-flight job embedding writes are version-guarded, and the pipeline drains queued work; no candidate-profile mutation entrypoint exists yet |
| VHS-AC06 | Given canonical searchable content has not changed, when regeneration runs, then matching `sourceHash`, model, and version prevent an unnecessary Voyage call | `EmbeddingPipelineSpec`; `SemanticSearchServiceSpec`; `MongoHiringCodecsSpec` | `sbt test` | PASS for current hash/model/version skip, model/version-change regeneration, same-version stale-hash rejection, and embedding metadata codec compatibility |
| VHS-AC07 | Given Atlas vector search runs, when `$vectorSearch` is built, then it uses configured index names, 1024 dimensions, metadata filters, bounded `numCandidates`, and `limit <= numCandidates` | `MongoSemanticSearchRepository`; `MongoSemanticSearchResultSpec`; config tests; Mongo setup integration | source inspection; `sbt test`; `sbt 'IntegrationTest / test'` | PARTIAL: runtime wiring, stale-hit filtering, model/version filters, 1024 dimension enforcement, metadata indexes/migration, and bounded candidates are covered locally; live Atlas `$vectorSearch` execution not run |
| VHS-AC08 | Given Voyage or Atlas fails, when search/regeneration is attempted, then typed sanitized errors are returned and domain writes remain intact | `SemanticSearchServiceSpec`; `AppConfigSpec`; `VoyageEmbeddingService` | `sbt test` | PARTIAL: typed provider/vector/input-size errors, key-required config, and sanitized packaged defaults are tested; real Voyage failure response not exercised |
| VHS-AC09 | Given local/dev credentials are configured, when integration tests run, then bounded real Voyage and Atlas behavior is evidenced without production-scale backfill | integration tests and manual config notes | `sbt 'IntegrationTest / test'` | BLOCKED/NOT RUN: no Voyage key or Atlas vector index environment was used in this local run |
| VHS-AC10 | Given implementation is complete, when final gates run, then `sbt test`, integration tests where configured, skill validation, diff check, Code Reviewer, Security Engineer, and QA are recorded | full gate | local commands and reviews | PARTIAL/BLOCKED: local gates pass and review-fix Code Reviewer, Security Engineer, Data Engineer, and QA verdicts pass; live Voyage/Atlas AC09 remains unrun |

## Performance and cost

- SLO targets remain targets until measured: semantic job search p95 < 500 ms; recommended jobs p95 < 300 ms; candidate matching p95 latency to be measured with a documented workload.
- Initial workload is bounded local/dev fixtures sized only to prove authorization, filters, stale embedding behavior, and vector query shape.
- Cost ceiling is dev-only provider/Atlas usage. Production search nodes, large backfills, and broad benchmarks require explicit authorization.

## Implementation handoff

- Ordered slices: contracts and metadata, Voyage config/adapter, semantic search service and authorization, MongoDB vector query/update paths, GraphQL schema/resolvers, asynchronous embedding pipeline, tests/evidence, independent review and QA.
- Allowed write paths: `src/main/scala/com/example/graphQL/cats/domain`, `service`, `transport/graphql`, `config`, `repository/mongo`, `infrastructure/embedding`, tests, docs/specs, `docs/api.md`, `docs/mongodb-design.md`, and relevant fixtures.
- Recovery approach: additive decode keeps legacy documents readable; failed embedding generation leaves prior embeddings untouched and reports stale/missing metadata until regeneration succeeds.

## Checkpoint and review

- Completed criteria and changed files: added additive GraphQL vector/hybrid search contract, search/embedding domain metadata, semantic search service, bounded embedding pipeline, Voyage adapter/config, production runtime wiring, Mongo embedding codec/update/vector query paths, stale-result filtering, metadata indexes, SDL fixture, and focused tests.
- Latest commands/results and their scope: non-blocking embedding scheduler review fix passed `EmbeddingPipelineSpec` and `JobServiceSpec` 13/13 focused unit tests, `MongoHiringRepositoriesIntegrationSpec` 10/10 integration tests, and `sbt test` 178/178 unit tests. Earlier config review-fix focused command passed `AppConfigSpec` 19/19 unit tests and `MainProcessSpec` 14/14 integration tests. Full live Voyage/Atlas verification remains unrun.
- Blockers and next concrete action: live credentialed Voyage + Atlas Vector Search verification remains unrun; production identity-provider integration and token issuance remain separately scoped.
- Code Reviewer verdict and scope: PASS after production runtime wiring, resolved-key Voyage factory, stale-result filtering, config sanitization, and checkpoint fixes; live Voyage/Atlas remains a documented gap.
- Security Engineer verdict and scope: PASS after enabled vector runtime fails closed without a configured Voyage key and defaults keep vector search disabled; live credentials/provider behavior remains a runtime gap.
- Data Engineer verdict and scope: PASS after model/version filtering, source-hash stale-result filtering, shared Mongo repository wiring, and integration evidence.
- Final independent QA verdict: PASS for local review fixes including non-blocking embedding scheduling; BLOCKED on VHS-AC09 only because live Voyage API + Atlas `$vectorSearch` verification was not run.
