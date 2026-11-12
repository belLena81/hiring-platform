# Hiring UI planning summary

Status: discussion only. No spec, code, `ui/` folder or Node toolchain exists or is authorized. Recorded 2026-10-30 from a planning conversation so the work can resume later. Re-check the source, schema and `AGENTS.md` before acting: this summary cannot certify current behavior or grant permission. Library versions and framework behavior named here come from general knowledge and must be verified against current documentation when work starts.

## 1. Resume checklist

1. Re-read `AGENTS.md` (the repository is backend-only until the user authorizes a scoped exception), `docs/use-cases.md`, `docs/api.md` and `src/test/resources/graphql/hiring.graphql`.
2. Confirm the MVP (local) state in `docs/development-milestones.md`. At the time of writing the analytics refactoring and the observability slice (MVP condition 7) were still open, so the UI is post-MVP or a parallel track.
3. Settle the open decisions in section 3.
4. Get explicit authorization for the `ui/` submodule and Node toolchain, then verify ignore rules with `git check-ignore` before installing anything.
5. Run the spikes in section 7 before writing the spec.
6. Write the spec in `docs/specs/` (stable acceptance IDs, source facts, unknowns, criterion-to-test evidence) per `docs/spec-driven-development.md`.

## 2. Decisions made so far

| Topic | Decision | Notes |
|---|---|---|
| Repo shape | Git submodule at `ui/` | The user chose a submodule. The UI repo needs its own agent instructions; the parent `AGENTS.md` needs a short exception note |
| Framework | Next.js, used client-only (pattern A) | Browser calls the Scala API directly. Next.js supplies routing, layouts and build tooling |
| GraphQL client | Apollo Client, in client components only | Chosen partly because job postings ask for it. TanStack Query remains the lower-risk fallback |
| Components | shadcn/ui + Tailwind (Radix primitives) | Components are copied into the repo and owned |
| Auth | Short-lived access token in memory + `HttpOnly` refresh cookie | The backend must add refresh/logout; this is a security-sensitive backend slice |
| Browser access | Separate origins with CORS and credentials | Needs same-site hosting (sibling subdomains; `localhost` ports in development) |
| Observability UI | None custom; Admin menu link to Grafana (new tab) | Grafana, Prometheus, Tempo and Loki already exist in `compose.observability.yaml` |
| Analytics UI | One Admin page over `analyticsReport(from, to)` | Window capped at 30 days |
| Scope | All use cases, delivered in phases | Each phase has its own acceptance criteria and review gates |
| Spike | Yes, before the spec | See section 7 |

## 3. Open decisions

1. **Static export or a Node server for Next.js.** Recommended: `output: 'export'` (no server to secure or deploy). Constraint: no middleware or server redirects, and dynamic ID routes cannot be pre-rendered, so job and application pages need query parameters or a client-side fallback. Role guards are client-side only (UX; the backend enforces access).
2. **Refresh and logout as HTTP endpoints or GraphQL mutations.** Every GraphQL mutation requires an `idempotencyKey`, which fits refresh poorly; a small HTTP endpoint may be cleaner.
3. **Production hosting.** Cookies need the UI and API on sibling subdomains of one registrable domain with TLS. Check against the demo-deployment milestone (one VM, free trial credit) before committing.
4. **Source of truth for the SDL.** Recommended: the backend repo; the UI codegen reads a copy with a script that detects drift.
5. **Where the spike runs.** A throwaway folder (scratchpad) or the real `ui/` submodule.
6. **Phase 1 backend scope** depends on the above and must be specified after they are settled.

## 4. Use cases the UI must cover

Derived from `docs/use-cases.md` and the Query/Mutation lists in `hiring.graphql` on 2026-10-29. Re-verify against the current schema.

| Area | Use cases | Operations |
|---|---|---|
| Account (all roles) | Sign up (Candidate or Recruiter only), log in, view/edit profile, delete account and check status. The Admin account comes from startup configuration, so there is no Admin signup | `signUp`, `login`, `me`, `updateMyProfile`, `deleteMyAccount`, `accountDeletionStatus` |
| Candidate discovery | UC01 filter search, nearby jobs, facets; UC02 semantic/hybrid search; UC03 job detail; UC06 recommendations | `jobs`, `nearbyJobs`, `jobDiscoveryFacets`, `semanticJobSearch`, `job`, `recommendedJobs`, `recordJobView`, `recordSearchResultClick` |
| Candidate applications | UC04 apply; UC05 own applications and history; decline/withdraw with required reason | `submitApplication`, `myApplications`, `applicationHistory`, `declineApplication` |
| Recruiter jobs | UC07 create (Draft), edit, publish, close | `myJobs`, `createJob`, `updateJob`, `publishJob`, `closeJob` |
| Recruiter applications | UC08 list per owned job; UC09 accept, interview, hire, reject (feedback required), decline | `jobApplications`, `acceptApplication`, `moveApplicationToInterview`, `hireApplication`, `rejectApplication`, `declineApplication` |
| Recruiter matching | UC10 candidate matches for an owned job | `candidateMatches` |
| Interviews | Schedule, inspect workflow, cancel, request/propose/accept/decline/withdraw/dismiss reschedule, admin repair | `scheduleInterview`, `interviewWorkflow`, `cancelInterview`, `requestInterviewReschedule`, `proposeInterviewReschedule`, `acceptInterviewReschedule`, `declineInterviewReschedule`, `withdrawInterviewReschedule`, `dismissInterviewRescheduleRequest`, `repairInterviewWorkflow` |
| Admin | UC12/UC13 analytics; users; embedding coverage; everything a recruiter can do | `analyticsReport`, `users`, `embeddingCoverage` |
| No UI | UC11 events and Kafka | Only `recordJobView` and `recordSearchResultClick` feed analytics |

Excluded from the first UI: Phase 11A guided discovery: saved searches and draft messages are not implemented, and `similarJobs` is implemented on the working tree after `e1eb2aa` (reviews pending); the phase is outside the MVP.

Role visibility: build navigation from `me.role`. The backend denies by default: an unowned resource is `NOT_FOUND`, a wrong-role call is `FORBIDDEN`. UI guards are UX only.

- Candidate: Jobs, Recommended, My applications, Interviews, Profile.
- Recruiter: My jobs, Applications, Candidate matches, Interviews, Profile.
- Admin: Analytics, Users, recruiter views, Operations (Grafana link).

## 5. Alternatives with pros and cons

Marked with the current choice.

### 5.1 Application framework

| Option | Pros | Cons |
|---|---|---|
| **Next.js, client-only (chosen)** | Recognized in job postings; file routing, layouts, route groups; static export removes the server; same build tooling as a full Next.js app | Little of its server value is used; static export blocks middleware and pre-rendered dynamic routes; heavier than Vite for a pure SPA; App Router caching/behavior has changed between majors |
| Next.js with BFF | Strong security model (refresh cookie owned by Next.js, no browser CORS to the API); the most job-relevant pattern | Adds a Node service to Compose and observability; Next.js becomes security-critical and must be kept patched (serious advisories in recent years); changes the backend auth slice; extra network hop |
| Next.js with server-rendered pages | Exercises the most Next.js features; better first paint | Token refresh during server rendering is hard (server components cannot set cookies); highest complexity; little benefit behind login |
| Vite + React Router | Simplest, fastest dev loop, static output, fits separate origins | Not Next.js (fewer matching job requirements); routing/data conventions are self-assembled |
| Vite + TanStack Router/Start | Fully typed routes and search params, good for filter and cursor URL state | Newer, smaller ecosystem |
| Remix / React Router framework mode | Clean loader/action model | Server-oriented; weak fit with a separate GraphQL API |

### 5.2 GraphQL client

| Option | Pros | Cons |
|---|---|---|
| **Apollo Client (chosen)** | Normalized cache (one mutation result updates every list); mature devtools; cursor pagination helpers; largest GraphQL community; frequently requested in job postings | Heavy; needs `typePolicies` (key fields, pagination merge) and `possibleTypes` for result unions; a cache that outlives a user session must be cleared on logout; persisted-query hashing must match the backend allowlist; Next.js App Router integration is more involved and has changed packages across versions; more concepts (links, cache policies) |
| TanStack Query + graphql-request | Small; one cache model for all server state; simple invalidation; infinite queries map well to cursors | No normalized cache (refetch or invalidate after mutations); less recognizable for GraphQL-specific postings |
| urql | Lightweight, GraphQL-native, optional normalized cache | Smaller community |
| Relay | Best fragment co-location and scale | Steep learning curve; strict schema conventions (global IDs, connection shape) |

### 5.3 Code generation

| Option | Pros | Cons |
|---|---|---|
| **graphql-codegen, `client` preset (recommended)** | Typed documents, fragment masking, scalar mapping, can emit the persisted-query manifest | Build step; configuration to maintain |
| gql.tada | Types inferred in the editor, no generated file | Less direct persisted-query manifest support |
| Hand-written types | None worth taking | Drift from the schema; contradicts the schema-first rule |

### 5.4 Styling and components

| Option | Pros | Cons |
|---|---|---|
| **shadcn/ui + Tailwind (chosen)** | Minimalistic by default; you own and can customize the code; accessible Radix primitives; design tokens as CSS variables; charts built on Recharts | You maintain copied components; Tailwind class discipline needed; updates are manual |
| Mantine | Rich kit (dates, notifications, charts); fast start | Less visual control; separate styling system |
| MUI | Most complete and widely known | Heavy; Material look hard to make minimal |
| Chakra / Ant Design | Fast to start | Opinionated look; large bundle (Ant); major-version churn (Chakra) |
| Headless only (Radix or React Aria) + CSS Modules | Total control | More hand work and design decisions |
| Panda CSS / vanilla-extract | Type-safe zero-runtime styles | Smaller ecosystems, more setup |

### 5.5 Forms and validation

| Option | Pros | Cons |
|---|---|---|
| **React Hook Form + Zod (recommended)** | Performant; schema reuse; large community | Two concepts to learn; server `ValidationError.field` must be mapped onto fields |
| TanStack Form | Strong type safety and async validation | Newer |
| Formik | Familiar | Legacy; not recommended for new work |
| React 19 form actions only | Fewer dependencies | Weak for complex validation and field arrays |

Valibot is a smaller alternative to Zod; Zod is the more common choice.

### 5.6 Client state

| Option | Pros | Cons |
|---|---|---|
| **React state and context (recommended start)** | No dependency; enough for session and theme | Context re-renders if overused |
| Zustand | Tiny; simple cross-page UI state | Another dependency; avoid duplicating server state in it |
| Jotai | Fine-grained derived state | More concepts |
| Redux Toolkit | Large teams, complex client state | Overkill here |

### 5.7 Routing and URL state

With Next.js, file-based routing replaces the router choice. Filters and cursors belong in the URL; `nuqs` provides typed URL state and supports Next.js. Under static export, dynamic ID routes need query parameters or a client fallback.

### 5.8 Tables, charts, dates

| Area | Options | Notes |
|---|---|---|
| Tables | **TanStack Table** (headless), plain HTML tables | Plain tables suffice for simple lists |
| Charts | **Recharts via shadcn charts**, Visx, Nivo, ECharts | The analytics page needs a funnel, durations and a time series only |
| Dates | date-fns, Day.js, `Intl` | The API uses UTC `Instant`; convert for display only |

### 5.9 Testing

| Layer | Choice | Alternatives |
|---|---|---|
| Unit/component | **Vitest + Testing Library** (query by role and accessible name) | Jest |
| Network mocking | **MSW**, reusing backend operation fixtures where possible | Mocking hooks (not recommended) |
| End to end | **Playwright** | Cypress |
| Accessibility | axe in tests and Playwright | Manual audit |
| Component docs | Storybook (optional) | None |

### 5.10 Lint, format, package manager

| Option | Pros | Cons |
|---|---|---|
| **ESLint flat config + typescript-eslint + react-hooks + jsx-a11y + Prettier (recommended)** | Established; full rule coverage for hooks and accessibility | Two tools; slower |
| Biome | One fast tool | Fewer React/a11y rules |
| Oxlint | Fastest | Newest |
| **pnpm (recommended)** | Fast; strict dependency resolution | Less universal than npm |
| npm / Yarn / Bun | npm is the simplest | Differing strictness and tooling |

### 5.11 Auth on the client

| Option | Pros | Cons |
|---|---|---|
| **In-memory access token + HttpOnly refresh cookie (chosen)** | Token not readable by injected scripts after reload; keeps the existing bearer path and tests | Backend work: hashed rotating refresh tokens in Mongo, reuse detection, revocation, CSRF controls, CORS credentials; same-site hosting needed |
| In-memory token, re-login on expiry | No backend change | Poor UX |
| `localStorage` token | Survives reload | Readable by any injected script; weakest option |
| Cookie-only session | Simplest for the browser | Replaces bearer auth for the UI; CSRF on every mutation |
| BFF owning the cookie | Strongest browser-side posture | Requires a Next.js server (see 5.1) |
| Auth libraries (Auth.js, Clerk) | Fast to start | Built for other server models; add more than they remove here |

## 6. Best-practice patterns to follow

React:

- Function components and hooks only.
- Server state belongs to the data layer (Apollo); do not copy it into `useState` or a store. UI state stays local; URL state (filters, cursor, tab) lives in the URL.
- Effects are a last resort; derive values in render. Fetching in effects is the anti-pattern a data library replaces.
- React 19 and the React Compiler: avoid hand-written `useMemo`/`useCallback`/`memo` unless measured; follow the Rules of React and enable the lint rules.
- Composition over configuration: compound components and `children`/slots instead of many boolean props.
- Stable keys; error boundaries around routes.

TypeScript:

- `strict: true`; consider `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes`.
- No `any` (use `unknown` and narrow); treat `as` and `!` as smells.
- Generate API types from the schema; do not hand-write them.
- Discriminated unions with exhaustive `never` checks for `Application | ValidationError | DomainError`.
- Parse external data (forms, URL parameters, storage) with Zod and infer types.
- Branded ID types mirroring the typed scalars (`JobID`, `UserID`).
- Prefer string-literal unions over enums; `import type`; `verbatimModuleSyntax`; `satisfies` where it avoids widening.

Structure and quality:

- Feature folders named after business capabilities (`features/applications`, `features/jobs`, `features/analytics`) with a small public surface; enforce boundaries with a lint rule.
- Features use only the shared `ui/` layer; ban raw color classes in features to prevent visual drift.
- Features call generated or wrapper hooks (`useApplications`, `useSubmitApplication`), never Apollo directly, so the client can be swapped.
- One typed error layer converts error codes (`FORBIDDEN`, `NOT_FOUND`, `RATE_LIMITED`, `UNAUTHORIZED`) into UI behavior.
- Accessibility from the start: semantic HTML, keyboard-operable dialogs, labelled and error-associated fields, focus visibility, WCAG AA contrast; design loading, empty, error and success states.
- Guard submit buttons against double submission; generate one idempotency key per user intent and reuse it on retry.
- Security: no `dangerouslySetInnerHTML`; validate URLs in `href`; Content-Security-Policy; commit the lockfile, review new dependencies, consider disabling install scripts by default; token never in `localStorage`.
- Local verification: one check command covering type-check, lint and tests (the project has no CI).

## 7. Spikes to run before the spec (planned, not started)

Output: a short findings note; spike code is thrown away or kept by decision. Anything needing a backend change feeds the Phase 1 spec.

1. **Apollo against the real contract.**
   - Persisted queries: the registration and hash format the backend allowlist accepts; whether a build-time manifest avoids `PERSISTED_QUERY_NOT_FOUND`.
   - A union mutation with `possibleTypes`; one idempotency key per intent reused on retry.
   - A cursor connection (`myApplications`) with a type policy that merges pages and resets on filter change.
   - Cache cleared on logout, with a test showing no previous-user data survives.
   - Scalar mapping for `JobID`, `UserID`, `Cursor`, `Instant`, `Long`, `UUID`.
2. **Cross-origin cookie auth.** A minimal stand-in refresh endpoint to test SameSite, credentials, CORS preflight and a single in-flight refresh on 401 through Apollo links. Also check static export behavior for dynamic ID routes and the role-guard redirect flash.
3. **Shared component layer.** One `DataList` with cursor paging, a status badge and a confirm dialog with a required reason, to confirm the shadcn layer composes as planned.

Exit criteria: each spike gives yes, no or workaround.

## 8. Draft phase plan

- **Phase 0: setup.** Create the submodule, its agent instructions and ignore rules; add the parent `AGENTS.md` exception note; SDL source-of-truth and drift check; UI architecture notes (folders, naming, error handling).
- **Phase 1: backend prerequisites** (spec, Security review and QA each).
  - 1.1 Refresh token design: hashed storage in Mongo, rotation, reuse detection, revocation on logout and account deletion, refresh/logout endpoint.
  - 1.2 CORS allowlist, cookie attributes (`HttpOnly`, `Secure`, `SameSite`), CSRF controls (Origin check plus custom header), rate limiting on refresh.
  - 1.3 Persisted-query registration for UI documents.
  - 1.4 Read gaps found while building screens (for example a "my interviews" list).
- **Phase 2: UI foundation.** Toolchain, codegen, Apollo setup, shared `ui/` layer (Button, Field, Badge, DataList with cursor paging, ConfirmDialog with required reason, PageLayout, role-based navigation, error and empty states), auth shell (login, signup, silent refresh, route guards).
- **Phase 3: Candidate.** UC01–UC06 and candidate decline.
- **Phase 4: Recruiter.** UC07–UC10 and status actions.
- **Phase 5: Interviews.** Scheduling, cancellation and reschedule flows, repair.
- **Phase 6: Admin.** Analytics page, users, embedding coverage, Grafana link.
- **Phase 7: hardening.** Accessibility audit, Playwright journeys per role including forbidden-access checks, independent Code, Security and QA reviews.

Each phase needs acceptance criteria tagged with stable IDs, and independent Code Reviewer, Security Engineer (auth, cookies, CORS) and QA verdicts per `AGENTS.md`.

## 9. Risks and things to watch

- **Cross-origin cookies** are the riskiest choice. They are safe only with same-site hosting. A different registrable domain would force `SameSite=None`, which weakens CSRF protection; rule it out. CORS must use an explicit origin allowlist, never `*`.
- **Apollo cache and authorization.** The cache is per session and must be cleared on logout or user switch. Wrong `typePolicies` cause stale or duplicated lists.
- **Persisted queries.** The backend can refuse unregistered documents; this is unproven with Apollo until spike 1.
- **Static export constraints** on dynamic routes and middleware.
- **Claim honesty.** With client-only Next.js, server components, route handlers, middleware and server actions are not exercised; say so when describing the project.
- **Operational:** a Next.js server (if not exported) adds a deployment unit and telemetry; Grafana has only a default password locally, so the Admin link is for local and demo use only.
- **Library churn and supply chain:** pin versions, commit the lockfile, review new packages. Verify all versions and framework behavior against current documentation.
- **Over-abstraction:** extract shared components from repeated use. The list, status badge and confirm dialog are the known repeats.
- **Contract changes touch two repos:** SDL in the backend, then a UI commit and a submodule pointer bump. Record that order in the spec.
- **Pre-MVP policy:** until the owner explicitly asks to start versioning (including API versioning) and migration scripts for schema changes (database, Kafka and so on) (owner decision 2026-11-08; the owner calls that point the first cloud release), API and analytics schemas have one active shape, so the UI must follow schema changes without compatibility layers; after that request the normal schema-evolution rules apply.
