# User Account Management

Status: implemented in the current checkout; live Mongo transaction and explain-plan evidence remains environment-dependent.

## Identity and Scope

- Task / business capability: complete local account lifecycle and self-service user profile management.
- User outcome: a new installation can create its first singleton Admin, other people can sign up as Candidate or Recruiter after bootstrap, users can log in and complete profiles, and each user can delete only their own account.
- Dependencies: current MongoDB user repository, served GraphQL transport, HS256 request authentication, singleton Admin invariant, replica-set transaction integration harness, and the existing hiring job/application model.
- Non-goals: email confirmation, password reset, social login, external identity-provider integration, Admin deletion of other users, impersonation, role changes through self-service, physical cascade deletion of hiring records, and frontend work.

## Verified Current Facts

- The served schema now exposes signup, login, bootstrap, `me`, bounded Admin user listing, profile update, and self-delete operations.
- HTTP authentication accepts HS256 bearer tokens whose `sub` maps to a currently Active stored user; the account service issues short-lived tokens.
- MongoDB setup now adds additive credential/lifecycle fields, a bootstrap registry, the unique canonical-name index, status/list indexes, and the role-specific profile validator.
- Existing hiring records reference user IDs, so physical user deletion would orphan recruiter, candidate, or event references.

## Public Contract

### Queries

```graphql
me: User
users(first: Int!, after: String, role: UserRole, status: UserStatus): UserConnection!
```

`me` returns the authenticated user's safe account and profile directly, or `null` with a top-level GraphQL error such as `UNAUTHORIZED` when no active authenticated account is available. `users` is Admin-only, bounded, cursor-paginated, and excludes password hashes, credentials, deleted profile content, and sensitive internal fields.

### Mutations

```graphql
signUp(input: SignUpInput!): SignUpResult!
login(input: LoginInput!): LoginResult!
bootstrapAdmin(input: BootstrapAdminInput!): BootstrapAdminResult!
updateMyProfile(input: UpdateMyProfileInput!): UpdateMyProfileResult!
deleteMyAccount: DeleteMyAccountResult!
```

`SignUpInput` contains `name`, `role`, and `password`; ordinary signup accepts only Candidate or Recruiter and returns `ADMIN_BOOTSTRAP_REQUIRED` until the first Admin exists. Admin signup is rejected with `ADMIN_BOOTSTRAP_ONLY`. `BootstrapAdminInput` contains `name` and `password` and is accepted only while the user registry is empty. `LoginInput` contains `name` and `password`.

Profile inputs are role-specific at signup. A Candidate must provide a valid `CandidateProfile`; a Recruiter must provide a valid `RecruiterProfile`; exactly one profile variant is accepted. Admin signup is unavailable and the singleton Admin has no profile. `updateMyProfile` supports Candidate and Recruiter accounts only; an Admin receives `PROFILE_UNSUPPORTED_FOR_ROLE`. Invalid Candidate/Recruiter profile variants use `PROFILE_ROLE_MISMATCH`. Email remains optional for legacy compatibility and is not a login identity.

### Authentication Contract

- Normalize names with one documented Unicode/case-folding policy before uniqueness checks and lookup.
- Enforce a unique `nameCanonical` MongoDB index.
- Hash passwords with an approved memory-hard password hashing adapter; never persist or log plaintext passwords.
- Return the same sanitized login failure for unknown names and wrong passwords.
- Issue short-lived HS256 access tokens with `sub`, `iss`, `aud`, `iat`, and `exp` claims.
- Every hiring request resolves the actor from the token subject and current MongoDB user state. Deleted users and client-controlled role claims cannot authorize requests.

## Persistence Shape and Indexes

Extend `users` additively with:

```text
_id
schemaVersion
name
nameCanonical
role
passwordHash
accountStatus: Active | Deleted
profile: optional tagged role-specific profile
createdAt
updatedAt
deletedAt: optional
```

Required indexes:

```text
unique(nameCanonical)
users(accountStatus, createdAt DESC, _id DESC)
users(role, accountStatus, createdAt DESC, _id DESC)
```

`profile` is one embedded document with `kind: Candidate` or `kind: Recruiter`; an Active Candidate or Recruiter must have the matching kind, while an Active Admin must have no profile. Deleted accounts are profile-less after deletion. Use a singleton bootstrap coordination record, such as `account_registry`, to serialize first-account creation and gate ordinary signup until bootstrap completes. The setup record and migration history must be versioned and restartable. Existing legacy users without password hashes require explicit credential enrollment or a local reset; do not derive passwords or invent names from existing fields.

Before creating the unique `nameCanonical` index, setup runs the idempotent `user-name-canonical-v1` migration. It backfills missing canonical names using the shared Unicode normalization policy, fails closed on invalid names or canonical collisions, and never invents replacement identities.

## Transaction and Concurrency Rules

### First Admin

`bootstrapAdmin` runs in a MongoDB transaction that acquires the bootstrap coordination record, verifies that `users` is empty, inserts exactly one Admin with the singleton marker, and records bootstrap completion permanently. Concurrent or later bootstrap attempts produce a typed already-initialized result; the singleton Admin cannot be recreated. Signup attempts observed before bootstrap completion return `ADMIN_BOOTSTRAP_REQUIRED` and do not insert a user.

### Signup

Signup validates input, requires exactly one role-matching profile for Candidate or Recruiter, hashes the password outside the database transaction, obtains a JWT before persistence, verifies the bootstrap registry is initialized, and then inserts the Active user. The unique name index is authoritative for concurrent same-name signup; duplicate-key failures become sanitized `NAME_TAKEN` errors.

### Profile Update

`updateMyProfile` derives the target from the authenticated actor. It may update only the actor's role-specific profile and safe display fields. It cannot change role, name uniqueness identity, password, or another user's profile. Admin accounts are profile-less and return `PROFILE_UNSUPPORTED_FOR_ROLE`; `PROFILE_ROLE_MISMATCH` is reserved for profile data whose variant does not match a Candidate or Recruiter role. Password change belongs to a separate future contract.

### Account Deletion

`deleteMyAccount` accepts no user ID. In one MongoDB transaction it:

1. Matches the authenticated active user.
2. Marks the account Deleted, removes `passwordHash` and profile data, writes `deletedAt`, and replaces the display name with a unique non-identifying tombstone.
3. Closes that user's open jobs when the user is a Recruiter or Admin, preserving job/application references and append-only application history.
4. Leaves applications and events queryable under their existing IDs while deleted-user presentation is sanitized.
5. Commits only if the actor update and any owned-job state changes succeed together.

The operation is idempotent for an already-deleted actor and cannot match a different user. Authentication must re-read account status, so an existing token stops authorizing after deletion without requiring a token blacklist.

## Acceptance Criteria

| ID | Given / When / Then | Evidence |
|---|---|---|
| UAM-AC01 | Given an empty user registry, when the first Admin bootstrap succeeds, then exactly one singleton Admin and one bootstrap record exist | `UserAccountService`; Mongo setup/transaction integration (live run pending) |
| UAM-AC02 | Given concurrent bootstrap and signup requests against an empty registry, when both race, then exactly one Admin is committed and signup is rejected until bootstrap completes | Transaction repository implementation; replica-set concurrency test pending |
| UAM-AC03 | Given a valid Candidate or Recruiter signup with exactly one matching profile, when the request succeeds, then a password hash and Active user are stored and no plaintext credential is persisted | Argon2 adapter, account service, BSON codec, GraphQL contract |
| UAM-AC04 | Given duplicate names with different case, when signup races or repeats, then one succeeds and the other returns `NAME_TAKEN` | Unique `nameCanonical` index, transactional repository conflict mapping, and account-service error translation |
| UAM-AC05 | Given valid and invalid credentials, when login is called, then only the matching Active user receives a signed token and failures are indistinguishable | JWT/auth tests and sanitized payload mapping |
| UAM-AC06 | Given a valid token, when `me` or profile update is called, then only the authenticated user's data changes | GraphQL access tests; service wiring |
| UAM-AC07 | Given an Admin, when `users` is queried, then results are bounded, paginated, role/status-filterable, and credential-free | SDL fixture, resolver, cursor codec, account list port |
| UAM-AC08 | Given any authenticated user, when `deleteMyAccount` is called, then only that account is logically deleted and its token no longer authorizes | Transaction repository implementation; live transaction test pending |
| UAM-AC09 | Given a Recruiter with open jobs and applications, when the account is deleted, then open jobs close atomically while applications and history remain consistent | Delete transaction implementation; live integration pending |
| UAM-AC10 | Given a deleted user or a client-supplied Admin role claim, when a protected operation is called, then authorization fails closed | Active-user reload and existing JWT access tests |
| UAM-AC11 | Given a Candidate, Recruiter, Admin, or deleted account document, when it is written or decoded, then the role/profile one-of and singleton invariants are preserved | `UserProfile`, Mongo codec, Mongo validator, profile migration integration |

## Migration, Recovery, and Risks

- Expand readers/codecs before requiring the tagged profile shape. The versioned profile migration converts legacy Candidate and Recruiter documents and fails closed for missing or contradictory active profiles.
- Backfill no passwords and no guessed unique names. For a disposable empty local database, deterministic setup may create the registry and indexes directly.
- Test transaction failure after user update and after job closure; verify retry/idempotency and that no partial account deletion is visible.
- Preserve applications and application events because they are operational history. A later retention/anonymization policy must be separately specified.
- Do not claim password security, token revocation, or deletion recovery beyond the selected hasher, token expiry, transaction tests, and backup/restore evidence.

## Implementation Checkpoint

- Source: `UserAccountService`, `Argon2PasswordHasher`, `JwtActorAuthenticator`, `MongoUserRepository`, `MongoHiringSetup`, and `HiringGraphQLSchema` implement the contract.
- Contracts: `src/test/resources/graphql/hiring.graphql` and the route/schema tests cover the additive API shape.
- Local evidence: focused account-service tests passed 8/8; `IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec` passed 14 Mongo/Testcontainers tests, including fresh-database setup, tagged-profile backfill, fail-closed setup, sparse email-index replacement, transactional account-name conflict mapping, transactional application behavior, and explain-plan checks. Account lifecycle transaction/concurrency criteria remain separately identified where their dedicated integration coverage is still pending.
- Query analysis: the account access patterns use canonical-name lookup, status/createdAt keyset listing, and role/status filtering. The new indexes are recorded by the repeatable setup migration; no latency or production SLO claim is made.

## Non-Functional Assumptions

- Initial account workload is bounded local hiring usage; no performance target is claimed until signup/login concurrency and user-list pagination are measured.
- JWT access tokens are short-lived and stateless; refresh tokens, password reset, and external identity providers are future scopes.
- MongoDB replica-set transactions are required for bootstrap coordination and account deletion tests.
