# User Account Management

Status: ready for implementation planning; no Phase 4.5 implementation is included in this document change.

## Identity and Scope

- Task / business capability: complete local account lifecycle and self-service user profile management.
- User outcome: a new installation can create its first singleton Admin, other people can sign up as Candidate or Recruiter after bootstrap, users can log in and complete profiles, and each user can delete only their own account.
- Dependencies: current MongoDB user repository, served GraphQL transport, HS256 request authentication, singleton Admin invariant, replica-set transaction integration harness, and the existing hiring job/application model.
- Non-goals: email confirmation, password reset, social login, external identity-provider integration, Admin deletion of other users, impersonation, role changes through self-service, physical cascade deletion of hiring records, and frontend work.

## Verified Current Facts

- The served schema has no user signup, login, user listing, profile update, or account deletion operation.
- Current HTTP authentication accepts an HS256 bearer token whose `sub` maps to a stored user; the application does not issue tokens.
- Current MongoDB setup has a `users` collection and a singleton Admin index, but no credential fields or account-deletion state.
- Existing hiring records reference user IDs, so physical user deletion would orphan recruiter, candidate, or event references.

## Public Contract

### Queries

```graphql
me: User
users(first: Int!, after: String, role: UserRole, status: UserStatus): UserConnection!
```

`me` returns the authenticated user's safe account and profile. `users` is Admin-only, bounded, cursor-paginated, and excludes password hashes, credentials, deleted profile content, and sensitive internal fields.

### Mutations

```graphql
signUp(input: SignUpInput!): AuthPayload!
login(input: LoginInput!): AuthPayload!
bootstrapAdmin(input: BootstrapAdminInput!): AuthPayload!
updateMyProfile(input: UpdateMyProfileInput!): UserPayload!
deleteMyAccount: DeleteAccountPayload!
```

`SignUpInput` contains `name`, `role`, and `password`; ordinary signup accepts only Candidate or Recruiter and returns `ADMIN_BOOTSTRAP_REQUIRED` until the first Admin exists. `BootstrapAdminInput` contains `name` and `password` and is accepted only while the user registry is empty. `LoginInput` contains `name` and `password`.

Profile inputs are role-specific and optional at signup. Existing Candidate profile fields are `skills`, `experienceSummary`, and `resumeRef`; the Recruiter profile contract must be defined from recruiter use cases before implementation, without making credentials or email mandatory.

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
profile: optional role-specific profile
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

Use a singleton bootstrap coordination record, such as `account_registry`, to serialize first-account creation and gate ordinary signup until bootstrap completes. The setup record and migration history must be versioned and restartable. Existing legacy users without password hashes require explicit credential enrollment or a local reset; do not derive passwords or invent names from existing fields.

## Transaction and Concurrency Rules

### First Admin

`bootstrapAdmin` runs in a MongoDB transaction that acquires the bootstrap coordination record, verifies that `users` is empty, inserts exactly one Admin with the singleton marker, and records bootstrap completion. Concurrent bootstrap attempts produce one committed first account and a typed already-initialized result for losers. Signup attempts observed before bootstrap completion return `ADMIN_BOOTSTRAP_REQUIRED` and do not insert a user.

### Signup

Signup validates input, hashes the password outside the database transaction, verifies the bootstrap registry is initialized, and then inserts the Active user. The unique name index is authoritative for concurrent same-name signup; duplicate-key failures become sanitized `NAME_TAKEN` errors.

### Profile Update

`updateMyProfile` derives the target from the authenticated actor. It may update only the actor's role-specific profile and safe display fields. It cannot change role, name uniqueness identity, password, or another user's profile. Password change belongs to a separate future contract.

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
| UAM-AC01 | Given an empty user registry, when the first Admin bootstrap succeeds, then exactly one singleton Admin and one bootstrap record exist | Domain unit test plus Mongo transaction integration test |
| UAM-AC02 | Given concurrent bootstrap and signup requests against an empty registry, when both race, then exactly one Admin is committed and signup is rejected until bootstrap completes | Replica-set concurrency integration test |
| UAM-AC03 | Given a valid Candidate or Recruiter signup, when the request succeeds, then a password hash and Active user are stored and no plaintext credential is persisted | Service, codec, and security tests |
| UAM-AC04 | Given duplicate names with different case, when signup races or repeats, then one succeeds and the other returns `NAME_TAKEN` | Unique-index integration test |
| UAM-AC05 | Given valid and invalid credentials, when login is called, then only the matching Active user receives a signed token and failures are indistinguishable | Auth unit/API tests |
| UAM-AC06 | Given a valid token, when `me` or profile update is called, then only the authenticated user's data changes | GraphQL authorization tests |
| UAM-AC07 | Given an Admin, when `users` is queried, then results are bounded, paginated, role/status-filterable, and credential-free | GraphQL contract and access tests |
| UAM-AC08 | Given any authenticated user, when `deleteMyAccount` is called, then only that account is logically deleted and its token no longer authorizes | Transaction integration test |
| UAM-AC09 | Given a Recruiter with open jobs and applications, when the account is deleted, then open jobs close atomically while applications and history remain consistent | Replica-set transaction and read-model integration tests |
| UAM-AC10 | Given a deleted user or a client-supplied Admin role claim, when a protected operation is called, then authorization fails closed | Security/API tests |

## Migration, Recovery, and Risks

- Expand readers/codecs before requiring new fields; support legacy users as credential-incomplete until an explicit enrollment policy is implemented.
- Backfill no passwords and no guessed unique names. For a disposable empty local database, deterministic setup may create the registry and indexes directly.
- Test transaction failure after user update and after job closure; verify retry/idempotency and that no partial account deletion is visible.
- Preserve applications and application events because they are operational history. A later retention/anonymization policy must be separately specified.
- Do not claim password security, token revocation, or deletion recovery beyond the selected hasher, token expiry, transaction tests, and backup/restore evidence.

## Non-Functional Assumptions

- Initial account workload is bounded local hiring usage; no performance target is claimed until signup/login concurrency and user-list pagination are measured.
- JWT access tokens are short-lived and stateless; refresh tokens, password reset, and external identity providers are future scopes.
- MongoDB replica-set transactions are required for bootstrap coordination and account deletion tests.
