# Current Contract Policy

Before MVP this repository maintains one active hiring contract. Startup removes every hiring-owned MongoDB collection and recreates the current validators, indexes, and search definitions. Local data is disposable.

GraphQL SDL, Mongo documents, cursor values, operational-event payloads, and search metadata describe only the active shape. There are no legacy readers, compatibility windows, backfills, stored schema revisions, or migration ledgers. A contract change updates its implementation, SDL fixture, executable operations, and this document in the same change.

Critical integrity remains enforced by MongoDB uniqueness and transactional identity, ownership, status, and state predicates. Resetting local data does not relax authorization, lifecycle, duplicate-application, or closed-job invariants.

Run `sbt test` for the Docker-independent contract suite and `sbt 'IntegrationTest / test'` for disposable MongoDB and HTTP checks. The current scope and evidence are tracked in [the active specification](specs/pre-mvp-contract-reset.md).
