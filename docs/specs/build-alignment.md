# Build alignment with the Foundation architecture

Status: complete for the bounded build alignment slice

## Scope and decisions

Align `build.sbt` with the documented Scala 3, Cats Effect/FS2, Sangria/http4s, Circe, MongoDB, and MUnit/Testcontainers stack. Keep one simple sbt application project with explicit, pinned dependencies and supported compiler flags. Use Java 17+ and Scala 3.9 LTS; keep the existing sbt 1.11.1 launcher in this slice.

The inspected source still uses Doobie/PostgreSQL in `UserRepo`, `Dao`, `Role`, and `Main.DB`. Keep these dependencies as an explicit transitional group until the persistence slice replaces that code. This build task does not migrate data, implement `Main.run`, change GraphQL operation/enum names, add Kafka/Spark, or create CI/CD. Convert the existing ScalaTest tests to MUnit and make minimal source adaptations needed by Scala 3 compilation.

Version choices are checked against Maven Central metadata and the official [Scala 3.9 release](https://scala-lang.org/news/3.9/), [http4s quick start](https://http4s.org/v0.23/docs/quickstart.html), and [MongoDB Scala driver setup](https://www.mongodb.com/docs/languages/scala/scala-driver/current/get-started/). Prefer stable libraries over milestone versions. The current build pins Doobie 1.0.0-RC12, replacing the original RC9 pin; this concurrent version change is preserved and included in compatibility verification. Retaining a Doobie release candidate is transitional compatibility debt, not a new database decision.

## Acceptance and evidence

| ID | Required result | Verification | Actual result |
|---|---|---|---|
| AC-01 | Build resolves pinned Scala 3/JDK17-compatible Foundation dependencies without unused example libraries or Scala 2-only flags | Compilation and dependency inspection | PASS: Scala 3.9.0 on Java 17.0.20; strict compilation and dependency trees succeed |
| AC-02 | Existing role behavior passes under MUnit; meaningful source compatibility checks pass | sbt test | PASS: two role tests and one existing GraphQL operation/enum validation test |
| AC-03 | SQL adapter dependencies remain explicitly transitional; GraphQL names and adapter behavior are preserved | Source review and applicable contract checks | PASS: named legacy dependency group, independent source review and schema smoke test; no database execution claimed |
| AC-04 | Local command explains the JDK prerequisite and runs project checks; docs/skills reflect the actual new baseline | Local validation, unsupported-JDK check, source/document review | PASS: Java 17 wrapper exits 0, Java 11 exits 1 before sbt with prerequisite message; eight skills validate |
| AC-05 | No DB service, data migration, future analytics infrastructure, or CI/CD is implied by dependency setup | Independent code/security review and final QA | PASS for the bounded scope; runtime and integration remain unverified |

## Owners and checkpoint

Scala Developer owns build/plugin/source/test compatibility changes. Coordinator owns baseline docs and the local verification command. Architect reviewed the bounded language/build slice; Security Engineer reviews dependency changes; Code Reviewer and final independent QA validate the result.

No database integration coverage or working MongoDB runtime is claimed merely by adding a driver and Testcontainers. Any unverified runtime, security-advisory, or integration gate must be reported separately from compilation and unit results.

## Completed verification

On 2026-09-16, with `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` and that JDK's `bin` directory prepended to `PATH`:

- `sbt test evicted dependencyTree 'Test / dependencyTree'`: exit 0; three tests passed, none failed.
- `bash scripts/check-local.sh`: exit 0; eight project-local skills validated and all three tests passed.
- `bash -n scripts/check-local.sh` and `git diff --check`: passed. No formatter is configured.
- Follow-up local-run verification: `Compile / run / fork := true` gives `IOApp` its own JVM. Bare `sbt run test` on project-selected Java 17 exits 0 without the non-main-thread warning; all three tests pass. The user-supplied no-op `Main.run` exits successfully and does not start an HTTP server or database client. Machine-specific JDK selection stays in ignored `.sbtopts`; shell/global Java remains 11.
- Resolved runtime dependencies include Logback core/classic 1.6.3, http4s Ember 0.23.37, MongoDB Scala/reactive/core 5.11.1, and PostgreSQL 42.7.10. Testcontainers 2.0.5 is test-only.

Independent Code Reviewer, scoped Security Engineer, and final QA verdicts: PASS. Security review covered the changed dependencies and selected published advisories, not a complete transitive vulnerability scan. No server, database integration, migration, or performance verification was performed. The next implementation slice remains the persistence/server work in the development plan, not further build expansion.
