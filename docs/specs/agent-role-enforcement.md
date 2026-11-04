# Agent role enforcement

## Identity and scope

- Task: move agent role boundaries, model choice and sandboxing from prompt text into enforced Claude Code configuration, following current Anthropic guidance.
- Status: in progress
- Coordinator / implementation owners: Product Manager (main session); Technical Writer for canonical documents.
- User outcome: a reviewer cannot change the code it reviews, an author cannot grade its own work in its own session, a false "reviewed" line is caught, and shell commands run inside an OS boundary.
- Authorized scope (user, 2026-11-04): the items proposed in the agentic-settings review, with Sonnet for executing a defined plan and Opus for planning, specs and refactoring.
- Non-goals: application code; committing; deleting the leftover `.claude/worktrees/kafka-hardening` worktree (reported, not removed); changing `~/.claude/settings.json` (auto mode is the user's own setting, and the guard denies writes outside the project).

## Source context and decisions

- Verified implementation: `.claude/settings.json`, `.claude/agents/*.md`, `.claude/skills/*/SKILL.md`, `scripts/guard-harness.py`, `scripts/check-done.sh`, `scripts/check-skills.py`, `scripts/check-spec-verdicts.py`. Claude Code 2.1.318; `bubblewrap` and `socat` installed.
- Target requirements: code.claude.com docs for best practices, subagents, skills, hooks, sandboxing, memory and model configuration (read 2026-11-04).
- Decisions:
  - D1 Session model `opusplan` (Opus in plan mode, Sonnet in execution) replaces the Sonnet pin and the old "do not use opusplan" rule, because the user wants Opus for planning and Sonnet for executing a defined plan.
  - D2 `product-manager` declares `model: opus`, because it writes specs. Refactoring goes to a new `scala-refactorer` subagent (Opus, high) using the `scala-developer` skill; a behavior-preserving structural change has no spec to bound it, so judgment matters more than for spec-bound authoring.
  - D3 Reviewer skills use `context: fork` so `/code-reviewer`, `/security-engineer` and `/qa-engineer` never run in the author's context. Because docs do not define a preloaded skill that itself forks, reviewer agents do not use `skills:`; a direct Agent launch reads the skill file.
  - D4 Reviewers keep Bash for test and git evidence, restricted by a frontmatter hook to allowlisted programs with exact argument shapes. A single scanner refuses backslashes, expansion, redirection, subshells, comments and newlines, so the shell cannot read a command differently from the hook; review round 1 proved 11 bypasses in the first design, which used two separate parsers and flag denylists. The hook is a guardrail against a reviewer changing what it reviews, not a security boundary: running tests still writes build output and executes project code.
  - D5 The Stop gate blocks repeatedly on skills and verdict lines, because those cannot legitimately fail in the middle of a task. The review log comparison, documentation currency and compile freshness stay one-shot: an unrelated commit can make a valid review look stale, and a turn can legitimately end unfinished. Subagent stops stay one-shot, because a read-only reviewer cannot fix a failing gate. After eight blocks in a row Claude Code overrides the next one, which prevents a loop.
  - D5a The Stop hook never compiles. Hooks run outside the sandbox and compiling executes build definitions and macros, so a compiling hook would run agent-written code with full access and no prompt (found while testing the first design, which did this). It compares file times and asks the agent to run `sbt Test/compile`, which runs inside the sandbox. It is a reminder, not proof.
  - D6 The "run check-local and QA" message stays a `systemMessage`, which reaches the user. `additionalContext` would reach the agent but keeps it running, costing a turn at every stop.
  - D7 Sandbox on with `autoAllowBashIfSandboxed: false`, so enabling it adds a boundary without removing an approval prompt, and with `allowUnsandboxedCommands: false`, so a failed command cannot be retried outside it. Docker-dependent entry points are the narrow exclusions, because Docker does not work in the sandbox; agents already run Docker-backed tests only through the wrapper.
  - D7a Sandbox wiring found by testing: `/tmp` is writable because the JVM temp directory is `/tmp` and sbt fails without it; `SBT_OPTS=-Dsbt.server.forcestart=true` (settings `env`, Claude Code sessions only) skips sbt's boot socket, which the sandbox refuses. `allowAllUnixSockets` was rejected because it would also expose the Docker socket.
  - D7c Inside the sandbox, protected paths such as `.mcp.json` and `.claude/hooks` appear as `/dev/null` character devices. `check-docs-current.py` skips character devices, because it would otherwise read them as decision changes on a clean tree. `.gitignore` covers only the names that never belong in this repository; ignoring `.mcp.json` would hide a real one from the gate. A sandboxed `git status` therefore still lists a few of these, so stage explicit paths, never `git add -A`.
  - D7d `denyWrite` covers the known files that run or configure code outside the sandbox: `scripts/`, `.githooks/`, compose files, Dockerfiles, existing build definitions and tool configuration, `.claude/rules`, `AGENTS.md`, `CLAUDE.md`, `.local/logs` and `.local/config`. Without it, sandboxed test code could rewrite a hook script or compose file that then runs unsandboxed with no prompt (review rounds 1 and 2). `.local/logs` is denied whole so a link planted there cannot redirect an unsandboxed wrapper's log writes. `.git` is denied whole, because a planted `.git/commondir` makes `git status` load another config and run its `core.fsmonitor`, which review round 3 reported and a scratch repository confirmed, and which the unprompted Stop hook would have triggered; denying only that file is impossible, because git dies on an empty or `/dev/null` `commondir`. Git commands that write `.git` therefore run outside the sandbox as named exclusions, still behind approval. It does not cover new files sbt would load; see D7h. The Edit and Write tools are outside the sandbox and still go through the guard's approval.
  - D7j Entry points outside the sandbox start Python with `-I`, because `python3 -` or `-c` puts the working directory first on the path, so a `pathlib.py` planted in the writable root would run as the user on the unprompted `status` or `start` (review round 3, confirmed in a scratch directory).
  - D7k Format checks run outside the sandbox only: in the pre-commit formatter and `scripts/check-local.sh`. scalafmt downloads itself, which the sandbox prevents, so `sbt scalafmtCheckAll` left the allowlists.
  - D7e The wrapper's code-running verbs and `check-local.sh` moved from `allow` to `ask`. They are outside the sandbox because they need Docker, and they run test code an agent may have edited, so running them without a prompt would make the sandbox irrelevant for the main test entry point. This is a safe default the owner can reverse.
  - D7f Owner decision (2026-11-04): isolate the sbt home per project. `SBT_OPTS` in settings `env` points Claude Code sessions at `.sbt-home/` in each build (global base, boot, Ivy and Coursier home; ignored by git and Docker), and `~/.sbt`, `~/.ivy2` and `~/.cache/coursier` are neither writable nor readable from the sandbox, which stops session test code reaching the owner's terminal builds or other projects through them. Proven: with those directories hidden, sandboxed `sbt test` passes 1827 of 1827 from `.sbt-home`. Cost: a second dependency copy (about 400 MB per build here).
  - D7h Remaining gap, not closable by `denyWrite`: sandboxed code can still create new `*.sbt`, `.sbtrc` or `project/*.scala` files and write `.sbt-home` and `target`, because sbt needs those writable or cannot read a denied absent file. sbt loads new build files, and an approved wrapper run or a Claude Code commit also loads jars and the launcher from `.sbt-home` and classes from `target`, so these execute as the user in a later run outside the sandbox in this repository. Every such run starts with a person: approving a wrapper run or a commit, or running sbt or `git commit` in a terminal (a terminal does not use `.sbt-home`). Before approving one on a tree an agent changed, check `git status --ignored` for unexpected build files; that shows new files, not changed content in ignored files.
  - D7i JVM downloads do not work in the sandbox: the proxy answers the JVM's tunnel with 407, which `jdk.http.auth.tunneling.disabledSchemes` does not fix. So the allowlist is empty, and a dependency change needs one approved wrapper run, which runs outside the sandbox with the same `SBT_OPTS` and fills `.sbt-home`: `scripts/run-local-tests.sh test '<glob>'` for the root build and `scripts/run-local-tests.sh analytics '<glob>'` for analytics. Sandboxed sbt then only reads it.
  - D7g `sbt test` passes fully in the sandbox. A `DisposableAtlasSpec` case needed DNS, which the sandbox lacks (`getent hosts` fails there), until a concurrent change outside this task gave `DisposableAtlas.authorizedUri` a stub `DnsClient`.
  - D7b An unlisted host prompts in manual mode rather than being refused, so nothing reaches the network without the user's approval. `network.strictAllowlist`, which refuses instead, works only in user or managed settings, so it stays the user's choice.
  - D8 A completed review leaves a local log entry, read only from the reviewer's own report (final message or `SubagentHandback` input) and only from a line that is exactly the verdict, because review round 1 caught the first design logging PASS for a reviewer that returned FAIL. `check-spec-verdicts.py` compares it only with uncommitted specs marked done, because older reviews predate the log and the log is local to this clone. Committing stays the human override. The log proves a reviewer role ran and stated a verdict since the last commit; it does not prove the review was honest or which spec it covered, and an author could prompt a reviewer to state PASS.

## Behavior and contracts

No API, event or schema contract changes. `.claude/settings.json`, agent frontmatter and the operator scripts are the changed contracts.

## Domain language and examples

- Context: agent workflow; no hiring aggregate or invariant is touched. New terms are workflow vocabulary, documented in `docs/agent-development.md`, not hiring vocabulary for `docs/domain-language.md`.

| Rule | Example tag | Scenario |
|---|---|---|
| A reviewer collects evidence only | ARE-01.1 | Given the read-only scope, `git diff`, `grep -n x src/A.scala`, `sbt test` and `scripts/run-local-tests.sh test` are allowed |
| | ARE-01.2 | `sed -i s/a/b/ src/A.scala`, `touch x`, `echo x > y`, `git checkout .`, `find . -delete`, `sort -o a b`, `python3 evil.py`, `echo $(touch x)` are refused |
| | ARE-01.3 | Malformed hook input is refused (fail closed), and `scripts/run-local-tests.sh stop` is refused |
| Authors of documents stay in their paths | ARE-02.1 | The writer may edit `docs/api.md` and `README.md`, not `src/main/A.scala` or `docs/../build.sbt` |
| | ARE-02.2 | The architect may edit `docs/specs/x.md` and `ARCHITECTURE.md`, not `src/main/A.scala` |
| Reviewer skills fork | ARE-03.1 | `check-skills.py` fails if a reviewer skill lacks `context: fork` and its own `agent` |
| Role definitions carry their enforcement | ARE-04.1 | `check-skills.py` fails if a reviewer lacks the read-only hook, or an author lacks its `skills` preload |
| Stop gate | ARE-05.1 | With a done spec lacking verdicts, Stop exits 2 on the first stop and again when `stop_hook_active` is true |
| | ARE-05.2 | With only a documentation failure, the first stop exits 2 and the repeated stop exits 0 |
| | ARE-05.3 | A repeated subagent stop exits 0 without checks |
| Compile freshness | ARE-06.1 | Changed Scala newer than its build's newest compiler analysis, or with no analysis, exits 2 once and names `sbt Test/compile`; staged and non-ASCII changes count; older Scala, deleted Scala, integration sources, sbt edits and a tree without Scala changes pass; each build is judged by its own analysis; the hook never compiles |
| Review log | ARE-07.1 | A reviewer report with the line `VERDICT: PASS`, in a final message or a handback, appends role, verdict, time and commit; the report template and quoted text append nothing; a link at the log path is not followed; it never blocks |
| | ARE-07.2 | An uncommitted done spec claiming PASS fails without a logged PASS since the last commit, fails if the latest entry is FAIL, and passes with a logged PASS; `not applicable` and committed specs are not compared; a non-ASCII spec name is still compared |
| | ARE-07.3 | `--lines-only` checks verdict lines and skips the log |
| Fail closed | ARE-08.1 | Both PreToolUse hooks declare `onFailure: block` |
| Sandbox | ARE-09.1 | Sandboxed `sbt test` works; a sandboxed listing of `~/.ssh` shows nothing; an unlisted host cannot be reached without the user's approval |
| | ARE-09.2 | From inside the sandbox, `scripts/guard-harness.py`, `.githooks/pre-commit`, `build.sbt`, `project/plugins.sbt`, `compose.test.yaml`, `.sbtopts`, `.claude/rules/`, `.local/logs/` and `AGENTS.md` are not writable, and sbt still starts |
| | ARE-09.3 | The wrapper's code-running verbs and `check-local.sh` need approval |
| | ARE-09.4 | With `~/.sbt`, `~/.ivy2` and `~/.cache/coursier` hidden and read-only, sandboxed `sbt test` passes from `.sbt-home`, and the analytics build compiles from its own |
| Live behaviour | ARE-10.1 | `scripts/check-agent-hooks-live.sh` shows, in real `claude -p` sessions, repeated Stop blocking, a reviewer refused a write, the wrapper's test verb denied with nobody to approve it, and a host outside the allowlist unreachable with nobody to approve it |

## Acceptance and evidence

| ID | Criterion | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| ARE-01, 02 | Agent scope hook | `scripts/guard-agent-scope.py`, `scripts/tests/test_guard_agent_scope.py` | unittest | Passing; includes every reported bypass |
| ARE-03, 04 | Definition checks | `scripts/check-skills.py`, `scripts/tests/test_check_skills_definitions.py` | unittest, check-skills | Passing |
| ARE-05, 06 | Stop gate | `scripts/check-done.sh`, `scripts/tests/test_check_done_gate.py` | unittest | Passing |
| ARE-07 | Review log | `scripts/record-review-run.py`, `scripts/check-spec-verdicts.py`, `scripts/tests/test_review_run_log.py` | unittest; live entry from a re-review | Unit tests passing; live entry pending re-review |
| ARE-08 | Fail closed | `.claude/settings.json`, `scripts/tests/test_check_skills_definitions.py` | unittest | Passing; whether Claude Code honors `onFailure` is not observed, so hooks also carry `\|\| exit 2` |
| ARE-09 | Sandbox | `.claude/settings.json`, definition test | live commands, unittest | 1827 of 1827 in sandboxed `sbt test` with the home directories hidden (338 jars in `.sbt-home`); analytics compiles from its own (524 jars); ARE-09.2 observed |
| ARE-10 | Live test | `scripts/check-agent-hooks-live.sh` | live run | PASS on 2026-11-04, four of four: 9 Stop blocks in one session; reviewer `touch` refused; wrapper verb denied and not run; unlisted host unreachable |

## Performance and cost

`AGENTS.md` shrinks from 22,131 to about 17,800 bytes, which applies for every session and subagent. The live test spends real tokens on Haiku with a dollar cap and is run on demand only.

## Documentation impact

| Document | Decision to state | Why it matters | Status |
|---|---|---|---|
| `docs/agent-development.md` | Model policy, reviewer hook, forked skills, Stop gate, sandbox, review log, path rules | Agents and operators rely on it to know what is enforced | updated |
| `docs/engineering-quality.md` | New operator script in the contract | Contract coverage | updated |
| `docs/spec-driven-development.md` | Remove "Inherit session model/settings" | Contradicts the model policy | updated |
| `README.md` | `scripts/check-local.sh` without `bash`, because exclusions match command text | The `bash` form runs inside the sandbox, where Docker is unavailable | updated |
| `docs/wiki/workflows-and-patterns.md` | Only if it states superseded behavior | Avoid stale claims | checked, no change needed |

## Implementation handoff

- Slices: scripts and tests; agents and skills; settings; `AGENTS.md` and rules; docs; live test; reviews.
- Allowed write paths: `.claude/`, `AGENTS.md`, `CLAUDE.md`, `scripts/`, `docs/`, `README.md`, `.gitignore`, `.dockerignore`.

## Checkpoint and review

- Completed criteria and changed files: implementation of ARE-01 to ARE-10 and canonical documents are in the working tree, not committed.
- Latest commands/results and their scope: script tests pass; `check-skills.py` and `check-docs-current.py` PASS; `check-agent-hooks-live.sh` PASS, four of four; sandboxed `sbt test` 1827 of 1827; analytics `Test/compile` passes in the sandbox; wrapper `test` 1827 of 1827 outside the sandbox; sandboxed `scripts/run-local-tests.sh status` reached Docker when run alone. `scripts/check-local.sh` not run, because it takes about two hours and the workflow change touches none of the application code it covers; the owner decides whether to run it.
- Source changes outside this task, made by someone else during this work and left untouched: `SafeDiagnostics.withEventSink` (commit `1ec7a9d` left a lambda that fails to link with `LambdaConversionException`, which also hung `sbt test` for 15 minutes), `DisposableAtlas` with a stub `DnsClient`, and `HiringRoleMatrixAcceptanceIntegrationSpec`.
- Review rounds: rounds 1, 2 and 3 returned FAIL from Code Reviewer and Security Engineer, and round 4 returned FAIL from Code Reviewer, Security Engineer and QA. The owner chose per-project sbt isolation (D7f) on 2026-11-04, and on the same day asked to commit before a passing review, so this spec stays in progress and no verdict line records PASS.
- Review log coverage: `.local/logs/review-runs.jsonl` holds rounds 2 onward. Round 1 has no entries: the first recorder missed the Code Reviewer's handback and logged a false PASS for the Security Engineer, which was deleted when that defect was found.
- Open from round 4: (1) Security: a planted copy of an excluded script or `.git` in a subdirectory; `guard-harness.py` now requires the project root for excluded commands, and that fix has had no independent review. (2) Code Reviewer: D7d and `docs/agent-development.md` imply all git writes are excluded, but only the 16 listed are; any other git write fails on read-only `.git` and needs a terminal. (3) QA: the ARE-07 row still reads as pending.
- Blockers and next concrete action: fix (2) and (3), then re-run Code Reviewer, Security Engineer and final QA on the fixes. `scripts/check-local.sh` not run. Approval was observed only in default permission mode.
- Code Reviewer verdict and scope: pending
- Security Engineer verdict and scope: pending
- Final independent QA verdict: pending
