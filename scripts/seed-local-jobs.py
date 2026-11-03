"""Seeds a local API with synthetic hiring data for latency measurements. Run it through scripts/seed-local-jobs.sh.

Uses only the Python standard library. Credentials stay in memory and in one 0600 file under .local/data/; they are never
printed. Only a loopback API is contacted (no redirects, no proxy), every response value that reaches the file is validated, and
every request has a timeout. The run is bounded by the number of jobs (at most 500) and accounts (at most 8).
"""

import datetime
import json
import os
import re
import secrets
import signal
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

REQUEST_TIMEOUT = 5  # seconds per request
HEARTBEAT = 5        # seconds between progress lines; with one request in flight every gap stays <= 10 s
PAGE = 100           # myJobs page size (the API maximum)
NAMESPACE = uuid.UUID("6f1c0b52-5d0e-4b57-9a54-0c6b1e5a7d11")  # fixed: makes job idempotency keys repeatable
RECRUITER = "perf-recruiter"
CANDIDATE = "perf-candidate-{}"
ENV_FILE = ".local/data/perf-seed.env"
CITIES = [("Germany", "Berlin"), ("Portugal", "Lisbon"), ("Ukraine", "Kyiv"), ("Poland", "Warsaw"), ("Czechia", "Prague")]
SKILLS = ["scala", "kafka", "mongodb", "graphql", "spark", "docker"]
ROLES = ["Backend Engineer", "Data Engineer", "Platform Engineer", "Search Engineer"]
TOKEN = re.compile(r"[A-Za-z0-9._~+/=-]{10,4096}")
INSTANT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z")
LOOPBACK = re.compile(r"https?://(127\.0\.0\.1|localhost|\[::1\])(:[0-9]{1,5})?(/[A-Za-z0-9._~/-]*)?")
PASSWORD_LINE = re.compile(r"(PERF_[A-Z0-9_]+_PASSWORD)='([A-Za-z0-9]{12,64})'\n?")

SIGN_UP = """mutation SignUp($input: SignUpInput!) { signUp(input: $input) { __typename
  ... on AuthSuccess { accessToken expiresAt } ... on ValidationError { code } ... on DomainError { code retryAfterSeconds } } }"""
LOGIN = """mutation Login($input: LoginInput!) { login(input: $input) { __typename
  ... on AuthSuccess { accessToken expiresAt } ... on ValidationError { code } ... on DomainError { code retryAfterSeconds } } }"""
CREATE_JOB = """mutation CreateJob($input: CreateJobInput!) { createJob(input: $input) { __typename
  ... on Job { id status } ... on ValidationError { code message } ... on DomainError { code } } }"""
PUBLISH_JOB = """mutation PublishJob($input: JobActionInput!) { publishJob(input: $input) { __typename
  ... on Job { id status } ... on ValidationError { code message } ... on DomainError { code } } }"""
MY_OPEN_JOBS = """query MyOpenJobs($first: Int!, $after: Cursor) { myJobs(first: $first, after: $after, status: OPEN) {
  edges { node { id } } pageInfo { hasNextPage endCursor } } }"""


class Stop(Exception):
    def __init__(self, code, reason, message):
        super().__init__(message)
        self.code, self.reason, self.message = code, reason, message


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuses every redirect, so a token or password is never replayed to another location."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


# No proxy (the environment's proxy variables are ignored) and no redirects: the request goes to the loopback URL or fails.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


class Progress:
    """Prints a status line at most every HEARTBEAT seconds while requests are in flight."""

    def __init__(self):
        self.started = time.monotonic()
        self.last = self.started
        self.phase = "starting"

    def say(self, message, force=False):
        now = time.monotonic()
        if force or now - self.last >= HEARTBEAT:
            print("[%3ds] %s" % (now - self.started, message), flush=True)
            self.last = now

    def phase_started(self, phase):
        self.phase = phase
        self.say(phase, True)

    def tick(self):
        self.say("still working: " + self.phase)


PROGRESS = Progress()


def valid_token(value):
    return isinstance(value, str) and TOKEN.fullmatch(value) is not None


def valid_instant(value):
    """A strict ISO-8601 UTC instant such as 2026-11-03T12:00:00Z (optional fraction); anything else is refused."""
    if not isinstance(value, str) or INSTANT.fullmatch(value) is None:
        return False
    try:
        datetime.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return False
    return True


def loopback_base(value):
    return isinstance(value, str) and LOOPBACK.fullmatch(value) is not None


def checked_session(data):
    """Returns (token, expiresAt) of an AuthSuccess, or stops: a value that would reach the env file must be well formed."""
    token, expires = data.get("accessToken"), data.get("expiresAt")
    if not valid_token(token):
        raise Stop(1, "invalid-token", "the API returned an access token in an unexpected format; nothing was written")
    if not valid_instant(expires):
        raise Stop(1, "invalid-expiry", "the API returned a token expiry that is not an ISO-8601 instant; nothing was written")
    return token, expires


def post(base, query, variables, token=None, rate_limit_reason="rate-limited"):
    """One GraphQL request; returns the decoded data or raises Stop for transport and top-level GraphQL failures."""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(base + "/graphql", json.dumps({"query": query, "variables": variables}).encode(), headers)
    try:
        with OPENER.open(request, timeout=REQUEST_TIMEOUT) as response:
            body = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 429:
            raise Stop(2, "rate-limited", "the API answered HTTP 429 (query cost budget or rate limit); wait a minute and rerun")
        raise Stop(1, "http-%d" % error.code, "the API answered HTTP %d" % error.code)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        raise Stop(2, "api-unreachable", "no usable answer from %s/graphql within %ds" % (base, REQUEST_TIMEOUT))
    PROGRESS.tick()
    errors = body.get("errors") or []
    if errors:
        code = str((errors[0].get("extensions") or {}).get("code", "UNKNOWN"))[:40]
        if code == "RATE_LIMITED":
            raise Stop(2, rate_limit_reason, "the API rate limited this call (RATE_LIMITED); wait a minute and rerun")
        raise Stop(1, "graphql-" + re.sub(r"[^a-z0-9]+", "-", code.lower()), "the API returned the GraphQL error %s" % code)
    return body.get("data") or {}


def read_passwords(path):
    """Passwords written by an earlier run (KEY='value' lines); an unreadable file counts as none."""
    found = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                match = PASSWORD_LINE.fullmatch(line)
                if match:
                    found[match.group(1)] = match.group(2)
    except OSError:
        pass
    return found


def write_private(path, text):
    """Writes text to path with mode 0600 through a temporary file in the same directory, then renames it into place."""
    directory = os.path.dirname(path)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=directory, prefix=".perf-seed.")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        if os.path.exists(temporary):
            os.unlink(temporary)
        raise


def render(passwords, tokens=None, expiries=None):
    """The env file: the passwords always; the token export and its expiry once every token has been validated."""
    lines = ["# Synthetic local performance seed credentials. Generated; never commit or share.\n"]
    lines += ["%s='%s'\n" % (key, value) for key, value in sorted(passwords.items())]
    if tokens:
        if not all(valid_token(token) for token in tokens) or not all(valid_instant(value) for value in expiries):
            raise Stop(1, "invalid-session-value", "refusing to write a token or expiry in an unexpected format")
        lines.append("export TRAFFIC_BEARER_TOKENS='%s'\n" % ",".join(tokens))
        lines.append("# tokens expire at (earliest): %s\n" % min(expiries))
    return "".join(lines)


def auth(base, name, role, profile, password_key, passwords, path):
    """Logs in with the stored password, or signs up with a new random one; returns (token, expiresAt).

    A rate-limited login stops the run (it never falls through to signUp). A new password is written to the env file
    right after the signUp succeeds, so a later interruption cannot leave an account whose password is lost."""
    known = passwords.get(password_key)
    if known:
        data = post(base, LOGIN, {"input": {"idempotencyKey": str(uuid.uuid4()), "name": name, "password": known}},
                    rate_limit_reason="auth-rate-limited")["login"]
        if data["__typename"] == "AuthSuccess":
            return checked_session(data)
        if data.get("code") == "RATE_LIMITED":
            raise Stop(2, "auth-rate-limited", "login is rate limited; retry in %s s" % data.get("retryAfterSeconds", 60))
    password = secrets.token_urlsafe(32).replace("-", "A").replace("_", "B")[:32]
    sign_up = {"idempotencyKey": str(uuid.uuid4()), "name": name, "role": role, "password": password, "profile": profile}
    data = post(base, SIGN_UP, {"input": sign_up}, rate_limit_reason="auth-rate-limited")["signUp"]
    if data["__typename"] != "AuthSuccess":
        code = data.get("code", "UNKNOWN")
        if code == "RATE_LIMITED":
            raise Stop(2, "auth-rate-limited", "signup is rate limited; retry in %s s" % data.get("retryAfterSeconds", 60))
        raise Stop(2, "account-unavailable",
                   "cannot sign up %s (%s); if the account exists from an earlier run whose %s was lost, reset the local database or use another name" % (name, str(code)[:40], ENV_FILE))
    passwords[password_key] = password
    write_private(path, render(passwords))
    return checked_session(data)


def job_input(index):
    country, city = CITIES[index % len(CITIES)]
    skills = ["perfseed", SKILLS[index % len(SKILLS)], SKILLS[(index // len(SKILLS) + 1) % len(SKILLS)]]
    return {
        "idempotencyKey": str(uuid.uuid5(NAMESPACE, "create-%d" % index)),
        "title": "%s %04d" % (ROLES[index % len(ROLES)], index),
        "description": "Synthetic job %d created by the local performance seed. It describes no real company or person." % index,
        "requirements": ["Synthetic requirement A for job %d" % index, "Synthetic requirement B"],
        "skills": sorted(set(skills)),
        "country": country,
        "city": city,
        "remote": index % 3 == 0,
    }


def count_open_jobs(base, token, limit):
    count, after, pages = 0, None, 0
    while pages < 10:
        data = post(base, MY_OPEN_JOBS, {"first": PAGE, "after": after}, token)["myJobs"]
        count += len(data["edges"])
        pages += 1
        if count >= limit or not data["pageInfo"]["hasNextPage"]:
            break
        after = data["pageInfo"]["endCursor"]
    return count


def run(base, jobs, candidates, repo):
    path = os.path.join(repo, ENV_FILE)
    passwords = read_passwords(path)

    PROGRESS.phase_started("signing in the synthetic recruiter")
    recruiter_token, _ = auth(base, RECRUITER, "RECRUITER", {"recruiter": {"organizationName": "Perf Seed Organisation"}},
                              "PERF_RECRUITER_PASSWORD", passwords, path)
    PROGRESS.phase_started("counting the recruiter's open jobs")
    open_jobs = count_open_jobs(base, recruiter_token, jobs)
    created = 0
    if open_jobs >= jobs:
        PROGRESS.say("the recruiter already owns %d open jobs (wanted %d); creating none" % (open_jobs, jobs), True)
    else:
        for index in range(jobs):
            PROGRESS.phase = "creating and publishing job %d of %d" % (index + 1, jobs)
            created_job = post(base, CREATE_JOB, {"input": job_input(index)}, recruiter_token)["createJob"]
            if created_job["__typename"] != "Job":
                raise Stop(1, "create-job-refused", "createJob refused job %d (%s)" % (index, str(created_job.get("code"))[:40]))
            published = post(base, PUBLISH_JOB, {"input": {"idempotencyKey": str(uuid.uuid5(NAMESPACE, "publish-%d" % index)),
                                                           "jobId": created_job["id"]}}, recruiter_token)["publishJob"]
            if published["__typename"] != "Job":
                raise Stop(1, "publish-job-refused", "publishJob refused job %d (%s)" % (index, str(published.get("code"))[:40]))
            created += 1
    PROGRESS.phase_started("signing in %d synthetic candidate(s)" % candidates)
    tokens, expiries = [], []
    for number in range(1, candidates + 1):
        PROGRESS.phase = "signing in candidate %d of %d" % (number, candidates)
        profile = {"candidate": {"skills": ["scala", "graphql"], "experienceSummary": "Synthetic candidate for latency measurements."}}
        token, expires = auth(base, CANDIDATE.format(number), "CANDIDATE", profile, "PERF_CANDIDATE_%d_PASSWORD" % number, passwords, path)
        tokens.append(token)
        expiries.append(expires)
    write_private(path, render(passwords, tokens, expiries))
    print("seed-local-jobs: credentials and tokens written to %s (mode 0600, contents not shown); earliest token expiry %s" % (ENV_FILE, min(expiries)))
    print("seed-local-jobs: next: ( . %s && scripts/generate-local-traffic.sh <seconds> --mix search )   # a subshell keeps the tokens out of your shell" % ENV_FILE)
    return "jobs_open=%d jobs_created=%d candidates=%d" % (max(open_jobs, jobs), created, candidates)


def main(argv):
    base, jobs, candidates, repo = argv[1], int(argv[2]), int(argv[3]), argv[4]
    started = time.monotonic()
    signal.signal(signal.SIGTERM, signal.default_int_handler)  # TERM/HUP clean up like Ctrl+C
    signal.signal(signal.SIGHUP, signal.default_int_handler)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        if not loopback_base(base):
            raise Stop(2, "usage", "the base URL must be loopback (127.0.0.1, localhost or [::1]): credentials are sent to it")
        try:
            OPENER.open(base + "/health", timeout=3).close()
        except urllib.error.HTTPError:
            pass  # any HTTP answer means the API is up; the first mutation reports real problems
        except (urllib.error.URLError, TimeoutError, OSError):
            raise Stop(2, "api-unreachable", "no HTTP answer from %s/health within 3s; start the API first" % base)
        detail = run(base, jobs, candidates, repo)
    except Stop as stop:
        print("seed-local-jobs: %s" % stop.message, file=sys.stderr)
        verdict = "BLOCKED" if stop.code == 2 else "FAIL"
        print("RESULT: %s reason=%s duration=%ds base=%s" % (verdict, stop.reason, time.monotonic() - started, base), file=sys.stderr)
        return stop.code
    except KeyboardInterrupt:
        print("\nseed-local-jobs: interrupted; jobs already created stay (a rerun replays them); passwords of accounts signed up so far are in %s, "
              "tokens are written only when everything succeeded" % ENV_FILE, file=sys.stderr)
        return 130
    print("RESULT: PASS %s duration=%ds base=%s" % (detail, time.monotonic() - started, base))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))