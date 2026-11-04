"""The one definition of the workflow files: the paths whose change alters how agents are governed or how the build and
containers run. Imported by guard-harness.py (edits need human approval), check-docs-current.py (edits need canonical
documentation) and described by the review tier table in AGENTS.md (Security Engineer always required).

Each script adds its own extras on top (for example the guard also protects .git/ and secret locations; the docs gate also
counts production source). Keeping the shared core here, and a test that every workflow file is covered by both scripts,
stops the lists drifting apart. Imported with sys.path set by the importing script, because the scripts run under -I.
"""

import re

WORKFLOW_PREFIXES = (".claude/", ".githooks/", "scripts/", "project/", "analytics/project/")
WORKFLOW_FILES = frozenset({
    "AGENTS.md", "CLAUDE.md", "build.sbt", "analytics/build.sbt", "Dockerfile", "analytics/Dockerfile",
    ".dockerignore", "analytics/.dockerignore", ".mcp.json",
    # Tooling configuration that changes how the build, the formatter or the test JVMs behave.
    ".scalafmt.conf", "analytics/.scalafmt.conf", ".sbtopts", "analytics/.sbtopts", ".jvmopts", "analytics/.jvmopts",
})
COMPOSE_FILE = re.compile(r"^compose[\w.-]*\.ya?ml$")


def is_workflow_file(path):
    """Pure: is this repository-relative path one of the workflow files?"""
    return path.startswith(WORKFLOW_PREFIXES) or path in WORKFLOW_FILES or COMPOSE_FILE.match(path) is not None
