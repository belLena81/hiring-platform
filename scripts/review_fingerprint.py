"""The content fingerprint of the reviewable tree: what a reviewer read, independent of commits.

A review verdict is tied to this fingerprint, not to the last commit, so committing the reviewed work changes nothing and an
edit after the review does. The fingerprint covers every tracked and untracked, non-ignored file except the documentation
that needs no review by the AGENTS.md tier table (docs/, README.md, ARCHITECTURE.md; the spec that records the verdicts is
inside docs/). Sandbox mounts of denied paths (character devices, empty read-only files) are not content and are skipped.
Imported with sys.path set by the importing script, because the scripts run under -I.
"""

import hashlib
import os
import stat
import subprocess

EXCLUDED = (":(exclude).local", ":(exclude)docs",":(exclude)README.md", ":(exclude)ARCHITECTURE.md")


def _file_digest(path):
    """Pure-ish: a digest of one file's content or link target; None for a sandbox mount or an unreadable file."""
    try:
        status = os.lstat(path)
        if stat.S_ISLNK(status.st_mode):
            return hashlib.sha256(os.readlink(path).encode()).hexdigest()
        if not stat.S_ISREG(status.st_mode) or (status.st_size == 0 and not status.st_mode & 0o222):
            return None
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        return digest.hexdigest()
    except OSError:
        return None


def fingerprint(project):
    """The hex fingerprint of the reviewable tree under project, or None when git cannot list it."""
    try:
        listing = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ".", *EXCLUDED],
                                 cwd=project, capture_output=True, timeout=60, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    total = hashlib.sha256()
    for name in sorted(set(listing.split(b"\0")) - {b""}):
        digest = _file_digest(os.path.join(os.fsencode(project), name))
        if digest is not None:
            total.update(name + b"\0" + digest.encode() + b"\n")
    return total.hexdigest()