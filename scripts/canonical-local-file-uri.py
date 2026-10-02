#!/usr/bin/env python3
"""Render decoded absolute local paths as canonical Java file URIs."""
import pathlib
import sys
import urllib.parse


def canonical_file_uri(decoded_path: str) -> str:
    path = pathlib.Path(decoded_path)
    if not path.is_absolute() or any(part in (".", "..") for part in decoded_path.split("/")):
        raise ValueError("Expected an absolute path without dot segments")
    # java.net.URI("file", null, decodedPath, null, null).toASCIIString:
    # URI path characters, including '+', remain literal; spaces, '%' and
    # non-ASCII characters are escaped from the decoded path exactly once.
    return "file://" + urllib.parse.quote(path.as_posix(), safe="/+:@!$&'()*,-._~;=")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Expected exactly one decoded absolute path")
    print(canonical_file_uri(sys.argv[1]))
